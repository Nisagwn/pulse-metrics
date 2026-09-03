package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nisah/pulse-metrics/internal/obs"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// Alarm motoru varsayilanlari.
const (
	defaultEvalInterval = 30 * time.Second
	// baselineMultiplier: zscore icin taban cizgisi penceresi, kural
	// penceresinin kac kati olsun? Son 5 dakikayi son 1 saatle
	// kiyaslamak icin 12.
	baselineMultiplier = 12
	minBaselinePoints  = 10
)

// alertStateDDL: kurallarin PAYLASILAN tetiklenme durumu (Faz 4).
//
// TTL yok ve olmamali: bu tablo veri degil, KARAR tutuyor. Bir alarmin
// tetiklenmis oldugu bilgisi 7 gun sonra kendiliginden silinirse, alarm
// hala devam ederken sistem onu "yeni" sanip tekrar bildirir.
const alertStateDDL = `
	CREATE TABLE IF NOT EXISTS pulse.alert_state (
		rule_id    TEXT PRIMARY KEY,
		firing     BOOLEAN,
		notified   BOOLEAN,
		since_ms   BIGINT,
		owner      TEXT,
		updated_ms BIGINT
	)`

// alertStateNotifiedDDL: notified kolonunu var olan tabloya ekler.
//
// CREATE TABLE IF NOT EXISTS var olan bir tabloyu degistirmez, bu yuzden
// Faz 4'ten kalan tablolarda kolon ALTER ile ekleniyor.
//
// Buradaki karsitlik ogretici: SIRADAN bir kolon eklemek ucuz ve
// cevrimici bir islem - Scylla yalnizca sema surumunu gunceller, veriye
// dokunmaz. Faz 4'te partition key'i degistirmek ise butun veriyi
// tasimayi gerektirmisti. Ayni tabloda, iki tamamen farkli maliyet.
const alertStateNotifiedDDL = `ALTER TABLE pulse.alert_state ADD notified BOOLEAN`

// ensurePhase4Schema: Faz 4'te eklenen tablolar.
func ensurePhase4Schema(session *gocql.Session, logger *zap.Logger) error {
	if err := session.Query(alertStateDDL).Exec(); err != nil {
		return fmt.Errorf("alert_state tablosu yaratilamadi: %w", err)
	}
	// Kolon zaten varsa hata doner; bu beklenen durum, yutuyoruz.
	// ALTER'in IF NOT EXISTS karsiligi yok.
	if err := session.Query(alertStateNotifiedDDL).Exec(); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "conflicts with an existing column") &&
		!strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("alert_state.notified eklenemedi: %w", err)
	}
	logger.Info("Phase 4 schema ensured")
	return nil
}

// Condition: "p95 > 500" gibi bir kosulun cozulmus hali.
type Condition struct {
	Aggregation string // avg, sum, min, max, last, p50, p95, p99, zscore
	Operator    string // >, >=, <, <=
	Threshold   float64
}

// ParseCondition: "<toplama> <operator> <sayi>" bicimini cozer.
//
// Kucuk ve kapali bir dil bilerek secildi. Tam bir ifade ayristiricisi
// yazmak (parantezler, AND/OR, aritmetik) bu asamada gereksiz karmasiklik
// olurdu; kullanicinin yazdigi metni degerlendiren bir yapi ise guvenlik
// acigi. Uc parcali bicim ihtiyaci karsiliyor ve dogrulanmasi kolay.
func ParseCondition(s string) (Condition, error) {
	var c Condition

	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) != 3 {
		return c, fmt.Errorf("kosul bicimi %q gecersiz, beklenen: \"<toplama> <operator> <sayi>\" (ornek: \"p95 > 500\")", s)
	}

	agg := strings.ToLower(fields[0])
	switch agg {
	case "avg", "sum", "min", "max", "last", "count", "p50", "p95", "p99", "zscore":
	default:
		return c, fmt.Errorf("bilinmeyen toplama %q (avg, sum, min, max, last, count, p50, p95, p99, zscore)", fields[0])
	}

	op := fields[1]
	switch op {
	case ">", ">=", "<", "<=":
	default:
		return c, fmt.Errorf("bilinmeyen operator %q (>, >=, <, <=)", op)
	}

	threshold, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return c, fmt.Errorf("esik degeri sayi olmali: %q", fields[2])
	}

	return Condition{Aggregation: agg, Operator: op, Threshold: threshold}, nil
}

// Breached: verilen deger kosulu ihlal ediyor mu?
func (c Condition) Breached(value float64) bool {
	switch c.Operator {
	case ">":
		return value > c.Threshold
	case ">=":
		return value >= c.Threshold
	case "<":
		return value < c.Threshold
	case "<=":
		return value <= c.Threshold
	}
	return false
}

func (c Condition) String() string {
	return fmt.Sprintf("%s %s %g", c.Aggregation, c.Operator, c.Threshold)
}

// AlertEngine: kurallari periyodik olarak degerlendirir.
//
// Faz 4'te en onemli degisiklik burada: "hangi kural su anda tetiklenmis"
// bilgisi artik BELLEKTE DEGIL, veritabaninda.
//
// Faz 3'te bu bir map[string]bool idi ve tek collector varken dogru
// calisiyordu. Ikinci bir collector acildigi anda bozuluyordu: iki surecin
// de kendi map'i vardi, ikisi de ayni ihlali "yeni" sanip webhook
// gonderiyordu. Yani yatay olceklendirme sistemi calismaz hale
// getirmiyordu - daha sinsi bir sey yapiyordu: her alarmi iki kez
// bildiriyordu.
type AlertEngine struct {
	session  *gocql.Session
	logger   *zap.Logger
	client   *http.Client
	interval time.Duration

	// owner: bu surecin adi. Gecisi kimin yaptigini kaydeder; hata
	// ayiklarken "hangi collector bildirdi" sorusunun cevabi.
	owner string
}

// NewAlertEngine: alarm motorunu olusturur.
func NewAlertEngine(session *gocql.Session, logger *zap.Logger, interval time.Duration, owner string) *AlertEngine {
	if interval <= 0 {
		interval = defaultEvalInterval
	}
	if owner == "" {
		owner = "unknown"
	}
	return &AlertEngine{
		session:  session,
		logger:   logger,
		client:   &http.Client{Timeout: 10 * time.Second},
		interval: interval,
		owner:    owner,
	}
}

// transitionResult: bir gecis denemesinin sonucu.
//
// Ilk surum bunu bool ile ifade ediyordu ve iki farkli seyi tek degere
// sikistiriyordu: "yarisi kaybettim" ile "yapacak bir sey yoktu". Sonuc,
// yalan soyleyen bir olcuydu - kural sakin sakin dururken her
// degerlendirme turu "kaybedildi" olarak sayiliyor, panelde de bu
// "sistem yaris cozuyor" gibi gorunuyordu. Uc durum uc deger ister.
type transitionResult int

const (
	// transitionNone: durum zaten istenen halde, gecis gerekmedi.
	// Normal isleyiste turlerin buyuk cogunlugu budur ve sayilmaz.
	transitionNone transitionResult = iota
	// transitionWon: gecisi bu surec yapti, bildirimden o sorumlu.
	transitionWon
	// transitionLost: baska bir collector ayni gecisi once yapti.
	// Yatay olceklendirmenin dogru calistiginin kaniti; sifirdan
	// buyuk olmasi beklenir ve iyidir.
	transitionLost
	// transitionReclaimed: gecis daha once yapilmis ama bildirimi
	// tamamlanmamis; bu surec yarim kalan isi devraldi.
	transitionReclaimed
)

// staleAfter: bildirimi tamamlanmamis bir gecis ne kadar sonra
// "sahibi olmus" sayilir?
//
// Iki degerlendirme turu bekliyoruz: sahibi hayattaysa bu sure icinde
// isini bitirir. Alt sinir 60 saniye, cunku cok kisa araliklarla calisan
// bir motor kendi kendini devralmaya baslardi.
func (e *AlertEngine) staleAfter() time.Duration {
	d := 2 * e.interval
	if d < time.Minute {
		d = time.Minute
	}
	return d
}

// tryTransition: kuralin durumunu !desired -> desired olarak degistirmeye
// calisir. transitionWon dondurduyse gecisi BU surec yapti.
//
// Isin kalbi CQL'in IF cumlesi: hafif islem (lightweight transaction, LWT).
// Sradan bir UPDATE "son yazan kazanir" mantigiyla calisir - iki collector
// ayni anda firing=true yazsa ikisi de basarili olur ve iki bildirim gider.
// IF ekledigimizde Scylla arka planda Paxos calistirir ve guncellemeyi
// SADECE mevcut deger bekledigimiz degerse uygular. Yaristan tek kazanan
// cikar; kaybeden applied=false alir ve sessizce gecer.
//
// Bedeli var: LWT normal bir yazmadan cok daha pahalidir (dort gidis-donus
// ve dugumler arasi uzlasma). Bu yuzden SADECE durum gecisinde kullaniyoruz,
// sicak yolda degil. Kurallar her turda degerlendiriliyor; LWT ise yalnizca
// alarm gercekten tetiklendiginde veya cozuldugunde calisiyor - saatte
// birkac kez.
//
// Not: lider secimine (leader election) gerek yok. Gecisin kendisi zaten
// karsilikli dislama sagliyor; ayrica bir lider secip onu canli tutmaya
// calismak daha fazla hareketli parca demekti.
func (e *AlertEngine) tryTransition(ctx context.Context, ruleID string, desired bool) (transitionResult, error) {
	now := time.Now().UnixMilli()

	// 1) ONCE UCUZ OKUMA.
	//
	// Kurallar her turda degerlendiriliyor ama durum nadiren degisiyor.
	// Her degerlendirmede dogrudan LWT calistirmak, saniyede birkac kez
	// Paxos uzlasmasi demekti - istisnai durumun bedelini her tura
	// yaymak. Sradan bir SELECT ise tek gidis-donus.
	var (
		firing    bool
		notified  bool
		updatedMs int64
	)
	err := e.session.Query(
		`SELECT firing, notified, updated_ms FROM alert_state WHERE rule_id = ?`, ruleID).
		WithContext(ctx).Scan(&firing, &notified, &updatedMs)

	switch {
	case errors.Is(err, gocql.ErrNotFound):
		// Satir yok: kural hic tetiklenmemis demektir. Cozulecek bir sey
		// de yok, yani yalnizca tetiklenme yonu anlamli.
		if !desired {
			return transitionNone, nil
		}
		// Satiri yaratirken IF NOT EXISTS: iki collector ayni anda ilk
		// ihlali gorurse yalnizca biri yaratabilsin.
		created := map[string]interface{}{}
		applied, insErr := e.session.Query(`
			INSERT INTO alert_state (rule_id, firing, notified, since_ms, owner, updated_ms)
			VALUES (?, ?, ?, ?, ?, ?)
			IF NOT EXISTS`,
			ruleID, true, false, now, e.owner, now,
		).WithContext(ctx).MapScanCAS(created)
		if insErr != nil {
			return transitionNone, fmt.Errorf("alarm durumu yaratilamadi: %w", insErr)
		}
		if applied {
			return transitionWon, nil
		}
		return transitionLost, nil

	case err != nil:
		return transitionNone, fmt.Errorf("alarm durumu okunamadi: %w", err)

	case firing == desired:
		// Durum zaten istedigimiz gibi - ama BILDIRIM GITTI MI?
		//
		// Gecis ile bildirim atomik degil: gecisi kazanan surec
		// veritabanina yazip webhook'u gondermeden once olebilir. O
		// zaman satir "tetiklenmis" der, ihlal surer ve hicbir
		// degerlendirme bir daha bildirim uretmez. Alarm sonsuza kadar
		// susar - Faz 4'te bilerek acik biraktigim delik buydu.
		//
		// Cozum, satiri kucuk bir is kuyrugu gibi kullanmak:
		// notified=false "yarim kalmis is" demek. Sahibi makul bir sure
		// icinde bitirmediyse baska bir collector devralabilir.
		if !notified && time.Since(time.UnixMilli(updatedMs)) > e.staleAfter() {
			return e.reclaim(ctx, ruleID, now)
		}
		return transitionNone, nil
	}

	// 2) GERCEK GECIS: burada Paxos'a deger.
	//
	// IF olmadan bu sradan bir UPDATE olurdu ve "son yazan kazanir"
	// mantigiyla iki collector de basarili olurdu - iki bildirim.
	// IF ile Scylla guncellemeyi yalnizca mevcut deger hala
	// bekledigimiz degerse uygular; yaristan tek kazanan cikar.
	//
	// Yukaridaki SELECT ile bu UPDATE arasinda durum degismis olabilir.
	// Bu bir sorun degil, tam da IF'in yakaladigi durum: gec kalan
	// collector applied=false alir ve susar.
	current := map[string]interface{}{}
	applied, err := e.session.Query(`
		UPDATE alert_state SET firing = ?, since_ms = ?, owner = ?, updated_ms = ?
		WHERE rule_id = ?
		IF firing = ?`,
		desired, now, e.owner, now, ruleID, !desired,
	).WithContext(ctx).MapScanCAS(current)
	if err != nil {
		return transitionNone, fmt.Errorf("alarm durumu guncellenemedi: %w", err)
	}
	if applied {
		return transitionWon, nil
	}
	return transitionLost, nil
}

// reclaim: bildirimi tamamlanmamis ve bayatlamis bir gecisi devralir.
//
// IF notified = false sarti yine yarisi tek kazanana indiriyor. Kazanan
// updated_ms'i tazeledigi icin kaybedenin bir sonraki turda "bayat"
// gormesi de engellenmis oluyor.
func (e *AlertEngine) reclaim(ctx context.Context, ruleID string, now int64) (transitionResult, error) {
	current := map[string]interface{}{}
	applied, err := e.session.Query(`
		UPDATE alert_state SET owner = ?, updated_ms = ?
		WHERE rule_id = ?
		IF notified = false`,
		e.owner, now, ruleID,
	).WithContext(ctx).MapScanCAS(current)
	if err != nil {
		return transitionNone, fmt.Errorf("yarim kalan bildirim devralinamadi: %w", err)
	}
	if applied {
		return transitionReclaimed, nil
	}
	return transitionLost, nil
}

// markNotified: bildirim tamamlandi, is kapandi.
//
// Bu sradan bir UPDATE, LWT degil: bu noktada satirin sahibi biziz ve
// yarisacak kimse yok.
func (e *AlertEngine) markNotified(ctx context.Context, ruleID string) {
	if err := e.session.Query(
		`UPDATE alert_state SET notified = true, updated_ms = ? WHERE rule_id = ?`,
		time.Now().UnixMilli(), ruleID,
	).WithContext(ctx).Exec(); err != nil {
		// Yazilamazsa alarm yine de gonderildi; en kotu ihtimalle baska
		// bir collector bayat sanip tekrar gonderir. Kaybetmektense
		// tekrarlamak dogru takas.
		e.logger.Warn("Bildirim tamamlandi isaretlenemedi",
			zap.String("rule", ruleID), zap.Error(err))
	}
}

// Run: ctx iptal edilene kadar kurallari degerlendirir.
func (e *AlertEngine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	e.logger.Info("Alert engine started", zap.Duration("interval", e.interval))

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Alert engine stopping")
			return nil
		case <-ticker.C:
			if _, err := e.EvaluateAll(ctx, ""); err != nil {
				e.logger.Error("Kural degerlendirmesi basarisiz", zap.Error(err))
			}
		}
	}
}

// EvaluateAll: etkin kurallari degerlendirir, tetiklenen alarmlari dondurur.
func (e *AlertEngine) EvaluateAll(ctx context.Context, onlyRuleID string) ([]*pb.Alert, error) {
	rules, err := loadRules(ctx, e.session, "")
	if err != nil {
		return nil, err
	}

	var fired []*pb.Alert
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if onlyRuleID != "" && rule.RuleId != onlyRuleID {
			continue
		}

		obs.AlertEvaluations.Inc()
		alert, err := e.evaluateRule(ctx, rule)
		if err != nil {
			e.logger.Warn("Kural degerlendirilemedi",
				zap.String("rule", rule.RuleId), zap.Error(err))
			continue
		}
		if alert != nil {
			fired = append(fired, alert)
		}
	}
	return fired, nil
}

// evaluateRule: tek bir kurali degerlendirir.
//
// Durum gecisi mantigi:
//   - ihlal var + daha once tetiklenmemis -> alarm uret, kaydet, webhook
//   - ihlal var + zaten tetiklenmis       -> sessiz kal (spam yok)
//   - ihlal yok + tetiklenmisti           -> "cozuldu" alarmi uret
//   - ihlal yok + tetiklenmemis           -> sessiz kal
func (e *AlertEngine) evaluateRule(ctx context.Context, rule *pb.AlertRule) (*pb.Alert, error) {
	cond, err := ParseCondition(rule.Condition)
	if err != nil {
		return nil, err
	}

	window := time.Duration(rule.DurationSeconds) * time.Second
	if window <= 0 {
		window = 5 * time.Minute
	}

	value, ok, err := e.computeValue(ctx, rule, cond, window)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // deger yok: kural hakkinda bir sey soyleyemeyiz
	}

	breached := cond.Breached(value)

	// Durumu degistirme hakkini kazanabildik mi? Kazanamadiysak ya durum
	// zaten degismisti ya da baska bir collector ayni anda ayni gecisi
	// yapti - iki halde de bize dusen sey susmak.
	res, err := e.tryTransition(ctx, rule.RuleId, breached)
	if err != nil {
		return nil, err
	}
	state := "resolved"
	if breached {
		state = "firing"
	}
	switch res {
	case transitionNone:
		// Durum degismedi. Sayaci artirmiyoruz: bu bir olay degil,
		// olayin YOKLUGU. Her turu saymak olcuyu anlamsizlastirirdi.
		return nil, nil
	case transitionLost:
		obs.AlertTransitions.WithLabelValues(state, "lost").Inc()
		return nil, nil
	case transitionReclaimed:
		// Sifirdan buyuk olmasi bir collector'in bildirim gonderemeden
		// oldugunu soyler - izlemeye deger bir sinyal.
		obs.AlertTransitions.WithLabelValues(state, "reclaimed").Inc()
		e.logger.Warn("Yarim kalan alarm bildirimi devralindi",
			zap.String("rule", rule.RuleId), zap.String("state", state))
	default:
		obs.AlertTransitions.WithLabelValues(state, "won").Inc()
	}

	alert := buildAlert(rule, cond, value, !breached)

	if err := e.storeAlert(ctx, alert); err != nil {
		e.logger.Error("Alarm kaydedilemedi", zap.Error(err))
	}
	if rule.WebhookUrl != "" {
		e.notify(ctx, rule.WebhookUrl, alert)
	}
	// Is bitti: satiri kapat ki baska bir collector devralmasin.
	e.markNotified(ctx, rule.RuleId)

	e.logger.Info("Alarm durumu degisti",
		zap.String("rule", rule.RuleId),
		zap.String("service", rule.ServiceName),
		zap.String("metric", rule.MetricName),
		zap.Float64("value", value),
		zap.Bool("resolved", alert.Resolved),
	)

	return alert, nil
}

func buildAlert(rule *pb.AlertRule, cond Condition, value float64, resolved bool) *pb.Alert {
	name := rule.Name
	if name == "" {
		name = rule.RuleId
	}

	var msg string
	if resolved {
		msg = fmt.Sprintf("%s cozuldu: %s.%s %s = %.2f (esik %g)",
			name, rule.ServiceName, rule.MetricName, cond.Aggregation, value, cond.Threshold)
	} else {
		msg = fmt.Sprintf("%s tetiklendi: %s.%s %s = %.2f, kosul \"%s\"",
			name, rule.ServiceName, rule.MetricName, cond.Aggregation, value, cond.String())
	}

	return &pb.Alert{
		RuleId:      rule.RuleId,
		RuleName:    name,
		ServiceName: rule.ServiceName,
		MetricName:  rule.MetricName,
		Condition:   rule.Condition,
		MetricValue: value,
		Threshold:   cond.Threshold,
		Message:     msg,
		TimestampMs: time.Now().UnixMilli(),
		Severity:    rule.Severity,
		Resolved:    resolved,
	}
}

// computeValue: kuralin penceresi icin degeri hesaplar.
// zscore ozel bir durum: taban cizgisi karsilastirmasi gerektirir.
func (e *AlertEngine) computeValue(ctx context.Context, rule *pb.AlertRule, cond Condition, window time.Duration) (float64, bool, error) {
	now := time.Now()

	if cond.Aggregation == "zscore" {
		return e.computeZScore(ctx, rule, window, now)
	}

	values, err := e.fetchValues(ctx, rule, now.Add(-window), now)
	if err != nil {
		return 0, false, err
	}
	if len(values) == 0 {
		return 0, false, nil
	}

	return aggregateFloats(cond.Aggregation, values), true, nil
}

// computeZScore: istatistiksel anomali tespiti.
//
// Son pencerenin ortalamasi, daha uzun bir gecmisin ortalamasindan kac
// standart sapma uzakta? Sabit esikli kurallardan farki: "normal"in ne
// oldugunu verinin kendisi soyler. Gunduz 200 istek/sn, gece 20 istek/sn
// alan bir servise tek bir sabit esik koyamazsin.
func (e *AlertEngine) computeZScore(ctx context.Context, rule *pb.AlertRule, window time.Duration, now time.Time) (float64, bool, error) {
	recent, err := e.fetchValues(ctx, rule, now.Add(-window), now)
	if err != nil {
		return 0, false, err
	}
	if len(recent) == 0 {
		return 0, false, nil
	}

	// Taban cizgisi son pencereyi DISLAR: kendini kendiyle kiyaslamak
	// anomaliyi normale cevirir.
	baselineEnd := now.Add(-window)
	baselineStart := baselineEnd.Add(-window * baselineMultiplier)
	baseline, err := e.fetchValues(ctx, rule, baselineStart, baselineEnd)
	if err != nil {
		return 0, false, err
	}
	if len(baseline) < minBaselinePoints {
		return 0, false, nil // yeterli gecmis yok, sessiz kal
	}

	mean, stddev := meanStdDev(baseline)
	if stddev == 0 {
		// Tamamen sabit bir seri: sapma yoksa z-score tanimsiz.
		// Deger degistiyse anomali, degismediyse degil.
		if aggregateFloats("avg", recent) != mean {
			return math.Inf(1), true, nil
		}
		return 0, true, nil
	}

	z := (aggregateFloats("avg", recent) - mean) / stddev
	return z, true, nil
}

// fetchValues: bir metrigin zaman araligindaki ham degerleri.
//
// Faz 4'te kova dongusu eklendi. Bu ozellikle zscore icin onemli: taban
// cizgisi penceresi kural penceresinin 12 kati oldugundan neredeyse her
// zaman saat sinirini asar ve tek kovadan okumak gecmisin buyuk kismini
// gormezden gelirdi.
func (e *AlertEngine) fetchValues(ctx context.Context, rule *pb.AlertRule, start, end time.Time) ([]float64, error) {
	startMs, endMs := start.UnixMilli(), end.UnixMilli()

	var values []float64
	for _, bucket := range bucketsInRangeMax(startMs, endMs, metricBucketLimit) {
		iter := e.session.Query(`
			SELECT value FROM metrics
			WHERE service_name = ? AND metric_name = ? AND time_bucket = ?
			  AND timestamp >= ? AND timestamp <= ?`,
			rule.ServiceName, rule.MetricName, bucket, startMs, endMs,
		).WithContext(ctx).Iter()

		var v float64
		for iter.Scan(&v) {
			values = append(values, v)
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("metrik okunamadi: %w", err)
		}
	}
	return values, nil
}

// storeAlert: alarmi alerts tablosuna yazar.
func (e *AlertEngine) storeAlert(ctx context.Context, a *pb.Alert) error {
	bucket := TimeBucket(time.UnixMilli(a.TimestampMs))
	return e.session.Query(`
		INSERT INTO alerts (service_name, time_bucket, timestamp_ms, alert_id,
		                    rule_id, rule_name, metric_name, condition,
		                    metric_value, threshold, severity, message, resolved)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ServiceName, bucket, a.TimestampMs, newID(),
		a.RuleId, a.RuleName, a.MetricName, a.Condition,
		a.MetricValue, a.Threshold, a.Severity.String(), a.Message, a.Resolved,
	).WithContext(ctx).Exec()
}

// notify: webhook'a POST atar. Basarisizlik alarmi gecersiz kilmaz -
// alarm zaten veritabaninda ve panelde gorunur.
func (e *AlertEngine) notify(ctx context.Context, url string, a *pb.Alert) {
	body, err := json.Marshal(map[string]interface{}{
		"rule_id":      a.RuleId,
		"rule_name":    a.RuleName,
		"service":      a.ServiceName,
		"metric":       a.MetricName,
		"condition":    a.Condition,
		"value":        a.MetricValue,
		"threshold":    a.Threshold,
		"severity":     a.Severity.String(),
		"message":      a.Message,
		"resolved":     a.Resolved,
		"timestamp_ms": a.TimestampMs,
	})
	if err != nil {
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		e.logger.Warn("Webhook istegi olusturulamadi", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		e.logger.Warn("Webhook gonderilemedi", zap.String("url", url), zap.Error(err))
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		e.logger.Warn("Webhook hata dondu",
			zap.String("url", url), zap.Int("status", resp.StatusCode))
	}
}

// ActiveRuleIDs: su anda tetiklenmis kurallar.
//
// Paylasilan durumdan okunur, bu yuzden hangi collector'a sorarsan sor ayni
// cevabi alirsin. Panelin "yaniyor" rozeti Faz 3'te hangi collector'a
// dustugune gore degisebiliyordu; artik degismiyor.
//
// Tam tarama gibi gorunuyor ama tablo kural basina tek satir tutuyor -
// yuzlerce satirlik bir tablo, milyonlarcalik degil.
func (e *AlertEngine) ActiveRuleIDs() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	iter := e.session.Query(`SELECT rule_id, firing FROM alert_state`).
		WithContext(ctx).Iter()

	var (
		id     string
		firing bool
		out    []string
	)
	for iter.Scan(&id, &firing) {
		if firing {
			out = append(out, id)
		}
	}
	if err := iter.Close(); err != nil {
		e.logger.Warn("Alarm durumlari okunamadi", zap.Error(err))
		return nil
	}

	sort.Strings(out)
	return out
}

// --- istatistik yardimcilari ------------------------------------------------

func meanStdDev(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	var sq float64
	for _, v := range values {
		d := v - mean
		sq += d * d
	}
	// Ornek standart sapmasi (n-1): elimizdeki gecmis, tum olasi
	// olculerin bir ornegi, evrenin kendisi degil.
	denom := float64(len(values) - 1)
	if denom <= 0 {
		return mean, 0
	}
	return mean, math.Sqrt(sq / denom)
}

func aggregateFloats(kind string, values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch kind {
	case "sum":
		var s float64
		for _, v := range values {
			s += v
		}
		return s
	case "avg":
		var s float64
		for _, v := range values {
			s += v
		}
		return s / float64(len(values))
	case "min":
		m := math.Inf(1)
		for _, v := range values {
			m = math.Min(m, v)
		}
		return m
	case "max":
		m := math.Inf(-1)
		for _, v := range values {
			m = math.Max(m, v)
		}
		return m
	case "count":
		return float64(len(values))
	case "last":
		return values[len(values)-1]
	case "p50", "p95", "p99":
		q := map[string]float64{"p50": 0.50, "p95": 0.95, "p99": 0.99}[kind]
		cp := append([]float64(nil), values...)
		return percentile(cp, q)
	}
	return 0
}

// --- kural deposu -----------------------------------------------------------

func loadRules(ctx context.Context, session *gocql.Session, serviceName string) ([]*pb.AlertRule, error) {
	iter := session.Query(`
		SELECT rule_id, name, service_name, metric_name, condition,
		       duration_seconds, webhook_url, enabled, severity, created_at_ms
		FROM alert_rules`).WithContext(ctx).Iter()

	var (
		ruleID, name, svc, metric, cond, webhook, severity string
		duration                                           int
		enabled                                            bool
		createdAt                                          int64
		out                                                []*pb.AlertRule
	)
	for iter.Scan(&ruleID, &name, &svc, &metric, &cond, &duration,
		&webhook, &enabled, &severity, &createdAt) {
		if serviceName != "" && svc != serviceName {
			continue
		}
		out = append(out, &pb.AlertRule{
			RuleId:          ruleID,
			Name:            name,
			ServiceName:     svc,
			MetricName:      metric,
			Condition:       cond,
			DurationSeconds: int32(duration),
			WebhookUrl:      webhook,
			Enabled:         enabled,
			Severity:        parseSeverity(severity),
			CreatedAtMs:     createdAt,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("kurallar okunamadi: %w", err)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAtMs > out[j].CreatedAtMs })
	return out, nil
}

func parseSeverity(s string) pb.AlertSeverity {
	if v, ok := pb.AlertSeverity_value[strings.ToUpper(s)]; ok {
		return pb.AlertSeverity(v)
	}
	return pb.AlertSeverity_WARNING
}

// --- gRPC servisi -----------------------------------------------------------

// AlertServiceServer: pb.AlertServiceServer uygulamasi.
type AlertServiceServer struct {
	pb.UnimplementedAlertServiceServer

	session *gocql.Session
	logger  *zap.Logger
	engine  *AlertEngine
}

// NewAlertServiceServer: alarm gRPC servisini olusturur.
func NewAlertServiceServer(session *gocql.Session, logger *zap.Logger, engine *AlertEngine) *AlertServiceServer {
	return &AlertServiceServer{session: session, logger: logger, engine: engine}
}

// CreateRule: yeni bir alarm kurali kaydeder.
func (s *AlertServiceServer) CreateRule(ctx context.Context, rule *pb.AlertRule) (*pb.CreateRuleResponse, error) {
	if rule.GetServiceName() == "" || rule.GetMetricName() == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name ve metric_name zorunlu")
	}
	// Kosulu kaydetmeden once dogrula: bozuk bir kural sessizce
	// hicbir zaman tetiklenmeyen bir kural olurdu.
	cond, err := ParseCondition(rule.GetCondition())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ruleID := rule.GetRuleId()
	if ruleID == "" {
		ruleID = newID()
	}
	duration := rule.GetDurationSeconds()
	if duration <= 0 {
		duration = 300
	}
	severity := rule.GetSeverity()
	name := rule.GetName()
	if name == "" {
		name = fmt.Sprintf("%s %s", rule.GetMetricName(), cond.String())
	}

	if err := s.session.Query(`
		INSERT INTO alert_rules (rule_id, name, service_name, metric_name, condition,
		                         duration_seconds, webhook_url, enabled, severity, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ruleID, name, rule.GetServiceName(), rule.GetMetricName(), rule.GetCondition(),
		duration, rule.GetWebhookUrl(), rule.GetEnabled(), severity.String(),
		time.Now().UnixMilli(),
	).WithContext(ctx).Exec(); err != nil {
		return nil, status.Errorf(codes.Internal, "kural kaydedilemedi: %v", err)
	}

	return &pb.CreateRuleResponse{RuleId: ruleID}, nil
}

// ListRules: kayitli kurallar.
func (s *AlertServiceServer) ListRules(ctx context.Context, req *pb.ListRulesRequest) (*pb.ListRulesResponse, error) {
	rules, err := loadRules(ctx, s.session, req.GetServiceName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.ListRulesResponse{Rules: rules}, nil
}

// DeleteRule: kurali siler.
func (s *AlertServiceServer) DeleteRule(ctx context.Context, req *pb.DeleteRuleRequest) (*pb.DeleteRuleResponse, error) {
	if req.GetRuleId() == "" {
		return nil, status.Error(codes.InvalidArgument, "rule_id zorunlu")
	}
	if err := s.session.Query(`DELETE FROM alert_rules WHERE rule_id = ?`,
		req.GetRuleId()).WithContext(ctx).Exec(); err != nil {
		return nil, status.Errorf(codes.Internal, "kural silinemedi: %v", err)
	}
	// Kural gitti; durumu da gitmeli. Kalsaydi, ayni rule_id ile yeni bir
	// kural yaratildiginda kural "tetiklenmis" dogar ve ilk gercek ihlal
	// hic bildirilmezdi.
	if err := s.session.Query(`DELETE FROM alert_state WHERE rule_id = ?`,
		req.GetRuleId()).WithContext(ctx).Exec(); err != nil {
		s.logger.Warn("Alarm durumu silinemedi",
			zap.String("rule", req.GetRuleId()), zap.Error(err))
	}
	return &pb.DeleteRuleResponse{Deleted: true}, nil
}

// ListAlerts: tetiklenmis alarmlar.
func (s *AlertServiceServer) ListAlerts(ctx context.Context, req *pb.ListAlertsRequest) (*pb.ListAlertsResponse, error) {
	now := time.Now()
	end := req.GetEndTimeMs()
	if end <= 0 {
		end = now.UnixMilli()
	}
	start := req.GetStartTimeMs()
	if start <= 0 {
		start = now.Add(-24 * time.Hour).UnixMilli()
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}

	// Hangi servisleri tarayacagiz?
	services := []string{req.GetServiceName()}
	if req.GetServiceName() == "" {
		rules, err := loadRules(ctx, s.session, "")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}
		seen := map[string]bool{}
		services = nil
		for _, r := range rules {
			if !seen[r.ServiceName] {
				seen[r.ServiceName] = true
				services = append(services, r.ServiceName)
			}
		}
	}

	var out []*pb.Alert
	for _, svc := range services {
		if svc == "" {
			continue
		}
		for _, bucket := range bucketsInRange(start, end) {
			if len(out) >= limit {
				break
			}
			iter := s.session.Query(`
				SELECT timestamp_ms, rule_id, rule_name, metric_name, condition,
				       metric_value, threshold, severity, message, resolved
				FROM alerts
				WHERE service_name = ? AND time_bucket = ?
				  AND timestamp_ms >= ? AND timestamp_ms <= ?`,
				svc, bucket, start, end).WithContext(ctx).Iter()

			var (
				ts                                       int64
				ruleID, ruleName, metric, cond, sev, msg string
				value, threshold                         float64
				resolved                                 bool
			)
			for iter.Scan(&ts, &ruleID, &ruleName, &metric, &cond,
				&value, &threshold, &sev, &msg, &resolved) {
				if req.GetFiringOnly() && resolved {
					continue
				}
				out = append(out, &pb.Alert{
					RuleId:      ruleID,
					RuleName:    ruleName,
					ServiceName: svc,
					MetricName:  metric,
					Condition:   cond,
					MetricValue: value,
					Threshold:   threshold,
					Severity:    parseSeverity(sev),
					Message:     msg,
					TimestampMs: ts,
					Resolved:    resolved,
				})
				if len(out) >= limit {
					break
				}
			}
			if err := iter.Close(); err != nil {
				return nil, status.Errorf(codes.Internal, "alarmlar okunamadi: %v", err)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TimestampMs > out[j].TimestampMs })

	resp := &pb.ListAlertsResponse{Alerts: out}
	if s.engine != nil {
		resp.ActiveRuleIds = s.engine.ActiveRuleIDs()
	}
	return resp, nil
}

// EvaluateRules: kurallari hemen degerlendirir (beklemeden test etmek icin).
func (s *AlertServiceServer) EvaluateRules(ctx context.Context, req *pb.EvaluateRulesRequest) (*pb.EvaluateRulesResponse, error) {
	if s.engine == nil {
		return nil, status.Error(codes.Unavailable, "alarm motoru calismiyor")
	}

	rules, err := loadRules(ctx, s.session, "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	evaluated := 0
	for _, r := range rules {
		if r.Enabled && (req.GetRuleId() == "" || r.RuleId == req.GetRuleId()) {
			evaluated++
		}
	}

	fired, err := s.engine.EvaluateAll(ctx, req.GetRuleId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.EvaluateRulesResponse{Fired: fired, Evaluated: int32(evaluated)}, nil
}
