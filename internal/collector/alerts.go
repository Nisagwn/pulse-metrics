package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/nisah/pulse-metrics/internal/proto"
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
type AlertEngine struct {
	session  *gocql.Session
	logger   *zap.Logger
	client   *http.Client
	interval time.Duration

	// firing: su anda tetiklenmis kurallar. Bu durum olmadan her
	// degerlendirme turunda ayni alarm tekrar tekrar gonderilir -
	// klasik "alarm spam" problemi.
	mu     sync.Mutex
	firing map[string]bool
}

// NewAlertEngine: alarm motorunu olusturur.
func NewAlertEngine(session *gocql.Session, logger *zap.Logger, interval time.Duration) *AlertEngine {
	if interval <= 0 {
		interval = defaultEvalInterval
	}
	return &AlertEngine{
		session:  session,
		logger:   logger,
		client:   &http.Client{Timeout: 10 * time.Second},
		interval: interval,
		firing:   make(map[string]bool),
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

	e.mu.Lock()
	wasFiring := e.firing[rule.RuleId]
	switch {
	case breached && !wasFiring:
		e.firing[rule.RuleId] = true
	case !breached && wasFiring:
		delete(e.firing, rule.RuleId)
	default:
		e.mu.Unlock()
		return nil, nil // durum degismedi
	}
	e.mu.Unlock()

	alert := buildAlert(rule, cond, value, !breached)

	if err := e.storeAlert(ctx, alert); err != nil {
		e.logger.Error("Alarm kaydedilemedi", zap.Error(err))
	}
	if rule.WebhookUrl != "" {
		e.notify(ctx, rule.WebhookUrl, alert)
	}

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
func (e *AlertEngine) fetchValues(ctx context.Context, rule *pb.AlertRule, start, end time.Time) ([]float64, error) {
	iter := e.session.Query(`
		SELECT value FROM metrics
		WHERE service_name = ? AND metric_name = ?
		  AND timestamp >= ? AND timestamp <= ?`,
		rule.ServiceName, rule.MetricName, start.UnixMilli(), end.UnixMilli(),
	).WithContext(ctx).Iter()

	var (
		v      float64
		values []float64
	)
	for iter.Scan(&v) {
		values = append(values, v)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("metrik okunamadi: %w", err)
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
func (e *AlertEngine) ActiveRuleIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.firing))
	for id := range e.firing {
		out = append(out, id)
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
