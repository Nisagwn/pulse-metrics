//go:build integration

// Faz 4 entegrasyon testleri: uretime hazirlik.
//
// Uc iddiayi dogruluyorlar:
//  1. Collector yatay olceklenebilir - iki surec ayni alarmi iki kez
//     bildirmiyor (paylasilan durum + LWT).
//  2. metrics tablosu artik saatlik kovalarda ve kova sinirini asan
//     sorgular dogru calisiyor.
//  3. Collector kendi olculerini yayinliyor - izleme sisteminin kendisi
//     de izlenebiliyor.
package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/nisah/pulse-metrics/internal/collector"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// startCollectorWithHealth: startFullCollector'in saglik adresini de
// donduren surumu. /metrics ucunu test edebilmek icin gerekli.
func startCollectorWithHealth(t *testing.T, ctx context.Context, suffix string) (grpcAddr, healthAddr string) {
	t.Helper()

	metricsTopic := "pulse-itest-p4m-" + suffix
	tracesTopic := "pulse-itest-p4t-" + suffix
	logsTopic := "pulse-itest-p4l-" + suffix
	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)
	createTopic(t, logsTopic)

	grpcPort := freePort(t)
	healthPort := freePort(t)
	healthAddr = "127.0.0.1:" + healthPort

	c, err := collector.NewCollector(&collector.Config{
		KafkaBrokers:  []string{kafkaAddr},
		ScyllaAddr:    scyllaAddr,
		GRPCPort:      grpcPort,
		HealthAddr:    healthAddr,
		InstanceID:    "itest-" + suffix,
		Topic:         metricsTopic,
		GroupID:       "itest-p4m-" + suffix,
		TracesTopic:   tracesTopic,
		TracesGroupID: "itest-p4t-" + suffix,
		LogsTopic:     logsTopic,
		LogsGroupID:   "itest-p4l-" + suffix,
		AlertInterval: time.Hour, // arka plan turu testin zamanlamasina karismasin
	})
	if err != nil {
		t.Fatalf("collector kurulamadi: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(45 * time.Second):
			t.Error("collector 45 sn icinde kapanmadi")
		}
	})

	grpcAddr = "127.0.0.1:" + grpcPort
	waitForTCP(t, grpcAddr, 30*time.Second)
	waitForTCP(t, healthAddr, 30*time.Second)
	return grpcAddr, healthAddr
}

// writeMetricAt: metrics tablosuna dogrudan yazar (Faz 4 semasi).
func writeMetricAt(t *testing.T, sess *gocql.Session, svc, metric string, ts int64, value float64) {
	t.Helper()
	if err := sess.Query(`
		INSERT INTO metrics (service_name, metric_name, time_bucket, timestamp,
		                     instance_id, type, tags, labels, value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		svc, metric, collector.TimeBucket(time.UnixMilli(ts)), ts,
		"itest-1", "GAUGE", nil, nil, value).Exec(); err != nil {
		t.Fatalf("metrik yazilamadi: %v", err)
	}
}

func cleanupMetric(t *testing.T, sess *gocql.Session, svc, metric string, buckets ...string) {
	t.Cleanup(func() {
		for _, b := range buckets {
			_ = sess.Query(`DELETE FROM metrics WHERE service_name = ? AND metric_name = ? AND time_bucket = ?`,
				svc, metric, b).Exec()
		}
	})
}

// TestSemaKovaliDogrulanir: metrics tablosunun partition key'inde
// time_bucket var mi?
//
// Faz 1'den beri tasinan borcun kapandiginin en dogrudan kaniti. Ayrica
// collector'in acilista sema dogrulamasi yaptigini da gosterir: eski
// semali bir tabloyla acilis hata verirdi, bu test acilabildigine gore
// sema guncel.
func TestSemaKovaliDogrulanir(t *testing.T) {
	sess := scyllaSession(t)

	iter := sess.Query(`
		SELECT column_name FROM system_schema.columns
		WHERE keyspace_name = 'pulse' AND table_name = 'metrics' AND kind = 'partition_key'
		ALLOW FILTERING`).Iter()

	var (
		name string
		cols []string
	)
	for iter.Scan(&name) {
		cols = append(cols, name)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("sema okunamadi: %v", err)
	}

	want := map[string]bool{"service_name": false, "metric_name": false, "time_bucket": false}
	for _, c := range cols {
		if _, ok := want[c]; ok {
			want[c] = true
		}
	}
	for col, found := range want {
		if !found {
			t.Errorf("partition key'de %q yok (bulunanlar: %v)", col, cols)
		}
	}
	if len(cols) != 3 {
		t.Errorf("partition key 3 kolon olmali, %d bulundu: %v", len(cols), cols)
	}
}

// TestKovaSinirlariniAsanSorgu: iki farkli saatlik kovaya yazilan veri
// tek bir sorguda birlikte donmeli.
//
// Kovalama sessizce veri kaybetmenin kolay bir yolu: okuma yolu yalnizca
// tek kovaya bakarsa saat basinda grafikler yarilanir ve kimse fark
// etmez. Bu test tam olarak o hatayi yakalar.
func TestKovaSinirlariniAsanSorgu(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%100000)
	grpcAddr, _ := startCollectorWithHealth(t, ctx, suffix)

	sess := scyllaSession(t)
	svc := "itest-bucket-" + suffix
	metric := "test.bucket"

	now := time.Now()
	// Bilerek uc ayri saate yaziyoruz: simdi, 1 saat once, 2 saat once.
	stamps := []time.Time{now, now.Add(-time.Hour), now.Add(-2 * time.Hour)}
	buckets := make([]string, 0, len(stamps))
	for i, ts := range stamps {
		writeMetricAt(t, sess, svc, metric, ts.UnixMilli(), float64(i+1)*10)
		buckets = append(buckets, collector.TimeBucket(ts))
	}
	cleanupMetric(t, sess, svc, metric, buckets...)

	// Kovalarin gercekten farkli olmasi testin on kosulu.
	if buckets[0] == buckets[1] || buckets[1] == buckets[2] {
		t.Fatalf("kovalar farkli olmaliydi: %v", buckets)
	}

	client := metricsClient(t, grpcAddr)
	qCtx, qCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer qCancel()

	resp, err := client.Query(qCtx, &pb.MetricsQueryRequest{
		ServiceName: svc,
		MetricName:  metric,
		StartTimeMs: now.Add(-3 * time.Hour).UnixMilli(),
		EndTimeMs:   now.Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Query hatasi: %v", err)
	}

	var points int
	for _, s := range resp.GetSeries() {
		points += len(s.GetPoints())
	}
	if points != 3 {
		t.Fatalf("3 nokta bekleniyordu (3 ayri kovadan), %d alindi", points)
	}

	// Dar aralik: sadece son kova. Kova dongusu araligi da dogru
	// uygulamali, "hepsini getir" olmamali.
	resp, err = client.Query(qCtx, &pb.MetricsQueryRequest{
		ServiceName: svc,
		MetricName:  metric,
		StartTimeMs: now.Add(-30 * time.Minute).UnixMilli(),
		EndTimeMs:   now.Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("dar aralik Query hatasi: %v", err)
	}
	points = 0
	for _, s := range resp.GetSeries() {
		points += len(s.GetPoints())
	}
	if points != 1 {
		t.Errorf("dar aralikta 1 nokta bekleniyordu, %d alindi", points)
	}
}

// TestPaylasilanAlarmDurumu: FAZ 4'UN ASIL IDDIASI.
//
// Iki alarm motoru ayni kurali ayni anda degerlendiriyor. Faz 3'te ikisi
// de "bu yeni bir ihlal" deyip webhook gonderirdi. Faz 4'te durum
// veritabaninda paylasildigi ve gecis LWT (IF cumlesi) ile yapildigi
// icin yalnizca BIRI kazanir.
//
// Test hem tetiklenme hem cozulme yonunu kontrol ediyor: ikisinde de
// toplam bildirim sayisi tam olarak 1 olmali.
func TestPaylasilanAlarmDurumu(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%100000)
	grpcAddr, _ := startCollectorWithHealth(t, ctx, suffix)

	sess := scyllaSession(t)
	svc := "itest-shared-" + suffix
	metric := "test.shared"

	now := time.Now()
	cleanupMetric(t, sess, svc, metric,
		collector.TimeBucket(now), collector.TimeBucket(now.Add(-time.Hour)))

	// Kurali gRPC uzerinden yarat: dogrulamadan gecsin.
	ac := alertClient(t, grpcAddr)
	cCtx, cCancel := context.WithTimeout(context.Background(), 20*time.Second)
	created, err := ac.CreateRule(cCtx, &pb.AlertRule{
		Name:        "Paylasilan durum testi",
		ServiceName: svc,
		MetricName:  metric,
		Condition:   "max > 100",
		// Kisa pencere: cozulme yonunu birkac saniyede test edebilmek icin.
		DurationSeconds: 5,
		Enabled:         true,
		Severity:        pb.AlertSeverity_WARNING,
	})
	cCancel()
	if err != nil {
		t.Fatalf("CreateRule hatasi: %v", err)
	}
	ruleID := created.GetRuleId()
	// Temizlik gRPC ILE DEGIL, dogrudan CQL ile yapiliyor.
	//
	// Sebep ince: testin sonunda once `defer cancel()` calisir ve
	// collector kapanir, t.Cleanup fonksiyonlari ondan SONRA calisir.
	// Yani cleanup icinde DeleteRule cagirmak, olu bir gRPC sunucusuna
	// istek atmak demek - sessizce basarisiz olur ve kural veritabaninda
	// kalir. (Bu tam olarak basimiza geldi: entegrasyon testlerinden
	// kalan itest kurallari canli panelde gorundu.)
	t.Cleanup(func() {
		_ = sess.Query(`DELETE FROM alert_rules WHERE rule_id = ?`, ruleID).Exec()
		_ = sess.Query(`DELETE FROM alert_state WHERE rule_id = ?`, ruleID).Exec()
	})
	t.Cleanup(func() {
		for _, b := range []string{collector.TimeBucket(time.Now()),
			collector.TimeBucket(time.Now().Add(-time.Hour))} {
			_ = sess.Query(`DELETE FROM alerts WHERE service_name = ? AND time_bucket = ?`,
				svc, b).Exec()
		}
	})

	logger := zap.NewNop()
	engineA := collector.NewAlertEngine(sess, logger, time.Hour, "itest-collector-A")
	engineB := collector.NewAlertEngine(sess, logger, time.Hour, "itest-collector-B")

	evaluate := func(label string) int {
		eCtx, eCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer eCancel()

		firedA, err := engineA.EvaluateAll(eCtx, ruleID)
		if err != nil {
			t.Fatalf("[%s] A degerlendirmesi: %v", label, err)
		}
		firedB, err := engineB.EvaluateAll(eCtx, ruleID)
		if err != nil {
			t.Fatalf("[%s] B degerlendirmesi: %v", label, err)
		}
		return len(firedA) + len(firedB)
	}

	// --- 1) Esigi as: iki motordan TOPLAM bir bildirim cikmali ---
	writeMetricAt(t, sess, svc, metric, time.Now().UnixMilli(), 500)

	if n := evaluate("tetiklenme"); n != 1 {
		t.Fatalf("tetiklenmede toplam 1 bildirim bekleniyordu, %d alindi "+
			"(Faz 3 davranisi 2 olurdu: her collector kendi belleginden karar verirdi)", n)
	}

	// Durum paylasildigi icin iki motor da ayni cevabi vermeli.
	for label, e := range map[string]*collector.AlertEngine{"A": engineA, "B": engineB} {
		active := e.ActiveRuleIDs()
		found := false
		for _, id := range active {
			if id == ruleID {
				found = true
			}
		}
		if !found {
			t.Errorf("motor %s kurali tetiklenmis gormedi: %v", label, active)
		}
	}

	// --- 2) Tekrar degerlendir: durum degismedi, kimse bildirmemeli ---
	if n := evaluate("tekrar"); n != 0 {
		t.Errorf("durum degismeden bildirim cikmamaliydi, %d alindi (alarm spam)", n)
	}

	// --- 3) Cozulme: pencere kayinca ihlal ortadan kalkar ---
	// Kural penceresi 5 saniye; 6 saniye bekleyip dusuk bir deger
	// yaziyoruz, boylece pencere yalnizca dusuk degeri gorur.
	time.Sleep(6 * time.Second)
	writeMetricAt(t, sess, svc, metric, time.Now().UnixMilli(), 10)

	if n := evaluate("cozulme"); n != 1 {
		t.Errorf("cozulmede toplam 1 bildirim bekleniyordu, %d alindi", n)
	}

	for label, e := range map[string]*collector.AlertEngine{"A": engineA, "B": engineB} {
		for _, id := range e.ActiveRuleIDs() {
			if id == ruleID {
				t.Errorf("motor %s cozulmus kurali hala tetiklenmis goruyor", label)
			}
		}
	}
}

// TestOlcumUcu: collector kendi olculerini yayinliyor mu?
//
// Bir izleme sisteminin en can sikici arizasi sessizce durmasidir:
// panelde grafik duz cizgi olur ve bunun "trafik yok" mu "collector oldu"
// mu oldugunu kimse bilemez. /metrics ucu bu ayrimi mumkun kiliyor.
func TestOlcumUcu(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%100000)
	_, healthAddr := startCollectorWithHealth(t, ctx, suffix)

	body := httpGet(t, "http://"+healthAddr+"/metrics")

	// Kendi olculerimiz.
	for _, want := range []string{
		"pulse_build_info",
		"pulse_collector_kafka_lag",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics ciktisinda %q yok", want)
		}
	}

	// Prometheus istemcisinden ucretsiz gelen Go calisma zamani olculeri.
	// Bir goroutine sizintisini once burada gorursun.
	for _, want := range []string{"go_goroutines", "go_memstats_heap_inuse_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics ciktisinda Go calisma zamani olcusu %q yok", want)
		}
	}

	// Surum etiketi dolu olmali: "hangi surum calisiyor?" sorusunun cevabi.
	if !strings.Contains(body, `component="collector"`) {
		t.Error("pulse_build_info component etiketi yok")
	}

	// Saglik uclari da ayni portta durmali.
	if h := httpGet(t, "http://"+healthAddr+"/healthz"); !strings.Contains(h, `"status":"ok"`) {
		t.Errorf("/healthz beklenmeyen cevap: %s", h)
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("istek olusturulamadi: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s istegi basarisiz: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("cevap okunamadi: %v", err)
	}
	return string(b)
}
