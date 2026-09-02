//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nisah/pulse-metrics/internal/collector"
	"github.com/nisah/pulse-metrics/internal/logging"
	pb "github.com/nisah/pulse-metrics/internal/proto"
	"github.com/nisah/pulse-metrics/internal/tracing"
)

// startFullCollector: metrik + trace + log tuketimi ve alarm motoru acik.
func startFullCollector(t *testing.T, ctx context.Context, metricsTopic, tracesTopic, logsTopic, suffix string) string {
	t.Helper()

	grpcPort := freePort(t)
	c, err := collector.NewCollector(&collector.Config{
		KafkaBrokers:  []string{kafkaAddr},
		ScyllaAddr:    scyllaAddr,
		GRPCPort:      grpcPort,
		HealthAddr:    "127.0.0.1:" + freePort(t),
		Topic:         metricsTopic,
		GroupID:       "itest-m-" + suffix,
		TracesTopic:   tracesTopic,
		TracesGroupID: "itest-t-" + suffix,
		LogsTopic:     logsTopic,
		LogsGroupID:   "itest-l-" + suffix,
		// Arka plan degerlendirmesi testin zamanlamasina karismasin;
		// kurallari EvaluateRules ile acikca tetikliyoruz.
		DisableAlerts: false,
		AlertInterval: time.Hour,
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
			t.Log("uyari: collector 45 sn icinde kapanmadi")
		}
	})

	addr := "127.0.0.1:" + grpcPort
	waitForTCP(t, addr, 30*time.Second)
	return addr
}

func logClient(t *testing.T, addr string) pb.LogServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gRPC baglantisi kurulamadi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewLogServiceClient(conn)
}

func alertClient(t *testing.T, addr string) pb.AlertServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gRPC baglantisi kurulamadi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewAlertServiceClient(conn)
}

func cleanupLogs(t *testing.T, sess *gocql.Session, service string) {
	t.Cleanup(func() {
		bucket := collector.TimeBucket(time.Now())
		prev := collector.TimeBucket(time.Now().Add(-time.Hour))
		for _, b := range []string{bucket, prev} {
			_ = sess.Query(`DELETE FROM logs WHERE service_name = ? AND time_bucket = ?`,
				service, b).Exec()
			_ = sess.Query(`DELETE FROM alerts WHERE service_name = ? AND time_bucket = ?`,
				service, b).Exec()
		}
		_ = sess.Query(`DELETE FROM log_services WHERE service_name = ?`, service).Exec()
	})
}

// TestLogPipelineVeTraceKorelasyonu: loglar Kafka'dan ScyllaDB'ye ulasiyor
// ve ayni trace_id ile trace'e baglaniyor mu?
func TestLogPipelineVeTraceKorelasyonu(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-p3m-" + suffix
	tracesTopic := "pulse-itest-p3t-" + suffix
	logsTopic := "pulse-itest-p3l-" + suffix
	svc := "itest-log-" + suffix

	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)
	createTopic(t, logsTopic)

	sess := scyllaSession(t)
	cleanupLogs(t, sess, svc)
	cleanupTraceService(t, sess, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startFullCollector(t, ctx, metricsTopic, tracesTopic, logsTopic, suffix)

	// Tracer + logger: log kayitlari span baglamindan trace_id alacak.
	traceExp := tracing.NewBatchExporter(tracing.BatchExporterConfig{
		KafkaBrokers:  []string{kafkaAddr},
		Topic:         tracesTopic,
		ServiceName:   svc,
		BatchSize:     4,
		FlushInterval: 300 * time.Millisecond,
		OnError:       func(err error) { t.Logf("trace export: %v", err) },
	})
	tracer := tracing.NewTracer(svc, traceExp)

	logger := logging.New(logging.Config{
		KafkaBrokers:  []string{kafkaAddr},
		Topic:         logsTopic,
		ServiceName:   svc,
		InstanceID:    "itest-1",
		LoggerName:    "itest",
		MinLevel:      pb.LogLevel_LEVEL_INFO,
		BatchSize:     4,
		FlushInterval: 300 * time.Millisecond,
		OnError:       func(err error) { t.Logf("log export: %v", err) },
	})

	// Bir span icinde birkac log uret.
	spanCtx, span := tracer.Start(context.Background(), "islem")
	wantTraceID := span.TraceID()

	logger.Info(spanCtx, "islem basladi", map[string]string{"step": "start"})
	logger.Warn(spanCtx, "yavas yanit: 250 ms", nil)
	logger.Error(spanCtx, "islem basarisiz: siparis ord-000123", nil,
		map[string]string{"order_id": "ord-000123"})
	span.End()

	// Span disinda bir log daha: trace_id bos olmali.
	logger.Info(context.Background(), "span disinda bir mesaj", nil)

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	_ = tracer.Shutdown(flushCtx)
	_ = logger.Shutdown(flushCtx)
	flushCancel()

	lc := logClient(t, grpcAddr)

	// --- trace korelasyonu ---
	var traceLogs *pb.LogsQueryResponse
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := lc.GetTraceLogs(qCtx, &pb.GetTraceLogsRequest{TraceId: wantTraceID})
		qCancel()
		if err == nil && len(resp.GetLogs()) >= 3 {
			traceLogs = resp
			break
		}
		time.Sleep(time.Second)
	}
	if traceLogs == nil {
		t.Fatalf("trace %s icin 90 sn icinde 3 log gelmedi", wantTraceID)
	}

	for _, l := range traceLogs.GetLogs() {
		if l.GetTraceId() != wantTraceID {
			t.Errorf("log farkli trace'te: %s", l.GetTraceId())
		}
		if l.GetSpanId() == "" {
			t.Error("span_id bos")
		}
	}
	// Loglar zaman sirasinda gelmeli.
	for i := 1; i < len(traceLogs.GetLogs()); i++ {
		if traceLogs.GetLogs()[i].GetTimestampMs() < traceLogs.GetLogs()[i-1].GetTimestampMs() {
			t.Error("trace loglari zaman sirasinda degil")
		}
	}

	// --- seviye filtresi ---
	qCtx, qCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer qCancel()
	errLogs, err := lc.QueryLogs(qCtx, &pb.LogsQueryRequest{
		ServiceName: svc,
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		Levels:      []pb.LogLevel{pb.LogLevel_LEVEL_ERROR},
	})
	if err != nil {
		t.Fatalf("QueryLogs hatasi: %v", err)
	}
	if len(errLogs.GetLogs()) != 1 {
		t.Fatalf("1 ERROR kaydi bekleniyordu, %d alindi", len(errLogs.GetLogs()))
	}
	if errLogs.GetLogs()[0].GetAttributes()["order_id"] != "ord-000123" {
		t.Errorf("oznitelik saklanmadi: %v", errLogs.GetLogs()[0].GetAttributes())
	}

	// --- metin aramasi ---
	textLogs, err := lc.QueryLogs(qCtx, &pb.LogsQueryRequest{
		ServiceName: svc,
		Query:       "yavas",
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("metin aramasi hatasi: %v", err)
	}
	if len(textLogs.GetLogs()) != 1 {
		t.Errorf("metin aramasi 1 sonuc dondurmeliydi, %d", len(textLogs.GetLogs()))
	}

	// --- ListLogServices ---
	svcs, err := lc.ListLogServices(qCtx, &pb.ListLogServicesRequest{})
	if err != nil {
		t.Fatalf("ListLogServices hatasi: %v", err)
	}
	found := false
	for _, s := range svcs.GetServices() {
		if s == svc {
			found = true
		}
	}
	if !found {
		t.Errorf("%s log servisleri listesinde yok", svc)
	}

	// --- kalip tespiti: degisken parcalar maskelenmeli ---
	pat, err := lc.DetectPatterns(qCtx, &pb.DetectPatternsRequest{
		ServiceName: svc,
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("DetectPatterns hatasi: %v", err)
	}
	if len(pat.GetPatterns()) == 0 {
		t.Fatal("kalip bulunamadi")
	}
	sawMasked := false
	for _, p := range pat.GetPatterns() {
		if p.GetPattern() == "islem basarisiz: siparis ord-<N>" {
			sawMasked = true
			if p.GetErrorCorrelation() != 1.0 {
				t.Errorf("bu kalip %%100 hata korelasyonuna sahip olmali, %v", p.GetErrorCorrelation())
			}
		}
	}
	if !sawMasked {
		var got []string
		for _, p := range pat.GetPatterns() {
			got = append(got, p.GetPattern())
		}
		t.Errorf("maskelenmis kalip bulunamadi, gelenler: %v", got)
	}
}

// TestTopolojiIngestZamaninda: kenarlar ornekleme ile degil, ingest
// sirasinda peer.service ozniteliginden yaziliyor mu?
func TestTopolojiIngestZamaninda(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-topom-" + suffix
	tracesTopic := "pulse-itest-topot-" + suffix
	logsTopic := "pulse-itest-topol-" + suffix
	upSvc := "itest-up-" + suffix
	downSvc := "itest-down-" + suffix

	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)
	createTopic(t, logsTopic)

	sess := scyllaSession(t)
	cleanupTraceService(t, sess, upSvc)
	cleanupTraceService(t, sess, downSvc)
	t.Cleanup(func() {
		// Testin basi ile sonu farkli saat kovalarina dusebilir; ikisini de sil.
		for _, b := range []string{
			collector.TimeBucket(time.Now()),
			collector.TimeBucket(time.Now().Add(-time.Hour)),
		} {
			_ = sess.Query(`DELETE FROM service_edges WHERE caller_service = ? AND callee_service = ? AND time_bucket = ?`,
				upSvc, downSvc, b).Exec()
			_ = sess.Query(`DELETE FROM edge_pairs WHERE time_bucket = ? AND caller_service = ? AND callee_service = ?`,
				b, upSvc, downSvc).Exec()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startFullCollector(t, ctx, metricsTopic, tracesTopic, logsTopic, suffix)

	expCfg := func(name string) tracing.BatchExporterConfig {
		return tracing.BatchExporterConfig{
			KafkaBrokers:  []string{kafkaAddr},
			Topic:         tracesTopic,
			ServiceName:   name,
			BatchSize:     4,
			FlushInterval: 300 * time.Millisecond,
			OnError:       func(err error) { t.Logf("[%s] export: %v", name, err) },
		}
	}

	downTracer := tracing.NewTracer(downSvc, tracing.NewBatchExporter(expCfg(downSvc)))
	downstream := httptest.NewServer(downTracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(3 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		})))
	defer downstream.Close()

	upTracer := tracing.NewTracer(upSvc, tracing.NewBatchExporter(expCfg(upSvc)))
	upstream := httptest.NewServer(upTracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, downstream.URL, nil)
			resp, err := upTracer.Client().Do(req)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_ = resp.Body.Close()
			w.WriteHeader(http.StatusOK)
		})))
	defer upstream.Close()

	for i := 0; i < 6; i++ {
		resp, err := http.Get(upstream.URL + "/api")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	_ = upTracer.Shutdown(flushCtx)
	_ = downTracer.Shutdown(flushCtx)
	flushCancel()

	tc := traceClient(t, grpcAddr)

	// Span'ler partiler halinde geliyor; kenarin TAM sayima ulasmasini
	// bekle. Ilk gorunuste durmak kismen ingest edilmis bir sayim okur.
	const wantCalls = 6
	var edge *pb.ServiceDependency
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		qCtx, qCancel := context.WithTimeout(context.Background(), 20*time.Second)
		topo, err := tc.GetTopology(qCtx, &pb.TopologyRequest{
			StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
			EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		})
		qCancel()
		if err == nil {
			for _, e := range topo.GetEdges() {
				if e.GetCallerService() == upSvc && e.GetCalleeService() == downSvc {
					edge = e
				}
			}
		}
		if edge != nil && edge.GetCallCount() >= wantCalls {
			break
		}
		time.Sleep(time.Second)
	}
	if edge == nil {
		t.Fatalf("%s -> %s kenari 90 sn icinde olusmadi", upSvc, downSvc)
	}

	// Ornekleme degil tam sayim: 6 istegin hepsi sayilmali.
	if edge.GetCallCount() != wantCalls {
		t.Errorf("kenar cagri sayisi %d, %d bekleniyordu (tam sayim olmali)",
			edge.GetCallCount(), wantCalls)
	}
	if edge.GetP95LatencyMs() < 0 {
		t.Error("p95 negatif olamaz")
	}
}

// TestAlarmMotoru: kural olusturma, tetikleme, tekrar bastirma,
// cozulme ve webhook bildirimi.
func TestAlarmMotoru(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-am-" + suffix
	tracesTopic := "pulse-itest-at-" + suffix
	logsTopic := "pulse-itest-al-" + suffix
	svc := "itest-alert-" + suffix
	metric := "test.value"

	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)
	createTopic(t, logsTopic)

	sess := scyllaSession(t)
	cleanupLogs(t, sess, svc)
	t.Cleanup(func() {
		_ = sess.Query(`DELETE FROM metrics WHERE service_name = ? AND metric_name = ?`,
			svc, metric).Exec()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startFullCollector(t, ctx, metricsTopic, tracesTopic, logsTopic, suffix)
	ac := alertClient(t, grpcAddr)

	// Webhook alicisi.
	var (
		hookMu   sync.Mutex
		hookBody []map[string]interface{}
	)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]interface{}
		_ = json.Unmarshal(b, &m)
		hookMu.Lock()
		hookBody = append(hookBody, m)
		hookMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	// Metrikleri dogrudan yaz: alarm motoru metrics tablosunu okuyor.
	writeMetric := func(value float64, ago time.Duration) {
		ts := time.Now().Add(-ago).UnixMilli()
		if err := sess.Query(`
			INSERT INTO metrics (service_name, metric_name, timestamp, instance_id,
			                     type, tags, labels, value)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			svc, metric, ts, "itest-1", "GAUGE", nil, nil, value).Exec(); err != nil {
			t.Fatalf("metrik yazilamadi: %v", err)
		}
	}

	// Once sakin degerler.
	for i := 0; i < 5; i++ {
		writeMetric(10, time.Duration(i)*time.Second)
	}

	// --- gecersiz kosul reddedilmeli ---
	vCtx, vCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, err := ac.CreateRule(vCtx, &pb.AlertRule{
		ServiceName: svc, MetricName: metric, Condition: "bu gecersiz", Enabled: true,
	})
	vCancel()
	if err == nil {
		t.Error("gecersiz kosul reddedilmeliydi")
	}

	// --- kural olustur ---
	cCtx, cCancel := context.WithTimeout(context.Background(), 15*time.Second)
	created, err := ac.CreateRule(cCtx, &pb.AlertRule{
		Name:            "Test esigi",
		ServiceName:     svc,
		MetricName:      metric,
		Condition:       "max > 100",
		DurationSeconds: 600,
		WebhookUrl:      hook.URL,
		Enabled:         true,
		Severity:        pb.AlertSeverity_CRITICAL,
	})
	cCancel()
	if err != nil {
		t.Fatalf("CreateRule hatasi: %v", err)
	}
	ruleID := created.GetRuleId()
	t.Cleanup(func() {
		dCtx, dCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dCancel()
		_, _ = ac.DeleteRule(dCtx, &pb.DeleteRuleRequest{RuleId: ruleID})
	})

	// --- henuz tetiklenmemeli (max 10 < 100) ---
	eCtx, eCancel := context.WithTimeout(context.Background(), 60*time.Second)
	resp, err := ac.EvaluateRules(eCtx, &pb.EvaluateRulesRequest{RuleId: ruleID})
	eCancel()
	if err != nil {
		t.Fatalf("EvaluateRules hatasi: %v", err)
	}
	if len(resp.GetFired()) != 0 {
		t.Errorf("esik asilmadan tetiklenmemeliydi: %v", resp.GetFired())
	}

	// --- esigi as ---
	writeMetric(500, 0)

	eCtx, eCancel = context.WithTimeout(context.Background(), 60*time.Second)
	resp, err = ac.EvaluateRules(eCtx, &pb.EvaluateRulesRequest{RuleId: ruleID})
	eCancel()
	if err != nil {
		t.Fatalf("EvaluateRules hatasi: %v", err)
	}
	if len(resp.GetFired()) != 1 {
		t.Fatalf("1 alarm bekleniyordu, %d alindi", len(resp.GetFired()))
	}
	fired := resp.GetFired()[0]
	if fired.GetResolved() {
		t.Error("ilk alarm resolved olmamali")
	}
	if fired.GetMetricValue() != 500 {
		t.Errorf("alarm degeri %v, 500 bekleniyordu", fired.GetMetricValue())
	}
	if fired.GetSeverity() != pb.AlertSeverity_CRITICAL {
		t.Errorf("onem %v, CRITICAL bekleniyordu", fired.GetSeverity())
	}

	// --- tekrar degerlendirme spam uretmemeli ---
	eCtx, eCancel = context.WithTimeout(context.Background(), 60*time.Second)
	resp, err = ac.EvaluateRules(eCtx, &pb.EvaluateRulesRequest{RuleId: ruleID})
	eCancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetFired()) != 0 {
		t.Errorf("durum degismeden tekrar alarm uretilmemeli, %d uretildi", len(resp.GetFired()))
	}

	// --- webhook geldi mi? ---
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		hookMu.Lock()
		n := len(hookBody)
		hookMu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	hookMu.Lock()
	gotHooks := len(hookBody)
	var hookRule string
	if gotHooks > 0 {
		hookRule, _ = hookBody[0]["rule_id"].(string)
	}
	hookMu.Unlock()

	if gotHooks == 0 {
		t.Error("webhook bildirimi gelmedi")
	} else if hookRule != ruleID {
		t.Errorf("webhook rule_id = %q, beklenen %q", hookRule, ruleID)
	}

	// --- alarm kaydedildi mi? ---
	lCtx, lCancel := context.WithTimeout(context.Background(), 30*time.Second)
	alerts, err := ac.ListAlerts(lCtx, &pb.ListAlertsRequest{
		ServiceName: svc,
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
	})
	lCancel()
	if err != nil {
		t.Fatalf("ListAlerts hatasi: %v", err)
	}
	if len(alerts.GetAlerts()) == 0 {
		t.Error("alarm kaydedilmemis")
	}
	activeFound := false
	for _, id := range alerts.GetActiveRuleIds() {
		if id == ruleID {
			activeFound = true
		}
	}
	if !activeFound {
		t.Error("kural aktif tetiklenmis olarak isaretlenmeli")
	}

	// --- ListRules ---
	rCtx, rCancel := context.WithTimeout(context.Background(), 15*time.Second)
	rules, err := ac.ListRules(rCtx, &pb.ListRulesRequest{ServiceName: svc})
	rCancel()
	if err != nil {
		t.Fatalf("ListRules hatasi: %v", err)
	}
	if len(rules.GetRules()) != 1 {
		t.Errorf("1 kural bekleniyordu, %d", len(rules.GetRules()))
	}
}
