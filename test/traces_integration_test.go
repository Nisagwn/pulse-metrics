//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nisah/pulse-metrics/internal/collector"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
	"github.com/nisah/pulse-metrics/pkg/tracing"
)

// startTraceCollector: trace tuketimi acik bir collector baslatir.
func startTraceCollector(t *testing.T, ctx context.Context, metricsTopic, tracesTopic, suffix string) string {
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
	})
	if err != nil {
		t.Fatalf("collector kurulamadi: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Log("uyari: collector 20 sn icinde kapanmadi")
		}
	})

	addr := "127.0.0.1:" + grpcPort
	waitForTCP(t, addr, 30*time.Second)
	return addr
}

func traceClient(t *testing.T, addr string) pb.TraceServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("gRPC baglantisi kurulamadi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewTraceServiceClient(conn)
}

func cleanupTraceService(t *testing.T, sess *gocql.Session, service string) {
	t.Cleanup(func() {
		iter := sess.Query(`SELECT time_bucket, trace_id FROM trace_index WHERE service_name = ? ALLOW FILTERING`,
			service).Iter()
		var bucket, traceID string
		var buckets []string
		var traces []string
		for iter.Scan(&bucket, &traceID) {
			buckets = append(buckets, bucket)
			traces = append(traces, traceID)
		}
		_ = iter.Close()

		for i := range buckets {
			_ = sess.Query(`DELETE FROM trace_index WHERE service_name = ? AND time_bucket = ?`,
				service, buckets[i]).Exec()
			_ = sess.Query(`DELETE FROM spans WHERE trace_id = ?`, traces[i]).Exec()
		}
		_ = sess.Query(`DELETE FROM service_ops WHERE service_name = ?`, service).Exec()
	})
}

// TestTracePipelineUctanUca: iki HTTP servisi -> Kafka -> collector -> ScyllaDB
// -> gRPC. Trace'in servisler arasinda birlestigini uctan uca dogrular.
func TestTracePipelineUctanUca(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-tm-" + suffix
	tracesTopic := "pulse-itest-tr-" + suffix
	upSvc := "itest-up-" + suffix
	downSvc := "itest-down-" + suffix

	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)

	sess := scyllaSession(t)
	cleanupTraceService(t, sess, upSvc)
	cleanupTraceService(t, sess, downSvc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startTraceCollector(t, ctx, metricsTopic, tracesTopic, suffix)

	// --- iki servisli kucuk bir mimari ---
	expCfg := func(name string) tracing.BatchExporterConfig {
		return tracing.BatchExporterConfig{
			KafkaBrokers:  []string{kafkaAddr},
			Topic:         tracesTopic,
			ServiceName:   name,
			InstanceID:    "itest-1",
			BatchSize:     4,
			FlushInterval: 300 * time.Millisecond,
			// Sessiz veri kaybi testin neden basarisiz oldugunu gizler.
			OnError: func(err error) { t.Logf("[%s] export hatasi: %v", name, err) },
		}
	}

	downExp := tracing.NewBatchExporter(expCfg(downSvc))
	downTracer := tracing.NewTracer(downSvc, downExp, tracing.WithInstanceID("itest-1"))
	downstream := httptest.NewServer(downTracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, inner := downTracer.Start(r.Context(), "db-query",
				tracing.WithSpanKind(pb.SpanKind_SPAN_KIND_CLIENT))
			time.Sleep(5 * time.Millisecond)
			inner.End()
			w.WriteHeader(http.StatusOK)
		})))
	defer downstream.Close()

	upExp := tracing.NewBatchExporter(expCfg(upSvc))
	upTracer := tracing.NewTracer(upSvc, upExp, tracing.WithInstanceID("itest-1"))
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

	// Birkac istek at.
	var wantTraceID string
	for i := 0; i < 5; i++ {
		resp, err := http.Get(upstream.URL + "/checkout")
		if err != nil {
			t.Fatalf("istek basarisiz: %v", err)
		}
		if id := resp.Header.Get("X-Trace-Id"); id != "" && wantTraceID == "" {
			wantTraceID = id
		}
		_ = resp.Body.Close()
	}
	if wantTraceID == "" {
		t.Fatal("X-Trace-Id basligi donmedi")
	}

	// Bekleyen span'leri gonder.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_ = upTracer.Shutdown(flushCtx)
	_ = downTracer.Shutdown(flushCtx)
	flushCancel()

	client := traceClient(t, grpcAddr)

	// Trace 4 span'e ulasana kadar bekle:
	// up:SERVER, up:CLIENT, down:SERVER, down:db-query
	var trace *pb.Trace
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
		tr, err := client.GetTrace(qCtx, &pb.GetTraceRequest{TraceId: wantTraceID})
		qCancel()
		if err == nil && len(tr.GetSpans()) >= 4 {
			trace = tr
			break
		}
		time.Sleep(time.Second)
	}
	if trace == nil {
		t.Fatalf("trace %s 90 sn icinde 4 span'e ulasmadi", wantTraceID)
	}

	// --- dogrulamalar ---
	byID := map[string]*pb.Span{}
	services := map[string]bool{}
	for _, s := range trace.GetSpans() {
		byID[s.GetSpanId()] = s
		services[s.GetServiceName()] = true
		if s.GetTraceId() != wantTraceID {
			t.Errorf("span farkli trace'te: %s", s.GetTraceId())
		}
	}

	if !services[upSvc] || !services[downSvc] {
		t.Fatalf("iki servis de trace'te olmali, bulunanlar: %v", services)
	}

	// Servisler arasi ebeveyn baglantisi: down:SERVER'in ebeveyni
	// up:CLIENT olmali.
	var upClient, downServer *pb.Span
	for _, s := range trace.GetSpans() {
		if s.GetServiceName() == upSvc && s.GetKind() == pb.SpanKind_SPAN_KIND_CLIENT {
			upClient = s
		}
		if s.GetServiceName() == downSvc && s.GetKind() == pb.SpanKind_SPAN_KIND_SERVER {
			downServer = s
		}
	}
	if upClient == nil || downServer == nil {
		t.Fatalf("CLIENT/SERVER span'leri bulunamadi (span sayisi: %d)", len(trace.GetSpans()))
	}
	if downServer.GetParentSpanId() != upClient.GetSpanId() {
		t.Errorf("servisler arasi baglanti kopuk: %s != %s",
			downServer.GetParentSpanId(), upClient.GetSpanId())
	}

	// Oznitelikler saklandi mi?
	if downServer.GetAttributes()["http.method"] != "GET" {
		t.Errorf("http.method saklanmadi: %v", downServer.GetAttributes())
	}
	if trace.GetSpanCount() != int32(len(trace.GetSpans())) {
		t.Errorf("span_count tutarsiz: %d != %d", trace.GetSpanCount(), len(trace.GetSpans()))
	}
	if trace.GetDurationMicros() <= 0 {
		t.Error("trace suresi hesaplanmadi")
	}

	// --- QueryTraces ---
	qCtx, qCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer qCancel()
	qr, err := client.QueryTraces(qCtx, &pb.TraceQueryRequest{
		ServiceName: upSvc,
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("QueryTraces hatasi: %v", err)
	}
	if len(qr.GetTraces()) == 0 {
		t.Fatal("QueryTraces bos dondu")
	}
	found := false
	for _, tr := range qr.GetTraces() {
		if tr.GetTraceId() == wantTraceID {
			found = true
		}
	}
	if !found {
		t.Errorf("aranan trace sonuclarda yok (%d trace donduu)", len(qr.GetTraces()))
	}

	// --- ListOperations ---
	oCtx, oCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer oCancel()
	ops, err := client.ListOperations(oCtx, &pb.ListOperationsRequest{ServiceName: downSvc})
	if err != nil {
		t.Fatalf("ListOperations hatasi: %v", err)
	}
	if len(ops.GetOperations()) == 0 {
		t.Error("downstream servisin operasyonlari listelenmedi")
	}

	// --- GetTopology ---
	tCtx, tCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer tCancel()
	topo, err := client.GetTopology(tCtx, &pb.TopologyRequest{
		StartTimeMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
		EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		SampleLimit: 100,
	})
	if err != nil {
		t.Fatalf("GetTopology hatasi: %v", err)
	}

	edgeFound := false
	for _, e := range topo.GetEdges() {
		if e.GetCallerService() == upSvc && e.GetCalleeService() == downSvc {
			edgeFound = true
			if e.GetCallCount() <= 0 {
				t.Error("kenar cagri sayisi sifir")
			}
		}
	}
	if !edgeFound {
		t.Errorf("%s -> %s kenari topolojide yok", upSvc, downSvc)
	}
}

// TestTraceSorguDogrulama: eksik parametreler reddedilmeli.
func TestTraceSorguDogrulama(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-vm-" + suffix
	tracesTopic := "pulse-itest-vt-" + suffix
	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startTraceCollector(t, ctx, metricsTopic, tracesTopic, suffix)
	client := traceClient(t, grpcAddr)

	t.Run("servis adi yok", func(t *testing.T) {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.QueryTraces(c, &pb.TraceQueryRequest{}); err == nil {
			t.Fatal("hata bekleniyordu")
		}
	})

	t.Run("trace id yok", func(t *testing.T) {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.GetTrace(c, &pb.GetTraceRequest{}); err == nil {
			t.Fatal("hata bekleniyordu")
		}
	})

	t.Run("olmayan trace", func(t *testing.T) {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.GetTrace(c, &pb.GetTraceRequest{
			TraceId: "00000000000000000000000000000001"})
		if err == nil {
			t.Fatal("NotFound bekleniyordu")
		}
	})
}

// TestTimeBucket: kova hesabinin UTC'de ve saatlik oldugunu dogrular.
func TestTimeBucket(t *testing.T) {
	ts := time.Date(2026, 3, 15, 14, 37, 22, 0, time.UTC)
	if got := collector.TimeBucket(ts); got != "2026031514" {
		t.Errorf("TimeBucket = %q, beklenen 2026031514", got)
	}

	// Yerel saat dilimi sonucu degistirmemeli.
	loc, err := time.LoadLocation("Europe/Istanbul")
	if err != nil {
		t.Skip("saat dilimi verisi yok")
	}
	if got := collector.TimeBucket(ts.In(loc)); got != "2026031514" {
		t.Errorf("yerel saatte TimeBucket = %q, UTC ile ayni olmaliydi", got)
	}
}
