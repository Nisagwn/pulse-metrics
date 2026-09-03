//go:build integration

// Faz 5 entegrasyon testi: disa acilma.
//
// Tek bir iddiayi dogruluyor ama buyuk bir iddia: PulseMetrics artik
// kendi SDK'si olmadan da veri alabiliyor. Test, herhangi bir dilin
// OpenTelemetry SDK'sinin yapacagi seyi yapiyor - ham OTLP gonderiyor -
// ve verinin ScyllaDB'ye ulasip sorgulanabildigini gosteriyor.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nisah/pulse-metrics/internal/collector"
	"github.com/nisah/pulse-metrics/internal/otlp"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// startOTLPGateway: alicilari test icin ayaga kaldirir.
func startOTLPGateway(t *testing.T, ctx context.Context, metricsTopic, tracesTopic, logsTopic string) string {
	t.Helper()

	recv := otlp.NewReceiver(otlp.Config{
		KafkaBrokers: []string{kafkaAddr},
		TracesTopic:  tracesTopic,
		MetricsTopic: metricsTopic,
		LogsTopic:    logsTopic,
		Logger:       zap.NewNop(),
	})
	t.Cleanup(func() { _ = recv.Close() })

	port := freePort(t)
	lis, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("OTLP portu dinlenemedi: %v", err)
	}

	srv := grpc.NewServer()
	recv.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	addr := "127.0.0.1:" + port
	waitForTCP(t, addr, 20*time.Second)
	return addr
}

func otlpID(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

func otlpAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// TestOTLPUctanUca: OTLP ile gonderilen veri ScyllaDB'ye ulasiyor mu?
//
// Bu testte PulseMetrics'in kendi SDK'si HIC kullanilmiyor - sadece
// OpenTelemetry'nin resmi protobuf tipleri. Yani bir Python ya da Java
// uygulamasinin gonderecegi seyin aynisi.
func TestOTLPUctanUca(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	metricsTopic := "pulse-itest-p5m-" + suffix
	tracesTopic := "pulse-itest-p5t-" + suffix
	logsTopic := "pulse-itest-p5l-" + suffix
	createTopic(t, metricsTopic)
	createTopic(t, tracesTopic)
	createTopic(t, logsTopic)
	waitForTopic(t, tracesTopic)
	waitForTopic(t, logsTopic)

	caller := "itest-otlp-caller-" + suffix
	callee := "itest-otlp-callee-" + suffix

	sess := scyllaSession(t)
	cleanupLogs(t, sess, callee)
	cleanupTraceService(t, sess, caller)
	cleanupTraceService(t, sess, callee)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := startFullCollector(t, ctx, metricsTopic, tracesTopic, logsTopic, "p5-"+suffix)
	otlpAddr := startOTLPGateway(t, ctx, metricsTopic, tracesTopic, logsTopic)

	conn, err := grpc.NewClient(otlpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("OTLP baglantisi kurulamadi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	traceID := otlpID(16, 0x40)
	parentSpan := otlpID(8, 0x10)
	childSpan := otlpID(8, 0x20)

	now := time.Now()
	startNano := uint64(now.Add(-200 * time.Millisecond).UnixNano())
	endNano := uint64(now.UnixNano())

	// --- span'ler: iki servis, ebeveyn-cocuk ---
	tc := coltracepb.NewTraceServiceClient(conn)
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 30*time.Second)
	traceResp, err := tc.Export(sendCtx, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
					otlpAttr("service.name", caller),
					otlpAttr("service.instance.id", "pod-1"),
				}},
				ScopeSpans: []*tracepb.ScopeSpans{{
					Spans: []*tracepb.Span{{
						TraceId: traceID, SpanId: parentSpan,
						Name: "POST /siparis", Kind: tracepb.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: startNano, EndTimeUnixNano: endNano,
					}},
				}},
			},
			{
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
					otlpAttr("service.name", callee),
				}},
				ScopeSpans: []*tracepb.ScopeSpans{{
					Spans: []*tracepb.Span{{
						TraceId: traceID, SpanId: childSpan, ParentSpanId: parentSpan,
						Name: "GET /stok", Kind: tracepb.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: startNano + 10_000_000,
						EndTimeUnixNano:   endNano - 10_000_000,
						// peer.service OTel'in STANDART ozniteligi:
						// topoloji hicbir ozel ayar olmadan calismali.
						Attributes: []*commonpb.KeyValue{otlpAttr(otlp.AttrPeerService, caller)},
						Status: &tracepb.Status{
							Code: tracepb.Status_STATUS_CODE_ERROR, Message: "stok yok"},
					}},
				}},
			},
		},
	})
	sendCancel()
	if err != nil {
		t.Fatalf("OTLP trace gonderilemedi: %v", err)
	}
	if ps := traceResp.GetPartialSuccess(); ps.GetRejectedSpans() != 0 {
		t.Fatalf("span reddedildi: %d (%s)", ps.GetRejectedSpans(), ps.GetErrorMessage())
	}

	// --- loglar: ayni trace_id ile ---
	lc := collogspb.NewLogsServiceClient(conn)
	sendCtx, sendCancel = context.WithTimeout(context.Background(), 30*time.Second)
	logResp, err := lc.Export(sendCtx, &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				otlpAttr("service.name", callee),
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope: &commonpb.InstrumentationScope{Name: "app"},
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   endNano - 20_000_000,
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "OTLP ile gelen hata"}},
					TraceId: traceID, SpanId: childSpan,
				}},
			}},
		}},
	})
	sendCancel()
	if err != nil {
		t.Fatalf("OTLP log gonderilemedi: %v", err)
	}
	if ps := logResp.GetPartialSuccess(); ps.GetRejectedLogRecords() != 0 {
		t.Fatalf("log reddedildi: %d", ps.GetRejectedLogRecords())
	}

	wantTraceID := fmt.Sprintf("%x", traceID)

	// --- span'ler ScyllaDB'ye ulasti mi? ---
	tsc := traceClient(t, grpcAddr)
	var trace *pb.Trace
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, qErr := tsc.GetTrace(qCtx, &pb.GetTraceRequest{TraceId: wantTraceID})
		qCancel()
		if qErr == nil && len(resp.GetSpans()) >= 2 {
			trace = resp
			break
		}
		time.Sleep(time.Second)
	}
	if trace == nil {
		t.Fatalf("OTLP span'leri 90 sn icinde gelmedi (trace %s)", wantTraceID)
	}

	byService := map[string]*pb.Span{}
	for _, s := range trace.GetSpans() {
		byService[s.GetServiceName()] = s
	}
	parent, ok := byService[caller]
	if !ok {
		t.Fatalf("cagiran servisin span'i yok: %v", byService)
	}
	if parent.GetOperationName() != "POST /siparis" {
		t.Errorf("operasyon adi = %q", parent.GetOperationName())
	}
	// Nanosaniye -> mikrosaniye cevrimi dogru mu?
	if d := parent.GetEndTimeMicros() - parent.GetStartTimeMicros(); d < 190_000 || d > 210_000 {
		t.Errorf("sure %d us, ~200000 bekleniyordu", d)
	}

	child, ok := byService[callee]
	if !ok {
		t.Fatalf("cagrilan servisin span'i yok: %v", byService)
	}
	if child.GetStatus().GetCode() != pb.StatusCode_STATUS_CODE_ERROR {
		t.Errorf("durum = %v, ERROR bekleniyordu", child.GetStatus().GetCode())
	}
	if child.GetAttributes()[otlp.AttrPeerService] != caller {
		t.Errorf("peer.service tasinmadi: %v", child.GetAttributes())
	}

	// --- loglar trace'e bagli mi? ---
	logc := logClient(t, grpcAddr)
	var traceLogs *pb.LogsQueryResponse
	deadline = time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, qErr := logc.GetTraceLogs(qCtx, &pb.GetTraceLogsRequest{TraceId: wantTraceID})
		qCancel()
		if qErr == nil && len(resp.GetLogs()) >= 1 {
			traceLogs = resp
			break
		}
		time.Sleep(time.Second)
	}
	if traceLogs == nil {
		t.Fatal("OTLP loglari trace'e baglanmadi")
	}
	l := traceLogs.GetLogs()[0]
	if l.GetLevel() != pb.LogLevel_LEVEL_ERROR {
		t.Errorf("seviye = %v (OTLP siddet 17 -> ERROR olmaliydi)", l.GetLevel())
	}
	if l.GetMessage() != "OTLP ile gelen hata" {
		t.Errorf("mesaj = %q", l.GetMessage())
	}

	// --- topoloji: peer.service'ten cikarilmali ---
	var edge *pb.ServiceDependency
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && edge == nil {
		qCtx, qCancel := context.WithTimeout(context.Background(), 10*time.Second)
		topo, qErr := tsc.GetTopology(qCtx, &pb.TopologyRequest{
			StartTimeMs: time.Now().Add(-time.Hour).UnixMilli(),
			EndTimeMs:   time.Now().Add(time.Minute).UnixMilli(),
		})
		qCancel()
		if qErr == nil {
			for _, e := range topo.GetEdges() {
				if e.GetCallerService() == caller && e.GetCalleeService() == callee {
					edge = e
					break
				}
			}
		}
		if edge == nil {
			time.Sleep(2 * time.Second)
		}
	}
	if edge == nil {
		t.Fatal("OTLP verisinden topoloji kenari olusmadi")
	}
	// Tek cagri, hatali: hata orani 1 olmali.
	if edge.GetErrorRate() < 0.99 {
		t.Errorf("kenar hata orani %v, 1 bekleniyordu", edge.GetErrorRate())
	}

	t.Cleanup(func() {
		for _, b := range []string{collector.TimeBucket(time.Now()),
			collector.TimeBucket(time.Now().Add(-time.Hour))} {
			_ = sess.Query(`DELETE FROM service_edges WHERE caller_service = ? AND callee_service = ? AND time_bucket = ?`,
				caller, callee, b).Exec()
			_ = sess.Query(`DELETE FROM edge_pairs WHERE time_bucket = ? AND caller_service = ? AND callee_service = ?`,
				b, caller, callee).Exec()
		}
	})
}

// TestOTLPHTTPJSON: OTLP/HTTP + JSON yolu calisiyor mu?
//
// Onemli olan ayrinti: OTLP/JSON kimlikleri ONALTILIK tasir, protobuf'un
// JSON eslemesi ise base64 bekler. Bu test o donusumun yapildigini
// dogruluyor - yapilmasaydi gercek SDK'lardan gelen HER span sessizce
// reddedilirdi ve bunu ancak "veri neden gelmiyor" diye gunlerce
// arayarak fark ederdin.
//
// Kanit yanitin kendisinde: bir span reddedilseydi OTLP sozlesmesi
// geregi partial_success.rejectedSpans alani dolardi.
func TestOTLPHTTPJSON(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "pulse-itest-p5j-" + suffix
	createTopic(t, topic)
	// Topic'in TUM broker'lara yayilmasini bekle. Gateway bilerek
	// yeniden denemiyor - yazma basarisiz olursa 503 donup istemciye
	// birakiyor - yani burada beklemezsek ilk istek 503 alir.
	waitForTopic(t, topic)

	recv := otlp.NewReceiver(otlp.Config{
		KafkaBrokers: []string{kafkaAddr},
		TracesTopic:  topic,
		MetricsTopic: topic,
		LogsTopic:    topic,
		Logger:       zap.NewNop(),
	})
	t.Cleanup(func() { _ = recv.Close() })

	mux := http.NewServeMux()
	recv.Handler(mux)
	addr := "127.0.0.1:" + freePort(t)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("dinlenemedi: %v", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	waitForTCP(t, addr, 20*time.Second)

	send := func(traceID, spanID string) map[string]interface{} {
		t.Helper()
		body := fmt.Sprintf(`{"resourceSpans":[{
		  "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"json-svc-%s"}}]},
		  "scopeSpans":[{"spans":[{
		    "traceId":%q,"spanId":%q,
		    "name":"GET /json","kind":2,
		    "startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`,
			suffix, traceID, spanID,
			time.Now().Add(-time.Second).UnixNano(), time.Now().UnixNano())

		req, reqErr := http.NewRequest(http.MethodPost,
			"http://"+addr+"/v1/traces", strings.NewReader(body))
		if reqErr != nil {
			t.Fatalf("istek olusturulamadi: %v", reqErr)
		}
		req.Header.Set("Content-Type", "application/json")

		// Gercek bir OTel SDK'si gibi davran: 503 gecici bir durumdur
		// ve istemci yeniden dener. Gateway bilerek tamponlamiyor,
		// yeniden denemeyi istemciye birakiyor - test de o istemcinin
		// yerinde.
		var (
			resp *http.Response
			raw  []byte
		)
		for attempt := 0; attempt < 5; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * time.Second)
				req, _ = http.NewRequest(http.MethodPost,
					"http://"+addr+"/v1/traces", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
			}
			r, postErr := http.DefaultClient.Do(req)
			if postErr != nil {
				t.Fatalf("OTLP/HTTP istegi basarisiz: %v", postErr)
			}
			raw, _ = io.ReadAll(r.Body)
			_ = r.Body.Close()
			resp = r
			if r.StatusCode != http.StatusServiceUnavailable {
				break
			}
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HTTP %d: %s", resp.StatusCode, raw)
		}
		out := map[string]interface{}{}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &out)
		}
		return out
	}

	// 1) ONALTILIK kimlik: gercek OTel SDK'larinin gonderdigi bicim.
	resp := send("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
	if ps, ok := resp["partialSuccess"]; ok {
		t.Errorf("onaltilik kimlikli span reddedildi: %v "+
			"(hex->base64 cevrimi calismiyor)", ps)
	}

	// 2) BASE64 kimlik: protobuf'un standart JSON eslemesi. Ikisi de
	//    kabul edilmeli - bazi araclar standarda uyuyor.
	resp = send("S/kvNXezTaajzpKdDg5HNg==", "APBnqgupArc=")
	if ps, ok := resp["partialSuccess"]; ok {
		t.Errorf("base64 kimlikli span reddedildi: %v", ps)
	}

	// 3) BOZUK kimlik: reddedilmeli VE bildirilmeli. Sessizce dusurmek,
	//    istemcinin veri gonderdigini sanmasina yol acardi.
	resp = send("kisa", "dahakisa")
	ps, ok := resp["partialSuccess"]
	if !ok {
		t.Fatal("gecersiz kimlikli span reddedilmeliydi")
	}
	if m, isMap := ps.(map[string]interface{}); !isMap || m["rejectedSpans"] == nil {
		t.Errorf("rejectedSpans bildirilmeliydi: %v", ps)
	}
}
