package tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// --- W3C trace context ------------------------------------------------------

func TestTraceParentGidisDonus(t *testing.T) {
	// Spesifikasyondaki ornek deger.
	const header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	sc, err := ParseTraceParent(header)
	if err != nil {
		t.Fatalf("cozulemedi: %v", err)
	}
	if got := sc.TraceID.String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace_id = %s", got)
	}
	if got := sc.SpanID.String(); got != "00f067aa0ba902b7" {
		t.Errorf("span_id = %s", got)
	}
	if !sc.IsSampled() {
		t.Error("sampled bayragi okunmadi")
	}
	if !sc.Remote {
		t.Error("agdan gelen context Remote isaretlenmeli")
	}
	if got := sc.TraceParent(); got != header {
		t.Errorf("yeniden uretilen baslik = %q, beklenen %q", got, header)
	}
}

func TestTraceParentGecersizGirdiler(t *testing.T) {
	cases := map[string]string{
		"bos":               "",
		"parca eksik":       "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"kisa trace id":     "00-4bf92f35-00f067aa0ba902b7-01",
		"sifir trace id":    "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"sifir span id":     "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"gecersiz surum ff": "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"hex olmayan":       "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01",
		"v00 fazla parca":   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseTraceParent(header); err == nil {
				t.Errorf("hata bekleniyordu: %q", header)
			}
		})
	}
}

func TestTraceParentIleriSurum(t *testing.T) {
	// Standart, bilinmeyen ileri surumlerde ilk dort parcanin okunmasini oneriyor.
	sc, err := ParseTraceParent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-yeni")
	if err != nil {
		t.Fatalf("ileri surum reddedilmemeliydi: %v", err)
	}
	if !sc.IsValid() {
		t.Error("kimlikler cozulmeliydi")
	}
}

// --- span yasam dongusu -----------------------------------------------------

func TestKokSpanYeniTraceBaslatir(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc-a", exp)

	_, span := tr.Start(context.Background(), "islem")
	span.End()

	spans := exp.Spans()
	if len(spans) != 1 {
		t.Fatalf("1 span bekleniyordu, %d alindi", len(spans))
	}
	s := spans[0]
	if s.ParentSpanId != "" {
		t.Errorf("kok span'in ebeveyni olmamali, %q bulundu", s.ParentSpanId)
	}
	if s.ServiceName != "svc-a" || s.OperationName != "islem" {
		t.Errorf("beklenmeyen span: %+v", s)
	}
	if s.EndTimeMicros < s.StartTimeMicros {
		t.Error("bitis zamani baslangictan once")
	}
}

func TestIcIceSpanlerAyniTraceDeKalir(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc-a", exp)

	ctx, parent := tr.Start(context.Background(), "ust")
	_, child := tr.Start(ctx, "alt")
	child.End()
	parent.End()

	spans := exp.Spans()
	if len(spans) != 2 {
		t.Fatalf("2 span bekleniyordu, %d alindi", len(spans))
	}

	var c, p *pb.Span
	for _, s := range spans {
		if s.OperationName == "alt" {
			c = s
		} else {
			p = s
		}
	}
	if c == nil || p == nil {
		t.Fatal("span'ler bulunamadi")
	}
	if c.TraceId != p.TraceId {
		t.Errorf("trace_id'ler farkli: %s != %s", c.TraceId, p.TraceId)
	}
	if c.ParentSpanId != p.SpanId {
		t.Errorf("ebeveyn baglantisi yanlis: %s != %s", c.ParentSpanId, p.SpanId)
	}
}

func TestUzakEbeveynDevamEttirilir(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc-b", exp)

	remote, err := ParseTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}

	_, span := tr.Start(context.Background(), "gelen", WithRemoteParent(remote))
	span.End()

	s := exp.Spans()[0]
	if s.TraceId != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace_id devam etmedi: %s", s.TraceId)
	}
	if s.ParentSpanId != "00f067aa0ba902b7" {
		t.Errorf("parent_span_id yanlis: %s", s.ParentSpanId)
	}
	if s.SpanId == s.ParentSpanId {
		t.Error("yeni bir span_id uretilmeliydi")
	}
}

func TestEndIkiKezGuvenli(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp)

	_, span := tr.Start(context.Background(), "islem")
	span.End()
	span.End() // defer + erken End birlikte kullanilabilmeli

	if got := len(exp.Spans()); got != 1 {
		t.Errorf("span bir kez gonderilmeliydi, %d kez gonderildi", got)
	}
}

func TestRecordErrorDurumVeOlayYazar(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp)

	_, span := tr.Start(context.Background(), "islem")
	span.RecordError(context.DeadlineExceeded)
	span.End()

	s := exp.Spans()[0]
	if s.Status.Code != pb.StatusCode_STATUS_CODE_ERROR {
		t.Errorf("durum %v, ERROR bekleniyordu", s.Status.Code)
	}
	if len(s.Events) != 1 || s.Events[0].Name != "exception" {
		t.Errorf("exception olayi bekleniyordu: %+v", s.Events)
	}
}

// --- orneklem ---------------------------------------------------------------

func TestOrneklenmemisSpanGonderilmez(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp, WithSampler(NeverSample{}))

	ctx, span := tr.Start(context.Background(), "islem")
	span.End()

	if got := len(exp.Spans()); got != 0 {
		t.Errorf("orneklenmemis span gonderilmemeliydi, %d gonderildi", got)
	}
	// Ama kimlikler yine uretilmeli: baglam yayilimi devam etmeli.
	if !SpanContextFromContext(ctx).IsValid() {
		t.Error("orneklenmese de span context gecerli olmali")
	}
}

func TestOrneklemKarariTraceBoyuncaTasinir(t *testing.T) {
	exp := NewMemoryExporter()
	// Bu servis normalde hicbir seyi orneklemez...
	tr := NewTracer("downstream", exp, WithSampler(NeverSample{}))

	// ...ama yukaridan sampled=1 gelirse kaydetmeli. Aksi halde bir
	// trace'in yarisi kaydedilir, yarisi kaydedilmez.
	remote, _ := ParseTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	_, span := tr.Start(context.Background(), "gelen", WithRemoteParent(remote))
	span.End()

	if got := len(exp.Spans()); got != 1 {
		t.Errorf("ust servisin orneklem karari tasinmaliydi, %d span gonderildi", got)
	}
}

func TestRatioSamplerSinirlar(t *testing.T) {
	all := NewRatioSampler(1.0)
	none := NewRatioSampler(0.0)
	id := newTraceID()

	if !all.ShouldSample(id) {
		t.Error("oran 1.0 her zaman orneklemeliydi")
	}
	if none.ShouldSample(id) {
		t.Error("oran 0.0 hicbir zaman orneklememeli")
	}

	// Ayni trace_id ayni karari vermeli (deterministik).
	half := NewRatioSampler(0.5)
	first := half.ShouldSample(id)
	for i := 0; i < 20; i++ {
		if half.ShouldSample(id) != first {
			t.Fatal("ayni trace_id icin karar degisti - deterministik olmali")
		}
	}
}

// --- HTTP enstrumantasyonu --------------------------------------------------

// Iki servisin HTTP uzerinden tek bir trace'te birlestigini dogrular.
// Faz 2'nin asil iddiasi bu.
func TestHTTPUctanUcaBaglamYayilimi(t *testing.T) {
	downExp := NewMemoryExporter()
	downTracer := NewTracer("downstream", downExp)

	downstream := httptest.NewServer(downTracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Sunucu tarafinda ic bir span daha ac.
			_, inner := downTracer.Start(r.Context(), "db-query")
			inner.End()
			w.WriteHeader(http.StatusOK)
		})))
	defer downstream.Close()

	upExp := NewMemoryExporter()
	upTracer := NewTracer("upstream", upExp)

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

	// Enstrumante EDILMEMIS istemci: gercek bir kullanici gibi.
	resp, err := http.Get(upstream.URL + "/checkout")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if resp.Header.Get("X-Trace-Id") == "" {
		t.Error("X-Trace-Id yaniti dondurulmedi")
	}

	upSpans, downSpans := upExp.Spans(), downExp.Spans()
	if len(upSpans) != 2 {
		t.Fatalf("upstream'de 2 span bekleniyordu (SERVER + CLIENT), %d alindi", len(upSpans))
	}
	if len(downSpans) != 2 {
		t.Fatalf("downstream'de 2 span bekleniyordu (SERVER + ic), %d alindi", len(downSpans))
	}

	// Dort span de AYNI trace'te olmali.
	traceID := upSpans[0].TraceId
	for _, s := range append(append([]*pb.Span{}, upSpans...), downSpans...) {
		if s.TraceId != traceID {
			t.Fatalf("span %s farkli trace'te: %s != %s", s.OperationName, s.TraceId, traceID)
		}
	}

	// upstream'in CLIENT span'i, downstream'in SERVER span'inin ebeveyni olmali.
	var clientSpan, downServer *pb.Span
	for _, s := range upSpans {
		if s.Kind == pb.SpanKind_SPAN_KIND_CLIENT {
			clientSpan = s
		}
	}
	for _, s := range downSpans {
		if s.Kind == pb.SpanKind_SPAN_KIND_SERVER {
			downServer = s
		}
	}
	if clientSpan == nil || downServer == nil {
		t.Fatal("CLIENT veya SERVER span bulunamadi")
	}
	if downServer.ParentSpanId != clientSpan.SpanId {
		t.Errorf("servisler arasi ebeveyn baglantisi kurulamadi: %s != %s",
			downServer.ParentSpanId, clientSpan.SpanId)
	}
	if downServer.ServiceName != "downstream" {
		t.Errorf("servis adi yanlis: %s", downServer.ServiceName)
	}
}

func TestMiddlewareDurumKoduYazar(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp)

	srv := httptest.NewServer(tr.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/bozuk")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	s := exp.Spans()[0]
	if s.Attributes["http.status_code"] != "500" {
		t.Errorf("status_code = %q", s.Attributes["http.status_code"])
	}
	if s.Status.Code != pb.StatusCode_STATUS_CODE_ERROR {
		t.Errorf("5xx durumunda span ERROR olmali, %v bulundu", s.Status.Code)
	}
	if !strings.Contains(s.OperationName, "/bozuk") {
		t.Errorf("operasyon adi yolu icermeli: %s", s.OperationName)
	}
}

func TestMiddlewareBozukBaslikIleCalismayaistDevamEder(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp)

	srv := httptest.NewServer(tr.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set(TraceParentHeader, "bu-gecerli-degil")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	// Istek reddedilmemeli; yeni bir trace baslamali.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("bozuk baslik istegi reddetmemeli, HTTP %d", resp.StatusCode)
	}
	spans := exp.Spans()
	if len(spans) != 1 {
		t.Fatalf("1 span bekleniyordu, %d alindi", len(spans))
	}
	if spans[0].ParentSpanId != "" {
		t.Error("bozuk baslik sonrasi yeni bir kok trace baslamali")
	}
}

// --- eszamanlilik -----------------------------------------------------------

func TestSpanEszamanliKullanim(t *testing.T) {
	exp := NewMemoryExporter()
	tr := NewTracer("svc", exp)
	_, span := tr.Start(context.Background(), "islem")

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(3)
		go func(n int) { defer wg.Done(); span.SetAttribute("k", "v") }(i)
		go func() { defer wg.Done(); span.AddEvent("olay", nil) }()
		go func() { defer wg.Done(); _ = span.Duration() }()
	}
	wg.Wait()
	span.End()

	if got := len(exp.Spans()); got != 1 {
		t.Errorf("1 span bekleniyordu, %d alindi", got)
	}
}
