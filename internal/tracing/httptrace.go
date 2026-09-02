package tracing

import (
	"fmt"
	"net/http"
	"strconv"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// W3C Trace Context baslik adlari.
const (
	TraceParentHeader = "traceparent"
	TraceStateHeader  = "tracestate"
)

// Middleware: gelen HTTP isteklerini otomatik olarak enstrumante eder.
//
// Yaptigi is:
//  1. Gelen istekten traceparent'i okur (varsa ayni trace'in devamiyiz)
//  2. SERVER turunde bir span acar
//  3. Span'i request context'ine koyar - handler ic cagrilarda kullanir
//  4. Yanit durum kodunu ve suresini span'e yazar
//
// Kullanimi:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/orders", handler)
//	http.ListenAndServe(":8080", tracer.Middleware(mux))
func (t *Tracer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opts := []SpanOption{
			WithSpanKind(pb.SpanKind_SPAN_KIND_SERVER),
			WithAttributes(map[string]string{
				"http.method": r.Method,
				"http.target": r.URL.Path,
				"http.host":   r.Host,
				"net.peer.ip": clientIP(r),
			}),
		}

		// Gelen baglami cozmeye calis. Gecersizse yeni bir trace baslar -
		// hatali bir baslik yuzunden istegi reddetmek dogru olmaz.
		if parent, err := ParseTraceParent(r.Header.Get(TraceParentHeader)); err == nil {
			parent.TraceState = r.Header.Get(TraceStateHeader)
			opts = append(opts, WithRemoteParent(parent))
		}

		// Operasyon adi olarak "GET /orders" gibi bir sey kullaniyoruz.
		// Dikkat: r.URL.Path'i dogrudan kullanmak /orders/12345 gibi
		// yollarda kardinalite patlamasina yol acar. Gercek bir sistemde
		// yonlendirici sablonu ("/orders/{id}") kullanilmali.
		name := r.Method + " " + r.URL.Path

		ctx, span := t.Start(r.Context(), name, opts...)
		defer span.End()

		// Trace id'yi yanita koy: bir kullanici hata bildirdiginde
		// dogrudan o trace'i acabilmek icin cok degerli.
		w.Header().Set("X-Trace-Id", span.TraceID())

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetAttribute("http.status_code", strconv.Itoa(rec.status))
		if rec.status >= 500 {
			span.SetStatus(pb.StatusCode_STATUS_CODE_ERROR,
				fmt.Sprintf("HTTP %d", rec.status))
		} else {
			span.SetStatus(pb.StatusCode_STATUS_CODE_OK, "")
		}
	})
}

// statusRecorder: yazilan HTTP durum kodunu yakalar.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	// WriteHeader cagrilmadan Write yapilirsa Go 200 varsayar.
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// Flush: alttaki ResponseWriter destekliyorsa aktarir (SSE, streaming icin).
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		return v
	}
	return r.RemoteAddr
}

// --- Istemci tarafi ---------------------------------------------------------

// Transport: giden HTTP isteklerini enstrumante eder.
//
// CLIENT turunde bir span acar ve traceparent basligini istege ekler.
// Karsi taraftaki servis o basligi okuyup ayni trace'i surdurur - iki
// servisin span'leri boylece birlesir.
//
// Kullanimi:
//
//	client := &http.Client{Transport: tracer.Transport(nil)}
//	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
//	resp, err := client.Do(req)
//
// ctx'in istekle tasinmasi sart: ebeveyn span oradan bulunuyor.
func (t *Tracer) Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &tracingTransport{base: base, tracer: t}
}

type tracingTransport struct {
	base   http.RoundTripper
	tracer *Tracer
}

func (tt *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := tt.tracer.Start(req.Context(),
		req.Method+" "+req.URL.Host+req.URL.Path,
		WithSpanKind(pb.SpanKind_SPAN_KIND_CLIENT),
		WithAttributes(map[string]string{
			"http.method":   req.Method,
			"http.url":      req.URL.String(),
			"net.peer.name": req.URL.Hostname(),
		}),
	)
	defer span.End()

	// Istegi klonluyoruz: RoundTripper'in gelen istegi degistirmesi
	// yasak (net/http sozlesmesi).
	outReq := req.Clone(ctx)
	sc := span.SpanContext()
	outReq.Header.Set(TraceParentHeader, sc.TraceParent())
	if sc.TraceState != "" {
		outReq.Header.Set(TraceStateHeader, sc.TraceState)
	}

	resp, err := tt.base.RoundTrip(outReq)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.SetAttribute("http.status_code", strconv.Itoa(resp.StatusCode))
	if resp.StatusCode >= 400 {
		span.SetStatus(pb.StatusCode_STATUS_CODE_ERROR,
			fmt.Sprintf("HTTP %d", resp.StatusCode))
	} else {
		span.SetStatus(pb.StatusCode_STATUS_CODE_OK, "")
	}
	return resp, nil
}

// Client: enstrumante edilmis bir http.Client dondurur.
func (t *Tracer) Client() *http.Client {
	return &http.Client{Transport: t.Transport(nil)}
}
