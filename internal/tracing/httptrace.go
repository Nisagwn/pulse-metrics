package tracing

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// W3C Trace Context baslik adlari.
const (
	TraceParentHeader = "traceparent"
	TraceStateHeader  = "tracestate"
)

// PeerServiceAttr: gelen istegin hangi servisten geldigi.
//
// Bu oznitelik topolojinin ingest sirasinda hesaplanmasini mumkun kilar.
// Olmasaydi, collector bir SERVER span'i gordugunde ebeveyninin hangi
// serviste oldugunu bilemez, ogrenmek icin ek okuma yapmasi gerekirdi.
const PeerServiceAttr = "peer.service"

// tracestateKey: kendi servis adimizi tasidigimiz tracestate anahtari.
// tracestate W3C'de tam olarak bunun icin ayrilmistir: saticiya ozel,
// virgulle ayrilmis anahtar=deger ciftleri. Standart disi bir baslik
// uydurmak yerine standardin ayirdigi alani kullaniyoruz.
const tracestateKey = "pulse"

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
			state := r.Header.Get(TraceStateHeader)
			parent.TraceState = state
			opts = append(opts, WithRemoteParent(parent))

			// Cagiran servisi tracestate'ten cikar ve span'e yaz.
			if caller := callerFromTraceState(state); caller != "" {
				opts = append(opts, WithAttributes(map[string]string{
					PeerServiceAttr: caller,
				}))
			}
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

	// Kendi servis adimizi tracestate'e yaz. Karsi taraf bunu peer.service
	// olarak kaydedecek ve collector topolojiyi ek okuma yapmadan cikaracak.
	outReq.Header.Set(TraceStateHeader,
		withCaller(sc.TraceState, tt.tracer.serviceName))

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

// --- tracestate yardimcilari -----------------------------------------------

// withCaller: mevcut tracestate'e "pulse=svc:<ad>" girdisini ekler.
//
// W3C kurallari: en son degistiren en basta olur, ayni anahtar iki kez
// bulunmaz, en fazla 32 girdi. Bu yuzden once kendi anahtarimizi
// temizleyip basa ekliyoruz.
func withCaller(state, service string) string {
	if service == "" {
		return state
	}

	entry := tracestateKey + "=svc:" + sanitizeTraceStateValue(service)

	var kept []string
	for _, part := range strings.Split(state, ",") {
		part = strings.TrimSpace(part)
		if part == "" || strings.HasPrefix(part, tracestateKey+"=") {
			continue
		}
		kept = append(kept, part)
		if len(kept) >= 31 { // kendi girdimizle birlikte 32
			break
		}
	}

	if len(kept) == 0 {
		return entry
	}
	return entry + "," + strings.Join(kept, ",")
}

// callerFromTraceState: "pulse=svc:<ad>" girdisinden servis adini cikarir.
func callerFromTraceState(state string) string {
	for _, part := range strings.Split(state, ",") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, tracestateKey+"=") {
			continue
		}
		v := strings.TrimPrefix(part, tracestateKey+"=")
		if strings.HasPrefix(v, "svc:") {
			return strings.TrimPrefix(v, "svc:")
		}
	}
	return ""
}

// sanitizeTraceStateValue: tracestate degerlerinde virgul ve esittir
// isaretine izin verilmez; bicimi bozmamak icin temizliyoruz.
func sanitizeTraceStateValue(s string) string {
	s = strings.ReplaceAll(s, ",", "_")
	s = strings.ReplaceAll(s, "=", "_")
	s = strings.ReplaceAll(s, " ", "_")
	if len(s) > 96 {
		s = s[:96]
	}
	return s
}
