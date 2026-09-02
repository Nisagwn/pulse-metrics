package tracing

import (
	"context"
	"sync"
	"time"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// Exporter: bitmis span'leri bir yere gonderir (Kafka, stdout, test tamponu).
type Exporter interface {
	// ExportSpans engellememeli; yavas bir exporter uygulamayi yavaslatir.
	ExportSpans(ctx context.Context, spans []*pb.Span) error
	Shutdown(ctx context.Context) error
}

// Sampler: bu trace kaydedilsin mi? Yuksek trafikte her istegi kaydetmek
// hem pahali hem gereksizdir.
type Sampler interface {
	ShouldSample(traceID TraceID) bool
	Description() string
}

// Tracer: span uretir.
type Tracer struct {
	serviceName string
	instanceID  string
	exporter    Exporter
	sampler     Sampler

	mu     sync.Mutex
	closed bool
}

// TracerOption: Tracer yapilandirmasi.
type TracerOption func(*Tracer)

// WithSampler: orneklem stratejisini degistirir (varsayilan: hepsini al).
func WithSampler(s Sampler) TracerOption {
	return func(t *Tracer) { t.sampler = s }
}

// WithInstanceID: bu surecin kimligi.
func WithInstanceID(id string) TracerOption {
	return func(t *Tracer) { t.instanceID = id }
}

// NewTracer: verilen servis adi icin bir tracer olusturur.
func NewTracer(serviceName string, exporter Exporter, opts ...TracerOption) *Tracer {
	t := &Tracer{
		serviceName: serviceName,
		instanceID:  "default",
		exporter:    exporter,
		sampler:     AlwaysSample{},
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// ServiceName: bu tracer'in temsil ettigi servis.
func (t *Tracer) ServiceName() string { return t.serviceName }

// SpanOption: span baslatma secenekleri.
type SpanOption func(*Span)

// WithSpanKind: span'in rolu (SERVER, CLIENT, INTERNAL...).
func WithSpanKind(k pb.SpanKind) SpanOption {
	return func(s *Span) { s.kind = k }
}

// WithAttributes: baslangicta eklenecek oznitelikler.
func WithAttributes(attrs map[string]string) SpanOption {
	return func(s *Span) {
		for k, v := range attrs {
			s.attributes[k] = v
		}
	}
}

// WithRemoteParent: ag uzerinden gelen ebeveyn context'i (sunucu tarafi).
// Gecersiz bir context verilirse yeni bir trace baslatilir.
func WithRemoteParent(parent SpanContext) SpanOption {
	return func(s *Span) { s.remoteParent = &parent }
}

// Start: yeni bir span baslatir ve onu tasiyan bir context dondurur.
//
// Ebeveyn secimi sirasi:
//  1. WithRemoteParent ile acikca verilen (gelen HTTP istegi)
//  2. ctx icindeki aktif span (ayni surecte ic ice cagrilar)
//  3. hicbiri yoksa: yeni bir trace'in koku
//
// Doner donmez End() cagrilmali - genelde defer ile.
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	s := &Span{
		tracer:     t,
		name:       name,
		kind:       pb.SpanKind_SPAN_KIND_INTERNAL,
		startTime:  time.Now(),
		attributes: make(map[string]string),
		statusCode: pb.StatusCode_STATUS_CODE_UNSET,
	}
	for _, opt := range opts {
		opt(s)
	}

	var parent SpanContext
	switch {
	case s.remoteParent != nil && s.remoteParent.IsValid():
		parent = *s.remoteParent
	default:
		if p := SpanFromContext(ctx); p != nil {
			parent = p.SpanContext()
		}
	}

	if parent.IsValid() {
		// Ayni trace'in devami: trace_id ve orneklem karari korunur.
		// Orneklem kararinin tasinmasi kritik: yoksa bir trace'in
		// yarisi kaydedilir, yarisi kaydedilmez ve hicbir ise yaramaz.
		s.spanContext = SpanContext{
			TraceID:    parent.TraceID,
			SpanID:     newSpanID(),
			TraceFlags: parent.TraceFlags,
			TraceState: parent.TraceState,
		}
		s.parentSpanID = parent.SpanID
		s.hasParent = true
	} else {
		// Yeni trace koku: orneklem karari burada verilir.
		traceID := newTraceID()
		var flags byte
		if t.sampler.ShouldSample(traceID) {
			flags |= FlagSampled
		}
		s.spanContext = SpanContext{
			TraceID:    traceID,
			SpanID:     newSpanID(),
			TraceFlags: flags,
		}
	}

	return ContextWithSpan(ctx, s), s
}

// Shutdown: bekleyen span'leri gonderir ve tracer'i kapatir.
func (t *Tracer) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	if t.exporter == nil {
		return nil
	}
	return t.exporter.Shutdown(ctx)
}

// --- Span -------------------------------------------------------------------

// Span: tek bir is biriminin olcumu. Goroutine-guvenlidir.
type Span struct {
	tracer *Tracer

	spanContext  SpanContext
	parentSpanID SpanID
	hasParent    bool
	remoteParent *SpanContext

	name      string
	kind      pb.SpanKind
	startTime time.Time

	mu         sync.Mutex
	endTime    time.Time
	ended      bool
	attributes map[string]string
	events     []*pb.Event
	statusCode pb.StatusCode
	statusDesc string
}

// SpanContext: bu span'in kimligi.
func (s *Span) SpanContext() SpanContext {
	if s == nil {
		return SpanContext{}
	}
	return s.spanContext
}

// TraceID: kolaylik icin - loglara yazmak icin kullanisli.
func (s *Span) TraceID() string {
	if s == nil {
		return ""
	}
	return s.spanContext.TraceID.String()
}

// SetAttribute: span'e anahtar/deger ekler.
func (s *Span) SetAttribute(key, value string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return // bitmis span'e yazmak sessizce yok sayilir
	}
	s.attributes[key] = value
}

// AddEvent: span icinde zaman damgali bir olay isaretler.
func (s *Span) AddEvent(name string, attrs map[string]string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	copied := make(map[string]string, len(attrs))
	for k, v := range attrs {
		copied[k] = v
	}
	s.events = append(s.events, &pb.Event{
		Name:            name,
		TimestampMicros: time.Now().UnixMicro(),
		Attributes:      copied,
	})
}

// SetStatus: span'in sonucunu isaretler.
func (s *Span) SetStatus(code pb.StatusCode, description string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.statusCode = code
	s.statusDesc = description
}

// RecordError: hatayi hem olay hem durum olarak kaydeder.
func (s *Span) RecordError(err error) {
	if s == nil || err == nil {
		return
	}
	s.AddEvent("exception", map[string]string{
		"exception.message": err.Error(),
	})
	s.SetStatus(pb.StatusCode_STATUS_CODE_ERROR, err.Error())
}

// End: span'i bitirir ve exporter'a gonderir.
// Ikinci kez cagrilmasi guvenlidir (defer + erken End birlikte kullanilabilir).
func (s *Span) End() {
	if s == nil {
		return
	}

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.endTime = time.Now()
	proto := s.toProtoLocked()
	s.mu.Unlock()

	// Orneklenmemis span'ler uretilir ama gonderilmez: baglam yayilimi
	// icin kimlikleri gerekir, depolama icin degil.
	if !s.spanContext.IsSampled() || s.tracer == nil || s.tracer.exporter == nil {
		return
	}

	// Export engellememeli: uygulamanin sicak yolundayiz.
	// Kafka exporter'i zaten tamponluyor.
	_ = s.tracer.exporter.ExportSpans(context.Background(), []*pb.Span{proto})
}

// Duration: span'in suresi. Bitmemisse su ana kadar gecen sure.
func (s *Span) Duration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return s.endTime.Sub(s.startTime)
	}
	return time.Since(s.startTime)
}

func (s *Span) toProtoLocked() *pb.Span {
	attrs := make(map[string]string, len(s.attributes))
	for k, v := range s.attributes {
		attrs[k] = v
	}

	var parentID string
	if s.hasParent {
		parentID = s.parentSpanID.String()
	}

	return &pb.Span{
		TraceId:         s.spanContext.TraceID.String(),
		SpanId:          s.spanContext.SpanID.String(),
		ParentSpanId:    parentID,
		OperationName:   s.name,
		ServiceName:     s.tracer.serviceName,
		StartTimeMicros: s.startTime.UnixMicro(),
		EndTimeMicros:   s.endTime.UnixMicro(),
		Kind:            s.kind,
		Status: &pb.SpanStatus{
			Code:        s.statusCode,
			Description: s.statusDesc,
		},
		Attributes: attrs,
		Events:     s.events,
		TraceState: s.spanContext.TraceState,
	}
}

// --- Sampler uygulamalari ---------------------------------------------------

// AlwaysSample: her trace kaydedilir. Gelistirme icin dogru varsayilan.
type AlwaysSample struct{}

func (AlwaysSample) ShouldSample(TraceID) bool { return true }
func (AlwaysSample) Description() string       { return "AlwaysSample" }

// NeverSample: hicbir trace kaydedilmez. Baglam yayilimi yine calisir.
type NeverSample struct{}

func (NeverSample) ShouldSample(TraceID) bool { return false }
func (NeverSample) Description() string       { return "NeverSample" }

// RatioSampler: trace'lerin belirli bir oranini kaydeder.
//
// Karar trace_id'nin kendisinden turetilir, rastgele sayidan degil.
// Bu onemli: ayni trace_id'yi goren her servis ayni karari verir, yani
// bir trace ya butun olarak kaydedilir ya da hic kaydedilmez.
type RatioSampler struct {
	ratio     float64
	threshold uint64
}

// NewRatioSampler: 0.0 (hicbiri) ile 1.0 (hepsi) arasinda bir oran.
func NewRatioSampler(ratio float64) RatioSampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return RatioSampler{
		ratio:     ratio,
		threshold: uint64(ratio * float64(^uint64(0))),
	}
}

func (r RatioSampler) ShouldSample(id TraceID) bool {
	if r.ratio <= 0 {
		return false
	}
	if r.ratio >= 1 {
		return true
	}
	// trace_id'nin son 8 baytini isaret olarak kullan.
	var v uint64
	for _, b := range id[8:] {
		v = v<<8 | uint64(b)
	}
	return v < r.threshold
}

func (r RatioSampler) Description() string { return "RatioSampler" }
