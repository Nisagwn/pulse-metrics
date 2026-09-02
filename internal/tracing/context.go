// Package tracing: PulseMetrics'in dagitik izleme (distributed tracing) SDK'si.
//
// Temel fikir: bir kullanici istegi 5 servise ugruyorsa, bu 5 servisin
// urettigi kayitlarin ayni "trace"e ait oldugunu bilmemiz gerekir. Bunu
// saglayan sey, servisten servise tasinan kucuk bir kimlik paketidir:
// trace_id (tum yolculuk boyunca sabit) ve span_id (bu adima ozel).
//
// Tasima bicimi W3C Trace Context standardidir, cunku standart olmayan bir
// bicim secersek OpenTelemetry ile enstrumante edilmis servislerle
// konusamayiz:
//
//	traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//	             |  |                                |                |
//	             |  trace_id (16 bayt / 32 hex)      |                trace_flags
//	             version                             span_id (8 bayt / 16 hex)
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
)

// TraceFlags bitleri (W3C).
const (
	// FlagSampled: bu trace kaydedilecek. Sifirsa alt servisler de
	// kaydetmemeli - orneklem karari yolculuk boyunca tasinir.
	FlagSampled byte = 0x01
)

var (
	// ErrInvalidTraceParent: traceparent basligi bicime uymuyor.
	ErrInvalidTraceParent = errors.New("gecersiz traceparent basligi")

	zeroTraceID TraceID
	zeroSpanID  SpanID
)

// TraceID: 16 baytlik trace kimligi.
type TraceID [16]byte

// SpanID: 8 baytlik span kimligi.
type SpanID [8]byte

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

// IsValid: sifir olmayan bir kimlik mi? W3C sifir kimlikleri gecersiz sayar.
func (t TraceID) IsValid() bool { return t != zeroTraceID }
func (s SpanID) IsValid() bool  { return s != zeroSpanID }

// ParseTraceID: 32 hex karakterden TraceID uretir.
func ParseTraceID(s string) (TraceID, error) {
	var id TraceID
	if len(s) != 32 {
		return id, ErrInvalidTraceParent
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, ErrInvalidTraceParent
	}
	copy(id[:], b)
	if !id.IsValid() {
		return id, ErrInvalidTraceParent
	}
	return id, nil
}

// ParseSpanID: 16 hex karakterden SpanID uretir.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	if len(s) != 16 {
		return id, ErrInvalidTraceParent
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, ErrInvalidTraceParent
	}
	copy(id[:], b)
	if !id.IsValid() {
		return id, ErrInvalidTraceParent
	}
	return id, nil
}

func newTraceID() TraceID {
	var id TraceID
	// crypto/rand hata dondurebilir ama pratikte donmez; donerse de
	// gecersiz (sifir) bir kimlik uretmektense panic etmek yerine
	// gecersiz birakip cagiranin IsValid kontrolune birakiyoruz.
	_, _ = rand.Read(id[:])
	return id
}

func newSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// SpanContext: bir span'i tanimlayan ve servisler arasi tasinan bilgi.
// Degismezdir (immutable); yeni bir span icin yeni bir tane uretilir.
type SpanContext struct {
	TraceID    TraceID
	SpanID     SpanID
	TraceFlags byte
	TraceState string // W3C tracestate: saticiya ozel anahtarlar
	// Remote: bu context ag uzerinden mi geldi? Sunucu tarafinda
	// gelen istegin ebeveyn oldugunu bilmek icin.
	Remote bool
}

// IsValid: hem trace hem span kimligi gecerli mi?
func (sc SpanContext) IsValid() bool {
	return sc.TraceID.IsValid() && sc.SpanID.IsValid()
}

// IsSampled: bu trace kaydedilecek mi?
func (sc SpanContext) IsSampled() bool {
	return sc.TraceFlags&FlagSampled != 0
}

// TraceParent: W3C traceparent baslik degerini uretir.
func (sc SpanContext) TraceParent() string {
	var sb strings.Builder
	sb.Grow(55) // 2 + 1 + 32 + 1 + 16 + 1 + 2
	sb.WriteString("00-")
	sb.WriteString(sc.TraceID.String())
	sb.WriteByte('-')
	sb.WriteString(sc.SpanID.String())
	sb.WriteByte('-')
	sb.WriteString(hex.EncodeToString([]byte{sc.TraceFlags}))
	return sb.String()
}

// ParseTraceParent: gelen traceparent basligini cozer.
//
// Bicim: version "-" trace-id "-" parent-id "-" trace-flags
// Version 00 icin tam 4 parca olmali. Ileri surumler ek parca
// ekleyebilir; standart, bilinmeyen surumleri reddetmek yerine
// ilk 4 parcayi okumayi onerir.
func ParseTraceParent(header string) (SpanContext, error) {
	var sc SpanContext

	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) < 4 {
		return sc, ErrInvalidTraceParent
	}

	if len(parts[0]) != 2 {
		return sc, ErrInvalidTraceParent
	}
	if parts[0] == "ff" { // ff gecersiz surum olarak ayrilmis
		return sc, ErrInvalidTraceParent
	}
	if parts[0] == "00" && len(parts) != 4 {
		return sc, ErrInvalidTraceParent
	}

	traceID, err := ParseTraceID(parts[1])
	if err != nil {
		return sc, err
	}
	spanID, err := ParseSpanID(parts[2])
	if err != nil {
		return sc, err
	}

	if len(parts[3]) != 2 {
		return sc, ErrInvalidTraceParent
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil {
		return sc, ErrInvalidTraceParent
	}

	sc.TraceID = traceID
	sc.SpanID = spanID
	sc.TraceFlags = flags[0]
	sc.Remote = true
	return sc, nil
}

// --- context.Context tasima -------------------------------------------------

type spanKey struct{}

// ContextWithSpan: span'i context'e koyar. Alt cagrilar bunu ebeveyn olarak bulur.
func ContextWithSpan(ctx context.Context, s *Span) context.Context {
	return context.WithValue(ctx, spanKey{}, s)
}

// SpanFromContext: context'teki aktif span. Yoksa nil doner.
func SpanFromContext(ctx context.Context) *Span {
	if s, ok := ctx.Value(spanKey{}).(*Span); ok {
		return s
	}
	return nil
}

// SpanContextFromContext: context'teki aktif span'in kimlik bilgisi.
func SpanContextFromContext(ctx context.Context) SpanContext {
	if s := SpanFromContext(ctx); s != nil {
		return s.SpanContext()
	}
	return SpanContext{}
}
