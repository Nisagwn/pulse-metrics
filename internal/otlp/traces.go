package otlp

import (
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// Semantic Conventions: OpenTelemetry'nin standart oznitelik adlari.
//
// Bunlar uydurma degil, OTel'in yayimladigi sozlesme. Ayni adlari
// kullanmak, herhangi bir dilin resmi SDK'sinin urettigi verinin
// PulseMetrics'te dogru yere oturmasini sagliyor.
const (
	AttrServiceName       = "service.name"
	AttrServiceInstanceID = "service.instance.id"
	AttrHostName          = "host.name"
	// AttrPeerService: cagiran servisin adi. Faz 3'te topolojiyi ingest
	// aninda hesaplamak icin kendi SDK'mize eklemistik; mesele su ki bu
	// zaten OTel'in STANDART ozniteligi. Yani harici bir OTel SDK'si bunu
	// gonderdiginde servis haritasi hicbir ek is yapmadan calisiyor.
	AttrPeerService = "peer.service"
)

const unknownService = "unknown_service"

// ConvertTraces: OTLP span'lerini servis bazli PulseMetrics yuklerine cevirir.
//
// Neden servis bazli grupluyoruz? Cunku PulseMetrics'in Kafka yuku
// (TracesPayload) tek bir servise ait. OTLP ise tek istekte birden fazla
// ResourceSpans tasiyabilir - ornegin bir OTel Collector'un topladigi
// farkli servislerin verisi. Her kaynak ayri bir yuk oluyor.
//
// rejected: cevrilemeyen span sayisi. Cagiran bunu OTLP'nin
// partial_success alanina koyuyor; istemci neyin kabul edilmedigini
// ogreniyor. Sessizce dusurmek, istemcinin veri gonderdigini sanmasina
// yol acardi.
func ConvertTraces(resourceSpans []*tracepb.ResourceSpans) (payloads []*pb.TracesPayload, rejected int64) {
	for _, rs := range resourceSpans {
		if rs == nil {
			continue
		}

		resAttrs := Attributes(rs.GetResource().GetAttributes())
		service := resAttrs[AttrServiceName]
		if service == "" {
			service = unknownService
		}
		instance := resAttrs[AttrServiceInstanceID]
		if instance == "" {
			instance = resAttrs[AttrHostName]
		}
		if instance == "" {
			instance = "otlp"
		}

		// service.name artik ServiceName alaninda; oznitelik olarak da
		// tasimak her span'de tekrar eden olu veri olurdu.
		delete(resAttrs, AttrServiceName)

		var spans []*pb.Span
		for _, ss := range rs.GetScopeSpans() {
			// Scope, enstrumantasyon kutuphanesinin adi
			// ("net/http", "django"). Hangi kutuphanenin urettigini
			// bilmek hata ayiklarken ise yariyor.
			scope := ss.GetScope().GetName()

			for _, span := range ss.GetSpans() {
				converted := convertSpan(span, service, scope, resAttrs)
				if converted == nil {
					rejected++
					continue
				}
				spans = append(spans, converted)
			}
		}

		if len(spans) == 0 {
			continue
		}
		payloads = append(payloads, &pb.TracesPayload{
			ServiceName: service,
			InstanceId:  instance,
			Spans:       spans,
			Timestamp:   timestamppb.Now(),
		})
	}
	return payloads, rejected
}

func convertSpan(s *tracepb.Span, service, scope string, resAttrs map[string]string) *pb.Span {
	if s == nil {
		return nil
	}

	traceID := hexID(s.GetTraceId(), 16)
	spanID := hexID(s.GetSpanId(), 8)
	if traceID == "" || spanID == "" {
		// Gecersiz kimlikli bir span, hicbir zaman bulunamayacak bir
		// span demek: ne trace'ine baglanabilir ne aranabilir.
		return nil
	}

	attrs := merge(Attributes(s.GetAttributes()), resAttrs)
	if scope != "" {
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["otel.scope.name"] = scope
	}

	out := &pb.Span{
		TraceId:       traceID,
		SpanId:        spanID,
		ParentSpanId:  hexID(s.GetParentSpanId(), 8),
		OperationName: s.GetName(),
		ServiceName:   service,
		// OTLP nanosaniye tasir, PulseMetrics mikrosaniye saklar.
		// Bolme kayipli ama kasitli: mikrosaniye cozunurlugu dagitik
		// izleme icin fazlasiyla yeterli ve int64 tasmasindan uzak.
		StartTimeMicros: int64(s.GetStartTimeUnixNano() / 1000),
		EndTimeMicros:   int64(s.GetEndTimeUnixNano() / 1000),
		Kind:            convertSpanKind(s.GetKind()),
		TraceState:      s.GetTraceState(),
		Attributes:      attrs,
	}

	if st := s.GetStatus(); st != nil {
		out.Status = &pb.SpanStatus{
			Code:        convertStatusCode(st.GetCode()),
			Description: st.GetMessage(),
		}
	}

	for _, ev := range s.GetEvents() {
		out.Events = append(out.Events, &pb.Event{
			Name:            ev.GetName(),
			TimestampMicros: int64(ev.GetTimeUnixNano() / 1000),
			Attributes:      Attributes(ev.GetAttributes()),
		})
	}

	for _, link := range s.GetLinks() {
		lt := hexID(link.GetTraceId(), 16)
		ls := hexID(link.GetSpanId(), 8)
		if lt == "" || ls == "" {
			continue
		}
		out.Links = append(out.Links, &pb.Link{
			TraceId:    lt,
			SpanId:     ls,
			Attributes: Attributes(link.GetAttributes()),
		})
	}

	return out
}

// convertSpanKind: OTLP span turlerini PulseMetrics turlerine esler.
// Iki model burada birebir ortusuyor, cunku PulseMetrics'in span
// modeli bastan OTel'e bakilarak tasarlandi.
func convertSpanKind(k tracepb.Span_SpanKind) pb.SpanKind {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return pb.SpanKind_SPAN_KIND_INTERNAL
	case tracepb.Span_SPAN_KIND_SERVER:
		return pb.SpanKind_SPAN_KIND_SERVER
	case tracepb.Span_SPAN_KIND_CLIENT:
		return pb.SpanKind_SPAN_KIND_CLIENT
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return pb.SpanKind_SPAN_KIND_PRODUCER
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return pb.SpanKind_SPAN_KIND_CONSUMER
	default:
		return pb.SpanKind_SPAN_KIND_UNSPECIFIED
	}
}

func convertStatusCode(c tracepb.Status_StatusCode) pb.StatusCode {
	switch c {
	case tracepb.Status_STATUS_CODE_OK:
		return pb.StatusCode_STATUS_CODE_OK
	case tracepb.Status_STATUS_CODE_ERROR:
		return pb.StatusCode_STATUS_CODE_ERROR
	default:
		return pb.StatusCode_STATUS_CODE_UNSET
	}
}
