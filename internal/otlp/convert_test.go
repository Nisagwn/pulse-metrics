package otlp

import (
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// --- yardimcilar -------------------------------------------------------------

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func resource(kvs ...*commonpb.KeyValue) *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: kvs}
}

// id: n uzunlugunda, son bayti d olan gecerli bir kimlik.
func id(n int, d byte) []byte {
	b := make([]byte, n)
	b[n-1] = d
	return b
}

// --- oznitelikler ------------------------------------------------------------

func TestAnyValueString(t *testing.T) {
	cases := []struct {
		name string
		in   *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"metin", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "merhaba"}}, "merhaba"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: -42}}, "-42"},
		// 'g' bicimi kuyruk sifirlari birakmaz: "100.000000" degil "100".
		{"tam sayi double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 100}}, "100"},
		{"ondalik double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.5}}, "1.5"},
		{"baytlar", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3}}}, "AQID"},
		{"dizi", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{
				{Value: &commonpb.AnyValue_StringValue{StringValue: "a"}},
				{Value: &commonpb.AnyValue_IntValue{IntValue: 2}},
			}}}}, "[a,2]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := AnyValueString(tc.in); got != tc.want {
				t.Errorf("AnyValueString = %q, %q bekleniyordu", got, tc.want)
			}
		})
	}
}

// Ic ice haritalar noktali anahtarlarla duzlestirilmeli: PulseMetrics'in
// oznitelik tipi duz map<string,string>.
func TestAttributesDuzlestirme(t *testing.T) {
	nested := &commonpb.KeyValue{Key: "http", Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
			Values: []*commonpb.KeyValue{
				str("method", "GET"),
				{Key: "header", Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
						Values: []*commonpb.KeyValue{str("accept", "json")},
					}}}},
			}}}}}

	got := Attributes([]*commonpb.KeyValue{str("db.system", "postgres"), nested})

	want := map[string]string{
		"db.system":          "postgres",
		"http.method":        "GET",
		"http.header.accept": "json",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%q = %q, %q bekleniyordu (tumu: %v)", k, got[k], v, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d oznitelik, %d bekleniyordu: %v", len(got), len(want), got)
	}
}

func TestAttributesBosDonerNil(t *testing.T) {
	if got := Attributes(nil); got != nil {
		t.Errorf("bos liste nil dondurmeli, %v alindi", got)
	}
}

// hexID gecersiz kimlikleri reddetmeli: yanlis uzunluk ya da tamami sifir.
func TestHexID(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want int // beklenen uzunluk; 0 = reddedilmeli
	}{
		{"gecerli 16", id(16, 7), 32},
		{"gecerli 8", id(8, 9), 16},
		{"kisa", make([]byte, 4), 0},
		{"bos", nil, 0},
		{"tamami sifir", make([]byte, 16), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := 16
			if len(tc.in) == 8 {
				want = 8
			}
			got := hexID(tc.in, want)
			if len(got) != tc.want {
				t.Errorf("hexID uzunlugu %d, %d bekleniyordu (%q)", len(got), tc.want, got)
			}
		})
	}
}

// --- trace -------------------------------------------------------------------

func TestConvertTraces(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		Resource: resource(
			str("service.name", "odeme-servisi"),
			str("service.instance.id", "pod-7"),
			str("deployment.environment", "prod"),
		),
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{Name: "opentelemetry.instrumentation.flask"},
			Spans: []*tracepb.Span{{
				TraceId:           id(16, 1),
				SpanId:            id(8, 2),
				ParentSpanId:      id(8, 3),
				Name:              "POST /odeme",
				Kind:              tracepb.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: 1_000_000_000,
				EndTimeUnixNano:   1_250_000_000,
				TraceState:        "vendor=x",
				Attributes: []*commonpb.KeyValue{
					str("http.method", "POST"),
					str(AttrPeerService, "gateway"),
				},
				Status: &tracepb.Status{
					Code: tracepb.Status_STATUS_CODE_ERROR, Message: "kart reddedildi"},
				Events: []*tracepb.Span_Event{{
					Name: "exception", TimeUnixNano: 1_100_000_000,
					Attributes: []*commonpb.KeyValue{str("exception.type", "CardDeclined")},
				}},
			}},
		}},
	}}

	payloads, rejected := ConvertTraces(rs)
	if rejected != 0 {
		t.Fatalf("hicbir span reddedilmemeliydi, %d", rejected)
	}
	if len(payloads) != 1 {
		t.Fatalf("1 yuk bekleniyordu, %d", len(payloads))
	}

	p := payloads[0]
	if p.ServiceName != "odeme-servisi" {
		t.Errorf("service_name = %q", p.ServiceName)
	}
	if p.InstanceId != "pod-7" {
		t.Errorf("instance_id = %q", p.InstanceId)
	}
	if len(p.Spans) != 1 {
		t.Fatalf("1 span bekleniyordu, %d", len(p.Spans))
	}

	s := p.Spans[0]
	if s.OperationName != "POST /odeme" {
		t.Errorf("operation_name = %q", s.OperationName)
	}
	if s.Kind != pb.SpanKind_SPAN_KIND_SERVER {
		t.Errorf("kind = %v", s.Kind)
	}
	if s.Status.GetCode() != pb.StatusCode_STATUS_CODE_ERROR {
		t.Errorf("status = %v", s.Status.GetCode())
	}
	if s.Status.GetDescription() != "kart reddedildi" {
		t.Errorf("status aciklamasi = %q", s.Status.GetDescription())
	}
	// Nanosaniye -> mikrosaniye.
	if s.StartTimeMicros != 1_000_000 || s.EndTimeMicros != 1_250_000 {
		t.Errorf("zaman damgalari = %d..%d, 1000000..1250000 bekleniyordu",
			s.StartTimeMicros, s.EndTimeMicros)
	}
	if s.TraceState != "vendor=x" {
		t.Errorf("trace_state = %q", s.TraceState)
	}
	if len(s.TraceId) != 32 || len(s.SpanId) != 16 || len(s.ParentSpanId) != 16 {
		t.Errorf("kimlik uzunluklari yanlis: %q %q %q", s.TraceId, s.SpanId, s.ParentSpanId)
	}

	// Kaynak oznitelikleri span'e karismali, ama service.name karismamali:
	// o zaten ServiceName alaninda, tekrari olu veri olurdu.
	if s.Attributes["deployment.environment"] != "prod" {
		t.Errorf("kaynak ozniteligi tasinmadi: %v", s.Attributes)
	}
	if _, ok := s.Attributes["service.name"]; ok {
		t.Error("service.name oznitelik olarak da tasinmamali")
	}
	// Enstrumantasyon kutuphanesi kaydedilmeli.
	if s.Attributes["otel.scope.name"] != "opentelemetry.instrumentation.flask" {
		t.Errorf("scope adi yok: %v", s.Attributes)
	}
	// FAZ 3'UN KARSILIGI: peer.service OTel'in standart ozniteligi, yani
	// harici bir SDK gonderdiginde servis haritasi kendiliginden calisir.
	if s.Attributes[AttrPeerService] != "gateway" {
		t.Errorf("peer.service tasinmadi: %v", s.Attributes)
	}

	if len(s.Events) != 1 || s.Events[0].Name != "exception" {
		t.Fatalf("olay tasinmadi: %v", s.Events)
	}
	if s.Events[0].TimestampMicros != 1_100_000 {
		t.Errorf("olay zamani = %d", s.Events[0].TimestampMicros)
	}
}

// Gecersiz kimlikli span'ler reddedilmeli VE sayilmali: sessizce
// dusurmek, istemcinin veri gonderdigini sanmasina yol acar.
func TestConvertTracesGecersizKimlikReddedilir(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		Resource: resource(str("service.name", "svc")),
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{
				{TraceId: make([]byte, 4), SpanId: id(8, 1), Name: "kisa trace id"},
				{TraceId: id(16, 1), SpanId: make([]byte, 8), Name: "sifir span id"},
				{TraceId: id(16, 1), SpanId: id(8, 2), Name: "gecerli",
					StartTimeUnixNano: 1_000_000_000},
			},
		}},
	}}

	payloads, rejected := ConvertTraces(rs)
	if rejected != 2 {
		t.Errorf("2 span reddedilmeliydi, %d", rejected)
	}
	if len(payloads) != 1 || len(payloads[0].Spans) != 1 {
		t.Fatalf("1 gecerli span kalmaliydi: %v", payloads)
	}
	if payloads[0].Spans[0].OperationName != "gecerli" {
		t.Errorf("yanlis span gecti: %q", payloads[0].Spans[0].OperationName)
	}
}

func TestConvertTracesServisAdiYok(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{TraceId: id(16, 1), SpanId: id(8, 1)}},
		}},
	}}
	payloads, _ := ConvertTraces(rs)
	if len(payloads) != 1 || payloads[0].ServiceName != unknownService {
		t.Errorf("servis adi olmayan veri %q altinda toplanmali, %v", unknownService, payloads)
	}
}

// --- log ---------------------------------------------------------------------

// OTel'in 24 kademeli siddet olcegi PulseMetrics'in 5 seviyesine inmeli.
func TestSeverityEslesmesi(t *testing.T) {
	cases := []struct {
		n    logspb.SeverityNumber
		want pb.LogLevel
	}{
		{logspb.SeverityNumber_SEVERITY_NUMBER_TRACE, pb.LogLevel_LEVEL_DEBUG},
		{logspb.SeverityNumber_SEVERITY_NUMBER_TRACE4, pb.LogLevel_LEVEL_DEBUG},
		{logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, pb.LogLevel_LEVEL_DEBUG},
		{logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG4, pb.LogLevel_LEVEL_DEBUG},
		{logspb.SeverityNumber_SEVERITY_NUMBER_INFO, pb.LogLevel_LEVEL_INFO},
		{logspb.SeverityNumber_SEVERITY_NUMBER_INFO4, pb.LogLevel_LEVEL_INFO},
		{logspb.SeverityNumber_SEVERITY_NUMBER_WARN, pb.LogLevel_LEVEL_WARN},
		{logspb.SeverityNumber_SEVERITY_NUMBER_WARN4, pb.LogLevel_LEVEL_WARN},
		{logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, pb.LogLevel_LEVEL_ERROR},
		{logspb.SeverityNumber_SEVERITY_NUMBER_ERROR4, pb.LogLevel_LEVEL_ERROR},
		{logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, pb.LogLevel_LEVEL_FATAL},
		{logspb.SeverityNumber_SEVERITY_NUMBER_FATAL4, pb.LogLevel_LEVEL_FATAL},
	}
	for _, tc := range cases {
		if got := convertSeverity(tc.n, ""); got != tc.want {
			t.Errorf("siddet %v -> %v, %v bekleniyordu", tc.n, got, tc.want)
		}
	}
}

// Bazi kutuphaneler yalnizca metin gonderiyor. Sayi yoksa metni okumak,
// her seyi INFO'ya dusurmekten iyi - aksi halde seviye filtresi bu
// kayitlari hic gostermezdi.
func TestSeverityMetinGeriDonusu(t *testing.T) {
	cases := map[string]pb.LogLevel{
		"error":    pb.LogLevel_LEVEL_ERROR,
		"WARNING":  pb.LogLevel_LEVEL_WARN,
		"Critical": pb.LogLevel_LEVEL_FATAL,
		"debug":    pb.LogLevel_LEVEL_DEBUG,
		"":         pb.LogLevel_LEVEL_INFO, // bilgi yoksa makul varsayilan
		"saçma":    pb.LogLevel_LEVEL_INFO,
	}
	for text, want := range cases {
		if got := convertSeverity(0, text); got != want {
			t.Errorf("metin %q -> %v, %v bekleniyordu", text, got, want)
		}
	}
}

func TestConvertLogs(t *testing.T) {
	rl := []*logspb.ResourceLogs{{
		Resource: resource(str("service.name", "siparis"), str("host.name", "makine-3")),
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "app.logger"},
			LogRecords: []*logspb.LogRecord{
				{
					TimeUnixNano:   2_000_000_000,
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "odeme basarisiz"}},
					TraceId:    id(16, 5),
					SpanId:     id(8, 6),
					Attributes: []*commonpb.KeyValue{str("exception.stacktrace", "at foo()")},
				},
				// Zaman damgasi yok: TimeUnixNano bos, ObservedTime dolu.
				{
					ObservedTimeUnixNano: 3_000_000_000,
					SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "gozlemlenen"}},
				},
				// Hicbir zaman damgasi yok: reddedilmeli.
				{SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO},
			},
		}},
	}}

	payloads, rejected := ConvertLogs(rl)
	if rejected != 1 {
		t.Errorf("1 kayit reddedilmeliydi (zaman damgasiz), %d", rejected)
	}
	if len(payloads) != 1 || len(payloads[0].Logs) != 2 {
		t.Fatalf("2 kayit bekleniyordu: %v", payloads)
	}

	p := payloads[0]
	if p.InstanceId != "makine-3" {
		t.Errorf("instance_id host.name'e dusmeliydi, %q", p.InstanceId)
	}

	first := p.Logs[0]
	if first.Level != pb.LogLevel_LEVEL_ERROR {
		t.Errorf("seviye = %v", first.Level)
	}
	if first.Message != "odeme basarisiz" {
		t.Errorf("mesaj = %q", first.Message)
	}
	if first.TimestampMs != 2000 {
		t.Errorf("zaman damgasi = %d ms, 2000 bekleniyordu", first.TimestampMs)
	}
	if first.LoggerName != "app.logger" {
		t.Errorf("logger adi = %q", first.LoggerName)
	}
	// TRACE KORELASYONU: uc ayagi birbirine baglayan sey.
	if len(first.TraceId) != 32 || len(first.SpanId) != 16 {
		t.Errorf("trace baglantisi kurulamadi: %q / %q", first.TraceId, first.SpanId)
	}
	// OTel'in istisna sozlesmesi.
	if first.StackTrace != "at foo()" {
		t.Errorf("yigin izi tasinmadi: %q", first.StackTrace)
	}

	if p.Logs[1].TimestampMs != 3000 {
		t.Errorf("gozlem zamanina dusmeliydi, %d", p.Logs[1].TimestampMs)
	}
}

// --- metrik ------------------------------------------------------------------

func numberDP(value float64, ts uint64, attrs ...*commonpb.KeyValue) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		TimeUnixNano: ts,
		Attributes:   attrs,
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
	}
}

func convertOne(t *testing.T, m *metricspb.Metric) ([]*pb.Metric, int64) {
	t.Helper()
	rm := []*metricspb.ResourceMetrics{{
		Resource:     resource(str("service.name", "svc")),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{m}}},
	}}
	payloads, rejected := ConvertMetrics(rm)
	if len(payloads) == 0 {
		return nil, rejected
	}
	return payloads[0].Metrics, rejected
}

func TestConvertMetricsGauge(t *testing.T) {
	got, rejected := convertOne(t, &metricspb.Metric{
		Name: "process.memory",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
			DataPoints: []*metricspb.NumberDataPoint{numberDP(42.5, 5_000_000_000)},
		}},
	})
	if rejected != 0 || len(got) != 1 {
		t.Fatalf("1 metrik bekleniyordu, %d (red: %d)", len(got), rejected)
	}
	if got[0].Type != pb.MetricType_GAUGE || got[0].Value != 42.5 {
		t.Errorf("gauge = %v %v", got[0].Type, got[0].Value)
	}
	if got[0].TimestampMillis != 5000 {
		t.Errorf("zaman damgasi = %d ms", got[0].TimestampMillis)
	}
}

// Monotonik olmayan bir Sum aslinda bir gauge'dir. COUNTER isaretlemek,
// panelde artis hizi hesaplanirken negatif degerler uretirdi.
func TestConvertMetricsSumMonotoniklik(t *testing.T) {
	for _, monotonic := range []bool{true, false} {
		got, _ := convertOne(t, &metricspb.Metric{
			Name: "istekler",
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				IsMonotonic: monotonic,
				DataPoints:  []*metricspb.NumberDataPoint{numberDP(7, 1_000_000_000)},
			}},
		})
		want := pb.MetricType_GAUGE
		if monotonic {
			want = pb.MetricType_COUNTER
		}
		if len(got) != 1 || got[0].Type != want {
			t.Errorf("monotonic=%v -> %v, %v bekleniyordu", monotonic, got[0].Type, want)
		}
	}
}

func TestConvertMetricsGecersizDegerReddedilir(t *testing.T) {
	for name, v := range map[string]float64{"NaN": math.NaN(), "Inf": math.Inf(1)} {
		t.Run(name, func(t *testing.T) {
			got, rejected := convertOne(t, &metricspb.Metric{
				Name: "bozuk",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{numberDP(v, 1_000_000_000)},
				}},
			})
			if rejected != 1 || len(got) != 0 {
				t.Errorf("%s reddedilmeliydi: %d metrik, %d red", name, len(got), rejected)
			}
		})
	}
}

// Histogramin kovalari ayri metrik ADLARINA aciliyor. Etikete koysaydik
// hepsi ayni partition + ayni clustering key'e dusup birbirini ezerdi.
func TestConvertMetricsHistogram(t *testing.T) {
	sum := 12.5
	got, rejected := convertOne(t, &metricspb.Metric{
		Name: "http.duration",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints: []*metricspb.HistogramDataPoint{{
				TimeUnixNano:   1_000_000_000,
				Count:          10,
				Sum:            &sum,
				ExplicitBounds: []float64{0.1, 0.5},
				BucketCounts:   []uint64{3, 5, 2}, // ayrik sayaclar
			}},
		}},
	})
	if rejected != 0 {
		t.Fatalf("red beklenmiyordu: %d", rejected)
	}

	byName := map[string]*pb.Metric{}
	for _, m := range got {
		byName[m.Name] = m
	}

	if m := byName["http.duration_count"]; m == nil || m.Value != 10 {
		t.Errorf("_count yanlis: %v", m)
	}
	if m := byName["http.duration_sum"]; m == nil || m.Value != 12.5 {
		t.Errorf("_sum yanlis: %v", m)
	}

	// OTLP ayrik sayar, Prometheus kumulatif. Donusum: 3, 3+5=8, 8+2=10.
	want := map[string]float64{
		"http.duration_bucket_le_0.1": 3,
		"http.duration_bucket_le_0.5": 8,
		"http.duration_bucket_le_inf": 10,
	}
	for name, v := range want {
		m := byName[name]
		if m == nil {
			t.Fatalf("kova serisi yok: %s (uretilenler: %v)", name, byName)
		}
		if m.Value != v {
			t.Errorf("%s = %v, %v bekleniyordu (kumulatif olmali)", name, m.Value, v)
		}
	}
	// le etiketi de olmali: adi ayristirmak gerekmesin.
	if byName["http.duration_bucket_le_0.5"].Tags["le"] != "0.5" {
		t.Errorf("le etiketi yok: %v", byName["http.duration_bucket_le_0.5"].Tags)
	}
}

// Bozuk histogram: OTLP sozlesmesi kova sayisinin sinir sayisindan tam
// bir fazla olmasini gerektirir.
func TestConvertMetricsHistogramBozuk(t *testing.T) {
	got, rejected := convertOne(t, &metricspb.Metric{
		Name: "bozuk.hist",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints: []*metricspb.HistogramDataPoint{{
				TimeUnixNano:   1_000_000_000,
				Count:          5,
				ExplicitBounds: []float64{0.1, 0.5},
				BucketCounts:   []uint64{3, 5}, // 3 olmaliydi
			}},
		}},
	})
	if rejected != 1 {
		t.Errorf("bozuk histogram reddedilmeliydi, red: %d", rejected)
	}
	// _count ve _sum yine de uretilmeli: onlar bozuk degil.
	if len(got) != 2 {
		t.Errorf("_count ve _sum korunmaliydi, %d metrik", len(got))
	}
}

func TestConvertMetricsSummary(t *testing.T) {
	got, _ := convertOne(t, &metricspb.Metric{
		Name: "rpc.duration",
		Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
			DataPoints: []*metricspb.SummaryDataPoint{{
				TimeUnixNano: 1_000_000_000,
				Count:        100,
				Sum:          250,
				QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{
					{Quantile: 0.5, Value: 1.2},
					{Quantile: 0.99, Value: 9.8},
				},
			}},
		}},
	})

	byName := map[string]float64{}
	for _, m := range got {
		byName[m.Name] = m.Value
	}
	for name, want := range map[string]float64{
		"rpc.duration_count": 100,
		"rpc.duration_sum":   250,
		"rpc.duration_q0.5":  1.2,
		"rpc.duration_q0.99": 9.8,
	} {
		if byName[name] != want {
			t.Errorf("%s = %v, %v bekleniyordu (uretilenler: %v)", name, byName[name], want, byName)
		}
	}
}

// Ustel histogram desteklenmiyor - ama SESSIZCE dusurulmuyor. Istemci
// partial_success alaninda kac veri noktasinin kabul edilmedigini gorur.
func TestConvertMetricsUstelHistogramSayilir(t *testing.T) {
	got, rejected := convertOne(t, &metricspb.Metric{
		Name: "ustel",
		Data: &metricspb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricspb.ExponentialHistogram{
				DataPoints: []*metricspb.ExponentialHistogramDataPoint{
					{TimeUnixNano: 1_000_000_000}, {TimeUnixNano: 2_000_000_000},
				},
			}},
	})
	if rejected != 2 {
		t.Errorf("2 veri noktasi reddedilmeliydi, %d", rejected)
	}
	if len(got) != 0 {
		t.Errorf("hicbir metrik uretilmemeliydi, %d", len(got))
	}
}

func TestFormatBound(t *testing.T) {
	cases := map[float64]string{
		0.005:        "0.005",
		5:            "5",
		0.5:          "0.5",
		math.Inf(1):  "inf",
		math.Inf(-1): "-inf",
	}
	for in, want := range cases {
		if got := formatBound(in); got != want {
			t.Errorf("formatBound(%v) = %q, %q bekleniyordu", in, got, want)
		}
	}
	// 0.5 ile 5 ayni ada dusmemeli: ondalik korunuyor.
	if formatBound(0.5) == formatBound(5) {
		t.Error("0.5 ile 5 ayni metrik adina dusuyor")
	}
}
