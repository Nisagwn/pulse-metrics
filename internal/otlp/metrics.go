package otlp

import (
	"math"
	"strconv"
	"strings"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// maxBuckets: bir histogram veri noktasindan uretilecek en fazla seri.
//
// Sinir gerekli cunku her kova ayri bir metrik adi uretiyor (asagidaki
// aciklamaya bak) ve 500 kovali bir histogram 500 metrik adi demek olurdu.
// Asilan kovalar reddedilmis sayilir ve istemciye bildirilir.
const maxBuckets = 64

// ConvertMetrics: OTLP metriklerini PulseMetrics yuklerine cevirir.
//
// # IKI MODELIN CATISTIGI YER
//
// OTLP'de bir metrik bes farkli sekilde gelebilir: Gauge, Sum, Histogram,
// ExponentialHistogram, Summary. PulseMetrics'in modeli ise duz:
// (ad, tip, tek bir double deger, zaman damgasi).
//
// Gauge ve Sum birebir oturuyor. Histogram ve Summary oturmuyor - cunku
// onlar tek bir sayi degil, bir DAGILIM tasiyorlar. Onlari Prometheus'un
// yaptigi gibi aciyoruz: sayac, toplam ve kova basina birer seri.
func ConvertMetrics(resourceMetrics []*metricspb.ResourceMetrics) (payloads []*pb.MetricsPayload, rejected int64) {
	for _, rm := range resourceMetrics {
		if rm == nil {
			continue
		}

		resAttrs := Attributes(rm.GetResource().GetAttributes())
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
		delete(resAttrs, AttrServiceName)

		var metrics []*pb.Metric
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				converted, skipped := convertMetric(m, resAttrs)
				metrics = append(metrics, converted...)
				rejected += skipped
			}
		}

		if len(metrics) == 0 {
			continue
		}
		payloads = append(payloads, &pb.MetricsPayload{
			ServiceName: service,
			InstanceId:  instance,
			Metrics:     metrics,
			Timestamp:   timestamppb.Now(),
		})
	}
	return payloads, rejected
}

func convertMetric(m *metricspb.Metric, resAttrs map[string]string) (out []*pb.Metric, rejected int64) {
	if m == nil || m.GetName() == "" {
		return nil, 0
	}
	name := m.GetName()

	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range data.Gauge.GetDataPoints() {
			if p := numberPoint(name, pb.MetricType_GAUGE, dp, resAttrs); p != nil {
				out = append(out, p)
			} else {
				rejected++
			}
		}

	case *metricspb.Metric_Sum:
		// Monotonik olmayan bir Sum aslinda bir gauge'dir: deger asagi da
		// inebiliyorsa onu "sayac" diye isaretlemek, panelde artis hizi
		// hesaplanirken negatif degerlere yol acar.
		kind := pb.MetricType_GAUGE
		if data.Sum.GetIsMonotonic() {
			kind = pb.MetricType_COUNTER
		}
		for _, dp := range data.Sum.GetDataPoints() {
			if p := numberPoint(name, kind, dp, resAttrs); p != nil {
				out = append(out, p)
			} else {
				rejected++
			}
		}

	case *metricspb.Metric_Histogram:
		for _, dp := range data.Histogram.GetDataPoints() {
			ms, skipped := histogramPoints(name, dp, resAttrs)
			out = append(out, ms...)
			rejected += skipped
		}

	case *metricspb.Metric_Summary:
		for _, dp := range data.Summary.GetDataPoints() {
			out = append(out, summaryPoints(name, dp, resAttrs)...)
		}

	case *metricspb.Metric_ExponentialHistogram:
		// Ustel histogramlarda kova sinirlari acikca verilmez, bir taban
		// ve olcek uzerinden hesaplanir. Cevirisi mumkun ama PulseMetrics
		// tarafinda karsiligi olmadigi icin desteklenmiyor.
		//
		// Sessizce yutmak yerine SAYIYORUZ: istemci partial_success
		// alaninda kac veri noktasinin kabul edilmedigini goruyor. Veri
		// gonderdigini sanip hicbir sey gormemekten iyi.
		for _, dp := range data.ExponentialHistogram.GetDataPoints() {
			_ = dp
			rejected++
		}
	}

	return out, rejected
}

func numberPoint(name string, kind pb.MetricType, dp *metricspb.NumberDataPoint, resAttrs map[string]string) *pb.Metric {
	if dp == nil || dp.GetTimeUnixNano() == 0 {
		return nil
	}

	var value float64
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		value = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		value = float64(v.AsInt)
	default:
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		// NaN bir zaman serisinde saklanabilir ama her toplama islemini
		// (avg, p95, hatta max) zehirler. Kaynakta durdurmak dogru.
		return nil
	}

	return &pb.Metric{
		Name:            name,
		Type:            kind,
		Value:           value,
		Tags:            merge(Attributes(dp.GetAttributes()), resAttrs),
		TimestampMillis: int64(dp.GetTimeUnixNano() / 1e6),
	}
}

// histogramPoints: bir histogram veri noktasini serilere acar.
//
// # KOVA SINIRI NEDEN METRIK ADINDA?
//
// Prometheus'ta bir histogramin kovalari ayri zaman serileridir:
// http_duration_bucket{le="0.5"} ile {le="1"} farkli serilerdir.
// PulseMetrics'te "ayri seri"nin karsiligi ayri PARTITION, partition
// key ise (servis, metrik_adi, kova). Etiketler partition key'in parcasi
// DEGIL.
//
// Yani le'yi etikete koysaydik butun kovalar ayni partition'a, ayni
// zaman damgasina ve ayni instance_id'ye yazilirdi - clustering key
// birebir cakisir ve sadece SON kova hayatta kalirdi. Faz 1'de
// instance_id ile yasanan sessiz veri kaybinin aynisi.
//
// Bu yuzden sinir metrik adina giriyor. Cirkin ama dogru; ve aslinda
// Prometheus'un yaptiginin bire bir karsiligi.
func histogramPoints(name string, dp *metricspb.HistogramDataPoint, resAttrs map[string]string) (out []*pb.Metric, rejected int64) {
	if dp == nil || dp.GetTimeUnixNano() == 0 {
		return nil, 1
	}
	ts := int64(dp.GetTimeUnixNano() / 1e6)
	tags := merge(Attributes(dp.GetAttributes()), resAttrs)

	// _count ve _sum: her zaman guvenli, cakisma yok. Ikisi birlikte
	// ortalamayi da verir (sum/count).
	out = append(out, &pb.Metric{
		Name: name + "_count", Type: pb.MetricType_COUNTER,
		Value: float64(dp.GetCount()), Tags: tags, TimestampMillis: ts,
	})
	out = append(out, &pb.Metric{
		Name: name + "_sum", Type: pb.MetricType_COUNTER,
		Value: dp.GetSum(), Tags: tags, TimestampMillis: ts,
	})

	bounds := dp.GetExplicitBounds()
	counts := dp.GetBucketCounts()
	// OTLP sozlesmesi: kova sayisi, sinir sayisindan tam bir fazla
	// olmali (sonuncusu +Inf kovasi). Uymuyorsa veri bozuk.
	if len(counts) != len(bounds)+1 {
		return out, 1
	}

	// OTLP kova sayaclari AYRIK, Prometheus'unkiler KUMULATIF.
	// Donusum icin toplaya toplaya ilerliyoruz.
	var cumulative uint64
	for i, c := range counts {
		if i >= maxBuckets {
			rejected += int64(len(counts) - i)
			break
		}
		cumulative += c

		le := "inf"
		if i < len(bounds) {
			le = formatBound(bounds[i])
		}

		bucketTags := make(map[string]string, len(tags)+1)
		for k, v := range tags {
			bucketTags[k] = v
		}
		// Sinir hem adda hem etikette: ad cakismayi onluyor, etiket
		// degeri okunabilir kiliyor (adi ayristirmak gerekmesin).
		bucketTags["le"] = le

		out = append(out, &pb.Metric{
			Name:            name + "_bucket_le_" + le,
			Type:            pb.MetricType_COUNTER,
			Value:           float64(cumulative),
			Tags:            bucketTags,
			TimestampMillis: ts,
		})
	}

	return out, rejected
}

// summaryPoints: Summary, kaynagin kendi hesapladigi yuzdelikleri tasir.
//
// Histogram'dan farki: yuzdelikler onceden hesaplanmis geliyor, yani
// dogrular ama BIRLESTIRILEMEZLER. Iki instance'in p95'inin ortalamasi
// bir p95 degildir. Bu yuzden yuzdelik degeri de metrik adina giriyor.
func summaryPoints(name string, dp *metricspb.SummaryDataPoint, resAttrs map[string]string) []*pb.Metric {
	if dp == nil || dp.GetTimeUnixNano() == 0 {
		return nil
	}
	ts := int64(dp.GetTimeUnixNano() / 1e6)
	tags := merge(Attributes(dp.GetAttributes()), resAttrs)

	out := []*pb.Metric{
		{Name: name + "_count", Type: pb.MetricType_COUNTER,
			Value: float64(dp.GetCount()), Tags: tags, TimestampMillis: ts},
		{Name: name + "_sum", Type: pb.MetricType_COUNTER,
			Value: dp.GetSum(), Tags: tags, TimestampMillis: ts},
	}

	for _, q := range dp.GetQuantileValues() {
		if math.IsNaN(q.GetValue()) {
			continue
		}
		label := formatBound(q.GetQuantile())
		qTags := make(map[string]string, len(tags)+1)
		for k, v := range tags {
			qTags[k] = v
		}
		qTags["quantile"] = label

		out = append(out, &pb.Metric{
			Name:            name + "_q" + label,
			Type:            pb.MetricType_SUMMARY,
			Value:           q.GetValue(),
			Tags:            qTags,
			TimestampMillis: ts,
		})
	}
	return out
}

// formatBound: kova sinirini metrik adinda kullanilabilir bir metne cevirir.
//
//	0.005 -> "0.005"      5 -> "5"      +Inf -> "inf"
//
// Nokta korunuyor cunku metrik adlarinda zaten nokta var
// (process.memory.heap.alloc); ondaligi silmek 0.5 ile 5'i ayni ada
// dusururdu.
func formatBound(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	// Bilimsel gosterimdeki '+' ve ada girmemesi gereken karakterleri temizle.
	s = strings.ReplaceAll(s, "+", "")
	return s
}
