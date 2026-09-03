package collector

import (
	"math"
	"testing"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

func points(vals ...float64) []*pb.DataPoint {
	out := make([]*pb.DataPoint, len(vals))
	for i, v := range vals {
		out[i] = &pb.DataPoint{TimestampMs: int64(1000 + i), Value: v}
	}
	return out
}

func TestAggregate(t *testing.T) {
	pts := points(10, 20, 30, 40, 50)

	cases := []struct {
		kind string
		want float64
	}{
		{"avg", 30},
		{"sum", 150},
		{"min", 10},
		{"max", 50},
		{"count", 5},
		{"last", 50},
		{"p50", 30}, // nearest-rank: ceil(0.50*5)=3 -> 3. eleman
		{"p95", 50}, // ceil(0.95*5)=5
		{"p99", 50},
	}

	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			got, err := aggregate(tc.kind, points(10, 20, 30, 40, 50))
			if err != nil {
				t.Fatalf("aggregate(%q) beklenmeyen hata: %v", tc.kind, err)
			}
			if got.Value != tc.want {
				t.Errorf("aggregate(%q) = %v, beklenen %v", tc.kind, got.Value, tc.want)
			}
			// Zaman damgasi her zaman en yeni noktanin damgasi olmali.
			if got.TimestampMs != pts[len(pts)-1].TimestampMs {
				t.Errorf("timestamp = %d, beklenen %d", got.TimestampMs, pts[len(pts)-1].TimestampMs)
			}
		})
	}
}

func TestAggregateBilinmeyenTur(t *testing.T) {
	if _, err := aggregate("medyan", points(1, 2, 3)); err == nil {
		t.Fatal("bilinmeyen aggregation icin hata bekleniyordu")
	}
}

func TestAggregateBosSeri(t *testing.T) {
	got, err := aggregate("avg", nil)
	if err != nil {
		t.Fatalf("bos seri hata vermemeli: %v", err)
	}
	if got.Value != 0 || got.TimestampMs != 0 {
		t.Errorf("bos seri icin sifir nokta bekleniyordu, alinan %+v", got)
	}
}

func TestPercentile(t *testing.T) {
	// Tek elemanli seri her yuzdelikte kendisini dondurmeli.
	if got := percentile([]float64{42}, 0.99); got != 42 {
		t.Errorf("tek eleman p99 = %v, beklenen 42", got)
	}

	// Siralanmamis girdi de dogru sonuc vermeli.
	got := percentile([]float64{50, 10, 40, 20, 30}, 0.50)
	if got != 30 {
		t.Errorf("p50 = %v, beklenen 30", got)
	}

	// Sinirlar tasmamali.
	vals := []float64{1, 2, 3, 4}
	if got := percentile(vals, 0); got != 1 {
		t.Errorf("q=0 icin %v, beklenen 1", got)
	}
	if got := percentile(vals, 1); got != 4 {
		t.Errorf("q=1 icin %v, beklenen 4", got)
	}
}

func TestAggregateNegatifDegerler(t *testing.T) {
	// min/max icin baslangic degerlerinin (+Inf/-Inf) dogru secildigini dogrular.
	got, err := aggregate("max", points(-50, -10, -30))
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != -10 {
		t.Errorf("max = %v, beklenen -10", got.Value)
	}

	got, err = aggregate("min", points(-50, -10, -30))
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != -50 {
		t.Errorf("min = %v, beklenen -50", got.Value)
	}
	if math.IsInf(got.Value, 0) {
		t.Error("sonuc sonsuz olmamali")
	}
}

func TestParseMetricType(t *testing.T) {
	cases := map[string]pb.MetricType{
		"GAUGE":     pb.MetricType_GAUGE,
		"gauge":     pb.MetricType_GAUGE,
		"COUNTER":   pb.MetricType_COUNTER,
		"HISTOGRAM": pb.MetricType_HISTOGRAM,
		"SUMMARY":   pb.MetricType_SUMMARY,
		"":          pb.MetricType_GAUGE, // bilinmeyen -> GAUGE
		"saçma":     pb.MetricType_GAUGE,
	}
	for in, want := range cases {
		if got := parseMetricType(in); got != want {
			t.Errorf("parseMetricType(%q) = %v, beklenen %v", in, got, want)
		}
	}
}
