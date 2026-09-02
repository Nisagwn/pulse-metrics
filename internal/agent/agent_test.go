package agent

import (
	"sync"
	"testing"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

func TestMetricsCollectorBosBaslar(t *testing.T) {
	mc := NewMetricsCollector()
	if got := mc.Collect(); len(got) != 0 {
		t.Errorf("yeni collector bos olmali, %d metrik dondu", len(got))
	}
}

func TestRecordVeCollect(t *testing.T) {
	mc := NewMetricsCollector()
	mc.RecordMetric("request.latency", 150.5)
	mc.RecordMetric("queue.depth", 12)

	got := mc.Collect()
	if len(got) != 2 {
		t.Fatalf("2 metrik bekleniyordu, %d alindi", len(got))
	}

	byName := map[string]*pb.Metric{}
	for _, m := range got {
		byName[m.Name] = m
	}

	if m := byName["request.latency"]; m == nil || m.Value != 150.5 {
		t.Errorf("request.latency yanlis: %+v", m)
	}
	if m := byName["queue.depth"]; m == nil || m.Value != 12 {
		t.Errorf("queue.depth yanlis: %+v", m)
	}

	for _, m := range got {
		if m.Type != pb.MetricType_GAUGE {
			t.Errorf("%s tipi %v, GAUGE bekleniyordu", m.Name, m.Type)
		}
		if m.TimestampMillis == 0 {
			t.Errorf("%s icin timestamp atanmamis", m.Name)
		}
	}
}

func TestRecordMetricUzerineYazar(t *testing.T) {
	mc := NewMetricsCollector()
	mc.RecordMetric("cpu", 10)
	mc.RecordMetric("cpu", 90)

	got := mc.Collect()
	if len(got) != 1 {
		t.Fatalf("ayni ad tek girdi olmali, %d alindi", len(got))
	}
	if got[0].Value != 90 {
		t.Errorf("deger %v, son yazilan 90 bekleniyordu", got[0].Value)
	}
}

// TestEszamanliErisim: -race ile calistirildiginda mutex'in isini yaptigini
// dogrular. Agent'in olcum dongusu ile kullanicinin RecordMetric cagrilari
// farkli goroutine'lerden gelir, bu yuzden onemli.
func TestEszamanliErisim(t *testing.T) {
	mc := NewMetricsCollector()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			mc.RecordMetric("metric", float64(n))
		}(i)
		go func() {
			defer wg.Done()
			_ = mc.Collect()
		}()
	}
	wg.Wait()

	if got := mc.Collect(); len(got) != 1 {
		t.Errorf("1 metrik bekleniyordu, %d alindi", len(got))
	}
}
