package logging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/nisah/pulse-metrics/internal/proto"
	"github.com/nisah/pulse-metrics/internal/tracing"
)

// collect: Echo geri cagrisiyla kayitlari toplayan yardimci.
// Kafka'ya cikmadan logger'in davranisini test etmeyi saglar.
type collect struct {
	mu   sync.Mutex
	recs []*pb.LogRecord
}

func (c *collect) fn(r *pb.LogRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r)
}

func (c *collect) all() []*pb.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*pb.LogRecord, len(c.recs))
	copy(out, c.recs)
	return out
}

func newTestLogger(t *testing.T, minLevel pb.LogLevel) (*Logger, *collect) {
	t.Helper()
	c := &collect{}
	l := New(Config{
		KafkaBrokers:  []string{"127.0.0.1:1"}, // baglanmayacak, Echo kullaniyoruz
		ServiceName:   "test-svc",
		InstanceID:    "test-1",
		LoggerName:    "test",
		MinLevel:      minLevel,
		BatchSize:     1000,      // boyutla flush tetiklenmesin
		FlushInterval: time.Hour, // zamanla da tetiklenmesin: bu testlerde
		// Kafka'ya hic cikilmiyor, Echo yeterli
		MaxBuffer: 10,
		Echo:      c.fn,
	})
	t.Cleanup(func() {
		// Shutdown Kafka'ya yazmayi deneyip basarisiz olacak; sorun degil.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = l.Shutdown(ctx)
	})
	return l, c
}

func TestSeviyeFiltresi(t *testing.T) {
	l, c := newTestLogger(t, pb.LogLevel_LEVEL_WARN)
	ctx := context.Background()

	l.Debug(ctx, "debug", nil)
	l.Info(ctx, "info", nil)
	l.Warn(ctx, "warn", nil)
	l.Error(ctx, "error", nil, nil)

	recs := c.all()
	if len(recs) != 2 {
		t.Fatalf("WARN ve uzeri (2 kayit) bekleniyordu, %d alindi", len(recs))
	}
	if recs[0].Message != "warn" || recs[1].Message != "error" {
		t.Errorf("yanlis kayitlar gecti: %v", recs)
	}
}

// Bu paketin varlik sebebi: context'te span varsa trace_id otomatik eklenir.
func TestTraceKorelasyonu(t *testing.T) {
	l, c := newTestLogger(t, pb.LogLevel_LEVEL_INFO)

	exp := tracing.NewMemoryExporter()
	tracer := tracing.NewTracer("test-svc", exp)
	ctx, span := tracer.Start(context.Background(), "islem")
	defer span.End()

	l.Info(ctx, "span icinde", nil)

	recs := c.all()
	if len(recs) != 1 {
		t.Fatalf("1 kayit bekleniyordu, %d", len(recs))
	}
	sc := span.SpanContext()
	if recs[0].TraceId != sc.TraceID.String() {
		t.Errorf("trace_id = %q, beklenen %q", recs[0].TraceId, sc.TraceID.String())
	}
	if recs[0].SpanId != sc.SpanID.String() {
		t.Errorf("span_id = %q, beklenen %q", recs[0].SpanId, sc.SpanID.String())
	}
}

func TestSpanYokkaTraceIdBos(t *testing.T) {
	l, c := newTestLogger(t, pb.LogLevel_LEVEL_INFO)

	l.Info(context.Background(), "span disinda", nil)

	recs := c.all()
	if len(recs) != 1 {
		t.Fatalf("1 kayit bekleniyordu, %d", len(recs))
	}
	if recs[0].TraceId != "" {
		t.Errorf("trace_id bos olmaliydi, %q bulundu", recs[0].TraceId)
	}
}

func TestErrorOzniteligeYazilir(t *testing.T) {
	l, c := newTestLogger(t, pb.LogLevel_LEVEL_INFO)

	l.Error(context.Background(), "islem basarisiz", errors.New("baglanti reddedildi"), nil)

	recs := c.all()
	if len(recs) != 1 {
		t.Fatalf("1 kayit bekleniyordu, %d", len(recs))
	}
	if recs[0].Attributes["error"] != "baglanti reddedildi" {
		t.Errorf("error ozniteligi = %q", recs[0].Attributes["error"])
	}
	if recs[0].StackTrace == "" {
		t.Error("stack_trace doldurulmaliydi")
	}
	if recs[0].Level != pb.LogLevel_LEVEL_ERROR {
		t.Errorf("seviye = %v, ERROR bekleniyordu", recs[0].Level)
	}
}

func TestServisVeLoggerAdiDoldurulur(t *testing.T) {
	l, c := newTestLogger(t, pb.LogLevel_LEVEL_INFO)
	l.Info(context.Background(), "mesaj", nil)

	r := c.all()[0]
	if r.ServiceName != "test-svc" {
		t.Errorf("service_name = %q", r.ServiceName)
	}
	if r.LoggerName != "test" {
		t.Errorf("logger_name = %q", r.LoggerName)
	}
	if r.TimestampMs == 0 {
		t.Error("timestamp atanmamis")
	}
}

// Tampon dolunca yeni kayitlar dusurulmeli - bir log firtinasi
// uygulamayi durdurmamali.
func TestTamponTasmasiKayitDusurur(t *testing.T) {
	l, _ := newTestLogger(t, pb.LogLevel_LEVEL_INFO)
	// MaxBuffer 10; flush tetiklenmesin diye BatchSize 1000.
	for i := 0; i < 50; i++ {
		l.Info(context.Background(), "spam", nil)
	}
	if l.Dropped() == 0 {
		t.Error("tampon tasmasinda kayit dusurulmeliydi")
	}
}

func TestEszamanliKullanim(t *testing.T) {
	l, _ := newTestLogger(t, pb.LogLevel_LEVEL_INFO)

	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); l.Info(context.Background(), "eszamanli", nil) }()
		go func() { defer wg.Done(); _ = l.Dropped() }()
	}
	wg.Wait()
}

func TestParseVeLevelName(t *testing.T) {
	levels := []pb.LogLevel{
		pb.LogLevel_LEVEL_DEBUG, pb.LogLevel_LEVEL_INFO, pb.LogLevel_LEVEL_WARN,
		pb.LogLevel_LEVEL_ERROR, pb.LogLevel_LEVEL_FATAL,
	}
	for _, l := range levels {
		if got := ParseLevel(LevelName(l)); got != l {
			t.Errorf("gidis-donus bozuk: %v -> %q -> %v", l, LevelName(l), got)
		}
	}
	if ParseLevel("saçma") != pb.LogLevel_LEVEL_UNSPECIFIED {
		t.Error("bilinmeyen seviye UNSPECIFIED dondurmeli")
	}
}
