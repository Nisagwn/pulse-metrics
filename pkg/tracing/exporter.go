package tracing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// TracesTopic: span'lerin yazildigi Kafka topic'i.
// Metriklerden ayri bir topic: hacimleri ve tuketicileri farkli.
const TracesTopic = "pulse-traces"

// BatchExporter: span'leri tamponlar ve toplu halde Kafka'ya yazar.
//
// Neden tampon? End() uygulamanin sicak yolunda cagriliyor. Her span icin
// ayri bir Kafka yazmasi yapmak, olcmeye calistigimiz gecikmeyi bizim
// eklememiz demek olurdu. Span'ler bellekte birikir, ayri bir goroutine
// arka planda gonderir.
type BatchExporter struct {
	producer  *kafka.Writer
	service   string
	instance  string
	batchSize int
	maxBuffer int
	interval  time.Duration

	onError func(error)

	mu       sync.Mutex
	buffer   []*pb.Span
	dropped  int
	failed   int
	exported int
	closed   bool

	flushCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
}

// BatchExporterConfig: exporter ayarlari.
type BatchExporterConfig struct {
	KafkaBrokers  []string
	Topic         string        // bos ise TracesTopic
	ServiceName   string        //
	InstanceID    string        //
	BatchSize     int           // bos ise 128
	FlushInterval time.Duration // bos ise 2s
	MaxBuffer     int           // bos ise BatchSize * 8
	// OnError: bir toplu gonderim kalici olarak basarisiz oldugunda cagrilir.
	// Bos birakilirsa hatalar sadece sayilir. Izleme sisteminin kendi
	// veri kaybini sessizce yutmasi kabul edilemez.
	OnError func(error)
}

// NewBatchExporter: exporter'i olusturur ve arka plan gondericisini baslatir.
func NewBatchExporter(cfg BatchExporterConfig) *BatchExporter {
	topic := cfg.Topic
	if topic == "" {
		topic = TracesTopic
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 128
	}
	interval := cfg.FlushInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	maxBuffer := cfg.MaxBuffer
	if maxBuffer <= 0 {
		maxBuffer = batchSize * 8
	}

	e := &BatchExporter{
		producer: &kafka.Writer{
			Addr:        kafka.TCP(cfg.KafkaBrokers...),
			Topic:       topic,
			Compression: kafka.Snappy,
			// Trace'ler metriklerden daha toleransli: tek bir span'in
			// kaybi olcumu bozmaz, o yuzden RequireOne yeterli ve daha hizli.
			RequiredAcks:    kafka.RequireOne,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: time.Second,
		},
		service:   cfg.ServiceName,
		instance:  cfg.InstanceID,
		batchSize: batchSize,
		interval:  interval,
		flushCh:   make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
		maxBuffer: maxBuffer,
		onError:   cfg.OnError,
	}

	e.wg.Add(1)
	go e.run()
	return e
}

// ExportSpans: span'leri tampona koyar. Engellemez.
func (e *BatchExporter) ExportSpans(_ context.Context, spans []*pb.Span) error {
	if len(spans) == 0 {
		return nil
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("exporter kapali")
	}

	// Tampon dolduysa yeni span'leri dusur. Alternatif olan "engelle",
	// Kafka yavasladiginda uygulamayi da yavaslatirdi - izleme sistemi
	// asla izledigi uygulamayi durdurmamali.
	for _, s := range spans {
		if len(e.buffer) >= e.maxBuffer {
			e.dropped++
			continue
		}
		e.buffer = append(e.buffer, s)
	}
	shouldFlush := len(e.buffer) >= e.batchSize
	e.mu.Unlock()

	if shouldFlush {
		select {
		case e.flushCh <- struct{}{}:
		default: // zaten bir flush sinyali bekliyor
		}
	}
	return nil
}

// Dropped: tampon tasmasi yuzunden dusurulen span sayisi.
func (e *BatchExporter) Dropped() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// Failed: Kafka'ya yazilamayan span sayisi.
func (e *BatchExporter) Failed() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failed
}

// Exported: basariyla gonderilen span sayisi.
func (e *BatchExporter) Exported() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.exported
}

func (e *BatchExporter) run() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.doneCh:
			// Son flush'i Shutdown yapar; boylece cagiranin deadline'i
			// gecerli olur. Burada flush etmek onu yok saymak olurdu.
			return
		case <-ticker.C:
			e.flush(context.Background())
		case <-e.flushCh:
			e.flush(context.Background())
		}
	}
}

func (e *BatchExporter) flush(ctx context.Context) {
	e.mu.Lock()
	if len(e.buffer) == 0 {
		e.mu.Unlock()
		return
	}
	batch := e.buffer
	e.buffer = nil
	e.mu.Unlock()

	payload := &pb.TracesPayload{
		ServiceName: e.service,
		InstanceId:  e.instance,
		Spans:       batch,
		Timestamp:   timestamppb.Now(),
	}

	data, err := proto.Marshal(payload)
	if err != nil {
		e.recordFailure(len(batch), fmt.Errorf("span batch'i serilestirilemedi: %w", err))
		return // bozuk payload'i yeniden denemek anlamsiz
	}

	// Key olarak trace_id degil service_name kullaniyoruz: ayni servisin
	// span'leri ayni partition'a gider, yani collector tarafinda sira korunur.
	msg := kafka.Message{Key: []byte(e.service), Value: data}

	// Yeniden deneme: yeni yaratilmis bir topic'te lider secimi henuz
	// bitmemis olabilir ve ilk yazma "Unknown Topic Or Partition" alir.
	// Bu gecici bir durum, kalici bir hata degil.
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(exportBackoff(attempt)):
			case <-ctx.Done():
				e.recordFailure(len(batch), lastErr)
				return
			}
		}

		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lastErr = e.producer.WriteMessages(writeCtx, msg)
		cancel()

		if lastErr == nil {
			e.mu.Lock()
			e.exported += len(batch)
			e.mu.Unlock()
			return
		}
	}

	e.recordFailure(len(batch), lastErr)
}

// recordFailure: kalici basarisizligi sayar ve varsa geri cagriyi tetikler.
func (e *BatchExporter) recordFailure(n int, err error) {
	e.mu.Lock()
	e.failed += n
	cb := e.onError
	e.mu.Unlock()

	if cb != nil && err != nil {
		cb(fmt.Errorf("%d span gonderilemedi: %w", n, err))
	}
}

// Shutdown: bekleyen span'leri gonderir ve producer'i kapatir.
func (e *BatchExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.doneCh)
	e.wg.Wait()

	// Bekleyen span'leri cagiranin context'iyle gonder: Shutdown'a
	// 5 saniyelik bir ctx verildiyse burada 5 saniyeden fazla beklenmez.
	e.flush(ctx)

	return e.producer.Close()
}

// --- Test ve gelistirme icin ------------------------------------------------

// MemoryExporter: span'leri bellekte tutar. Testlerde kullanilir.
type MemoryExporter struct {
	mu    sync.Mutex
	spans []*pb.Span
}

// NewMemoryExporter: bellekte tutan exporter.
func NewMemoryExporter() *MemoryExporter { return &MemoryExporter{} }

func (m *MemoryExporter) ExportSpans(_ context.Context, spans []*pb.Span) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, spans...)
	return nil
}

func (m *MemoryExporter) Shutdown(context.Context) error { return nil }

// Spans: toplanan span'lerin kopyasi.
func (m *MemoryExporter) Spans() []*pb.Span {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*pb.Span, len(m.spans))
	copy(out, m.spans)
	return out
}

// Reset: toplanani temizler.
func (m *MemoryExporter) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = nil
}

// exportBackoff: yeniden denemeler arasi ustel bekleme, 5 saniyede sinirli.
//
// Ilk surum sabit 3 deneme / toplam ~1.5 saniye kullaniyordu. Yeni yaratilmis
// bir topic'in tum broker'lara yayilmasi bundan uzun surebiliyor ve kayitlar
// sessizce dusuyordu. Arka planda tamponlayan bir gonderici acele etmemeli:
// veri zaten bellekte bekliyor, kaybetmektense beklemek dogru.
func exportBackoff(attempt int) time.Duration {
	if attempt < 1 {
		return 0
	}
	d := 500 * time.Millisecond << uint(attempt-1)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}
