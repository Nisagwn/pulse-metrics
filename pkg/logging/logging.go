// Package logging: PulseMetrics'in log toplama SDK'si.
//
// Sradan bir logger'dan tek farki ama en onemli farki: context'te aktif bir
// span varsa, log kaydina trace_id ve span_id otomatik eklenir.
//
// Bunun degeri suradan geliyor: bir kullanici "siparisim gecmedi" dediginde
// elinde trace_id varsa, o istegin BUTUN servislerde urettigi loglari tek
// sorguyla toplayabilirsin. Trace sana nerede yavasladigini, log neden
// basarisiz oldugunu soyler. Ucuncu ayak (metrik) ise bunun ne kadar sik
// oldugunu.
package logging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
	"github.com/nisah/pulse-metrics/pkg/tracing"
)

// LogsTopic: log kayitlarinin yazildigi Kafka topic'i.
const LogsTopic = "pulse-logs"

// Logger: yapilandirilmis log kayitlarini Kafka'ya gonderir.
type Logger struct {
	service  string
	instance string
	name     string
	minLevel pb.LogLevel

	producer *kafka.Writer
	onError  func(error)

	batchSize int
	maxBuffer int
	interval  time.Duration

	mu       sync.Mutex
	buffer   []*pb.LogRecord
	dropped  int
	failed   int
	exported int
	closed   bool

	flushCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup

	// echo: gelistirmede logu ayrica bir yere yazmak icin.
	echo func(*pb.LogRecord)
}

// Config: logger ayarlari.
type Config struct {
	KafkaBrokers  []string
	Topic         string // bos ise LogsTopic
	ServiceName   string
	InstanceID    string
	LoggerName    string
	MinLevel      pb.LogLevel // bu seviyenin altindakiler atilir
	BatchSize     int         // bos ise 64
	FlushInterval time.Duration
	MaxBuffer     int
	OnError       func(error)
	Echo          func(*pb.LogRecord) // ornegin stdout'a yazmak icin
}

// New: logger olusturur ve arka plan gondericisini baslatir.
func New(cfg Config) *Logger {
	topic := cfg.Topic
	if topic == "" {
		topic = LogsTopic
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 64
	}
	interval := cfg.FlushInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	maxBuffer := cfg.MaxBuffer
	if maxBuffer <= 0 {
		maxBuffer = batchSize * 16
	}
	minLevel := cfg.MinLevel
	if minLevel == pb.LogLevel_LEVEL_UNSPECIFIED {
		minLevel = pb.LogLevel_LEVEL_INFO
	}
	instance := cfg.InstanceID
	if instance == "" {
		instance = "default"
	}

	l := &Logger{
		service:  cfg.ServiceName,
		instance: instance,
		name:     cfg.LoggerName,
		minLevel: minLevel,
		producer: &kafka.Writer{
			Addr:        kafka.TCP(cfg.KafkaBrokers...),
			Topic:       topic,
			Compression: kafka.Snappy,
			// Loglar hacimli ve tek bir kaydin kaybi felaket degil:
			// RequireOne daha hizli ve yeterli.
			RequiredAcks:    kafka.RequireOne,
			WriteBackoffMin: 100 * time.Millisecond,
			WriteBackoffMax: time.Second,
		},
		onError:   cfg.OnError,
		batchSize: batchSize,
		maxBuffer: maxBuffer,
		interval:  interval,
		flushCh:   make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
		echo:      cfg.Echo,
	}

	l.wg.Add(1)
	go l.run()
	return l
}

// Debug/Info/Warn/Error: seviye kisayollari.
// ctx zorunlu cunku trace baglantisi oradan geliyor.
func (l *Logger) Debug(ctx context.Context, msg string, attrs map[string]string) {
	l.log(ctx, pb.LogLevel_LEVEL_DEBUG, msg, "", attrs)
}

func (l *Logger) Info(ctx context.Context, msg string, attrs map[string]string) {
	l.log(ctx, pb.LogLevel_LEVEL_INFO, msg, "", attrs)
}

func (l *Logger) Warn(ctx context.Context, msg string, attrs map[string]string) {
	l.log(ctx, pb.LogLevel_LEVEL_WARN, msg, "", attrs)
}

func (l *Logger) Error(ctx context.Context, msg string, err error, attrs map[string]string) {
	stack := ""
	if err != nil {
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["error"] = err.Error()
		stack = fmt.Sprintf("%+v", err)
	}
	l.log(ctx, pb.LogLevel_LEVEL_ERROR, msg, stack, attrs)
}

// log: kaydi olusturur ve tampona koyar.
func (l *Logger) log(ctx context.Context, level pb.LogLevel, msg, stack string, attrs map[string]string) {
	if level < l.minLevel {
		return
	}

	rec := &pb.LogRecord{
		TimestampMs: time.Now().UnixMilli(),
		Level:       level,
		Message:     msg,
		ServiceName: l.service,
		LoggerName:  l.name,
		StackTrace:  stack,
		Attributes:  attrs,
	}

	// Trace korelasyonu: bu paketin butun varlik sebebi.
	if sc := tracing.SpanContextFromContext(ctx); sc.IsValid() {
		rec.TraceId = sc.TraceID.String()
		rec.SpanId = sc.SpanID.String()
	}

	if l.echo != nil {
		l.echo(rec)
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	if len(l.buffer) >= l.maxBuffer {
		// Log firtinasi uygulamayi durdurmamali; fazlasini dusur.
		l.dropped++
		l.mu.Unlock()
		return
	}
	l.buffer = append(l.buffer, rec)
	shouldFlush := len(l.buffer) >= l.batchSize
	l.mu.Unlock()

	if shouldFlush {
		select {
		case l.flushCh <- struct{}{}:
		default:
		}
	}
}

func (l *Logger) run() {
	defer l.wg.Done()

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.doneCh:
			// Son flush'i Shutdown yapar; boylece cagiranin deadline'i
			// gecerli olur. Burada flush etmek onu yok saymak olurdu.
			return
		case <-ticker.C:
			l.flush(context.Background())
		case <-l.flushCh:
			l.flush(context.Background())
		}
	}
}

func (l *Logger) flush(ctx context.Context) {
	l.mu.Lock()
	if len(l.buffer) == 0 {
		l.mu.Unlock()
		return
	}
	batch := l.buffer
	l.buffer = nil
	l.mu.Unlock()

	payload := &pb.LogsPayload{
		ServiceName: l.service,
		InstanceId:  l.instance,
		Logs:        batch,
		Timestamp:   timestamppb.Now(),
	}

	data, err := proto.Marshal(payload)
	if err != nil {
		l.recordFailure(len(batch), err)
		return
	}

	msg := kafka.Message{Key: []byte(l.service), Value: data}

	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(exportBackoff(attempt)):
			case <-ctx.Done():
				l.recordFailure(len(batch), lastErr)
				return
			}
		}
		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lastErr = l.producer.WriteMessages(writeCtx, msg)
		cancel()
		if lastErr == nil {
			l.mu.Lock()
			l.exported += len(batch)
			l.mu.Unlock()
			return
		}
	}
	l.recordFailure(len(batch), lastErr)
}

func (l *Logger) recordFailure(n int, err error) {
	l.mu.Lock()
	l.failed += n
	cb := l.onError
	l.mu.Unlock()
	if cb != nil && err != nil {
		cb(fmt.Errorf("%d log gonderilemedi: %w", n, err))
	}
}

// Dropped/Failed/Exported: gozlemlenebilirlik sayaclari.
func (l *Logger) Dropped() int  { l.mu.Lock(); defer l.mu.Unlock(); return l.dropped }
func (l *Logger) Failed() int   { l.mu.Lock(); defer l.mu.Unlock(); return l.failed }
func (l *Logger) Exported() int { l.mu.Lock(); defer l.mu.Unlock(); return l.exported }

// Shutdown: bekleyen loglari gonderir ve kapatir.
func (l *Logger) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	close(l.doneCh)
	l.wg.Wait()

	// Bekleyen loglari cagiranin context'iyle gonder.
	l.flush(ctx)

	return l.producer.Close()
}

// ParseLevel: metin seviyeyi enum'a cevirir.
func ParseLevel(s string) pb.LogLevel {
	switch s {
	case "DEBUG", "debug", "LEVEL_DEBUG":
		return pb.LogLevel_LEVEL_DEBUG
	case "INFO", "info", "LEVEL_INFO":
		return pb.LogLevel_LEVEL_INFO
	case "WARN", "warn", "WARNING", "LEVEL_WARN":
		return pb.LogLevel_LEVEL_WARN
	case "ERROR", "error", "LEVEL_ERROR":
		return pb.LogLevel_LEVEL_ERROR
	case "FATAL", "fatal", "LEVEL_FATAL":
		return pb.LogLevel_LEVEL_FATAL
	default:
		return pb.LogLevel_LEVEL_UNSPECIFIED
	}
}

// LevelName: enum'u kisa metne cevirir (DEBUG, INFO, ...).
func LevelName(l pb.LogLevel) string {
	switch l {
	case pb.LogLevel_LEVEL_DEBUG:
		return "DEBUG"
	case pb.LogLevel_LEVEL_INFO:
		return "INFO"
	case pb.LogLevel_LEVEL_WARN:
		return "WARN"
	case pb.LogLevel_LEVEL_ERROR:
		return "ERROR"
	case pb.LogLevel_LEVEL_FATAL:
		return "FATAL"
	default:
		return "UNSPECIFIED"
	}
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
