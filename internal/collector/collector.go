package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/nisah/pulse-metrics/internal/health"
	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// Config: collector configuration
type Config struct {
	KafkaBrokers []string
	ScyllaAddr   string
	GRPCPort     string
	HealthAddr   string
	Topic        string // bos ise "pulse-metrics"
	GroupID      string // bos ise "pulse-collector"

	// Trace tarafi (Faz 2). Ayri topic ve ayri consumer group:
	// trace tuketimi yavaslarsa metrik tuketimi etkilenmemeli.
	TracesTopic   string // bos ise "pulse-traces"
	TracesGroupID string // bos ise "pulse-collector-traces"
	DisableTraces bool   // testlerde trace tuketimini kapatmak icin

	// Log ve alarm tarafi (Faz 3).
	LogsTopic     string // bos ise "pulse-logs"
	LogsGroupID   string // bos ise "pulse-collector-logs"
	DisableLogs   bool
	DisableAlerts bool
	AlertInterval time.Duration // bos ise 30s

	Debug bool
}

// Collector: metrics collector server
type Collector struct {
	config      *Config
	logger      *zap.Logger
	session     *gocql.Session
	reader      *kafka.Reader
	traceReader *kafka.Reader
	logReader   *kafka.Reader
	alerts      *AlertEngine
	started     time.Time

	mu      sync.Mutex
	stopped bool
}

// Kafka varsayilanlari. Testler kendi topic/group'lariyla izole calisabilsin
// diye Config uzerinden degistirilebilir.
const (
	DefaultTopic   = "pulse-metrics"
	DefaultGroupID = "pulse-collector"
)

const (
	keyspaceDDL = `
		CREATE KEYSPACE IF NOT EXISTS pulse
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`

	// Sema aciklamasi icin scripts/init-scylla.cql dosyasina bak.
	tableDDL = `
		CREATE TABLE IF NOT EXISTS pulse.metrics (
			service_name TEXT,
			metric_name  TEXT,
			timestamp    BIGINT,
			instance_id  TEXT,
			type         TEXT,
			tags         MAP<TEXT, TEXT>,
			labels       MAP<TEXT, TEXT>,
			value        DOUBLE,
			PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
		) WITH CLUSTERING ORDER BY (timestamp DESC, instance_id ASC)
			AND default_time_to_live = 2592000`

	insertMetric = `
		INSERT INTO metrics (service_name, metric_name, timestamp, instance_id, type, tags, labels, value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// ensureSchema: keyspace ve tabloyu, keyspace'e BAGLANMADAN once yaratir.
//
// Bu ayrim onemli. Onceki surum once cluster.Keyspace = "pulse" diyip
// oturum aciyor, sonra keyspace'i yaratmaya calisiyordu; bos bir ScyllaDB'de
// ilk acilis "keyspace does not exist" ile basarisiz olurdu. Cozum, DDL'i
// keyspace belirtmeyen ayri ve kisa omurlu bir oturumdan calistirmak.
func ensureSchema(addr string, logger *zap.Logger) error {
	cluster := gocql.NewCluster(addr)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	// Bilerek Keyspace atanmiyor: henuz var olmayabilir.

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("bootstrap oturumu acilamadi: %w", err)
	}
	defer session.Close()

	if err := session.Query(keyspaceDDL).Exec(); err != nil {
		return fmt.Errorf("keyspace yaratilamadi: %w", err)
	}
	if err := session.Query(tableDDL).Exec(); err != nil {
		return fmt.Errorf("metrics tablosu yaratilamadi: %w", err)
	}

	logger.Info("Schema ensured", zap.String("keyspace", "pulse"))
	return nil
}

// NewCollector: create collector
func NewCollector(cfg *Config) (*Collector, error) {
	var logger *zap.Logger
	var err error

	if cfg.Debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	// Once sema, sonra asil oturum.
	if err := ensureSchema(cfg.ScyllaAddr, logger); err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(cfg.ScyllaAddr)
	cluster.Keyspace = "pulse"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	// Trace tablolari (Faz 2). Keyspace artik var, bu yuzden asil
	// oturumdan yaratilabilirler.
	if err := ensureTraceSchema(session, logger); err != nil {
		session.Close()
		return nil, err
	}

	// Log, kenar ve alarm tablolari (Faz 3).
	if err := ensurePhase3Schema(session, logger); err != nil {
		session.Close()
		return nil, err
	}

	topic := cfg.Topic
	if topic == "" {
		topic = DefaultTopic
	}
	groupID := cfg.GroupID
	if groupID == "" {
		groupID = DefaultGroupID
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	var traceReader *kafka.Reader
	if !cfg.DisableTraces {
		traceReader = newTraceReader(cfg)
	}

	var logReader *kafka.Reader
	if !cfg.DisableLogs {
		logReader = newLogReader(cfg)
	}

	var engine *AlertEngine
	if !cfg.DisableAlerts {
		engine = NewAlertEngine(session, logger, cfg.AlertInterval)
	}

	return &Collector{
		config:      cfg,
		logger:      logger,
		session:     session,
		reader:      reader,
		traceReader: traceReader,
		logReader:   logReader,
		alerts:      engine,
		started:     time.Now(),
	}, nil
}

// Session: saglik kontrolu ve testler icin acik oturumu verir.
func (c *Collector) Session() *gocql.Session { return c.session }

// Logger: cagiranin ayni logger'i kullanabilmesi icin.
func (c *Collector) Logger() *zap.Logger { return c.logger }

// Ping: ScyllaDB hala erisilebilir mi? /readyz bunu kullanir.
func (c *Collector) Ping(ctx context.Context) error {
	return c.session.Query("SELECT release_version FROM system.local").
		WithContext(ctx).Exec()
}

// Start: uc is parcasini paralel calistirir ve ilki hata verdiginde hepsini durdurur:
//   - Kafka tuketici dongusu (asil is)
//   - gRPC sorgu sunucusu (dashboard-api buradan okur)
//   - HTTP saglik sunucusu
//
// ctx iptal edildiginde (Ctrl+C) hepsi duzgunce kapanir.
func (c *Collector) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.logger.Info("Collector started",
		zap.String("scylla", c.config.ScyllaAddr),
		zap.String("grpc", c.config.GRPCPort),
		zap.String("health", c.config.HealthAddr),
	)

	errCh := make(chan error, 6)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consume(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consumeTraces(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.consumeLogs(ctx)
	}()

	if c.alerts != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- c.alerts.Run(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.serveGRPC(ctx)
	}()

	hs := health.New(c.config.HealthAddr, c.logger)
	hs.AddCheck("scylladb", c.Ping)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- hs.Serve(ctx)
	}()

	// Ilk biten (hata veren ya da temiz duran) digerlerini de durdurur.
	err := <-errCh
	cancel()
	wg.Wait()

	if closeErr := c.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// serveGRPC: MetricsService'i dinlemeye alir. Log'un soyledigi seyi
// gercekten yapan kod burasi.
func (c *Collector) serveGRPC(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+c.config.GRPCPort)
	if err != nil {
		return fmt.Errorf("gRPC portu dinlenemedi: %w", err)
	}

	srv := grpc.NewServer()
	pb.RegisterMetricsServiceServer(srv, NewMetricsService(c.session, c.logger, c.started))
	pb.RegisterTraceServiceServer(srv, NewTraceService(c.session, c.logger))
	pb.RegisterLogServiceServer(srv, NewLogServiceServer(c.session, c.logger))
	pb.RegisterAlertServiceServer(srv, NewAlertServiceServer(c.session, c.logger, c.alerts))

	go func() {
		<-ctx.Done()
		// GracefulStop bekleyen RPC'lerin bitmesini bekler.
		srv.GracefulStop()
	}()

	c.logger.Info("gRPC server listening", zap.String("port", c.config.GRPCPort))
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("gRPC sunucusu durdu: %w", err)
	}
	return nil
}

// consume: Kafka'dan okuyup ScyllaDB'ye yazan asil dongu.
func (c *Collector) consume(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			// ctx iptal edildiyse bu bir hata degil, kapanma sinyalidir.
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				c.logger.Info("Consumer loop stopping")
				return nil
			}
			c.logger.Error("Failed to read message", zap.Error(err))

			// Yeniden denemeden once bekle, ama beklerken de iptali dinle.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}
			continue
		}

		var payload pb.MetricsPayload
		if err := proto.Unmarshal(msg.Value, &payload); err != nil {
			// Bozuk mesaj kuyrugu tikamamali: logla ve devam et.
			c.logger.Error("Failed to unmarshal metrics", zap.Error(err))
			continue
		}

		if err := c.storeMetrics(ctx, &payload); err != nil {
			c.logger.Error("Failed to store metrics",
				zap.Error(err),
				zap.String("service", payload.ServiceName),
			)
		}
	}
}

// storeMetrics: persist metrics to ScyllaDB
//
// Her metrik ayri bir INSERT. Bilerek BATCH kullanmiyoruz: bir payload'daki
// metriklerin metric_name'leri farkli, yani farkli partition'lara dusuyorlar.
// Cassandra/Scylla'da coklu partition BATCH'i hizlandirmaz, aksine
// koordinator dugumu uzerinde darbogaz yaratir.
func (c *Collector) storeMetrics(ctx context.Context, payload *pb.MetricsPayload) error {
	if payload == nil || len(payload.Metrics) == 0 {
		return nil
	}

	instanceID := payload.InstanceId
	if instanceID == "" {
		// Bos instance_id clustering key'i ise yaramaz hale getirir;
		// en azindan ayirt edilebilir bir deger koy.
		instanceID = "unknown"
	}

	for _, metric := range payload.Metrics {
		if metric.Name == "" {
			c.logger.Warn("Skipping metric with empty name",
				zap.String("service", payload.ServiceName))
			continue
		}

		ts := metric.TimestampMillis
		if ts == 0 && payload.Timestamp != nil {
			ts = payload.Timestamp.AsTime().UnixMilli()
		}

		if err := c.session.Query(insertMetric,
			payload.ServiceName,
			metric.Name,
			ts,
			instanceID,
			metric.Type.String(), // GAUGE / COUNTER / HISTOGRAM / SUMMARY
			metric.Tags,
			metric.Labels,
			metric.Value,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("failed to insert metric %q: %w", metric.Name, err)
		}
	}

	if c.config.Debug {
		c.logger.Debug("Metrics stored",
			zap.String("service", payload.ServiceName),
			zap.String("instance", instanceID),
			zap.Int("count", len(payload.Metrics)),
		)
	}

	return nil
}

// Close: gracefully shutdown collector
func (c *Collector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return nil
	}
	c.stopped = true

	// Uc okuyucuyu PARALEL kapatiyoruz. Her biri consumer group'tan
	// ayrilirken saniyeler surebiliyor; sirayla kapatmak kapanis suresini
	// gereksiz yere ucler.
	readers := map[string]*kafka.Reader{
		"metrics": c.reader,
		"traces":  c.traceReader,
		"logs":    c.logReader,
	}

	var (
		closeWg  sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for name, rd := range readers {
		if rd == nil {
			continue
		}
		closeWg.Add(1)
		go func(name string, rd *kafka.Reader) {
			defer closeWg.Done()
			if err := rd.Close(); err != nil {
				c.logger.Error("Kafka okuyucusu kapatilamadi",
					zap.String("reader", name), zap.Error(err))
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(name, rd)
	}
	closeWg.Wait()

	if c.session != nil {
		c.session.Close()
	}

	c.logger.Info("Collector stopped gracefully")

	// Sync en sona: bundan sonra yazilan loglar diske ulasmayabilir.
	// Windows'ta stderr'e Sync ENOTTY dondurur, bu bir hata degil.
	_ = c.logger.Sync()

	return firstErr
}
