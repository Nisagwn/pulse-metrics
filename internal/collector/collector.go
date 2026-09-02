package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	pb "github.com/nisah/pulse-metrics/internal/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Config: collector configuration
type Config struct {
	KafkaBrokers []string
	ScyllaAddr   string
	GRPCPort     string
	Debug        bool
}

// Collector: metrics collector server
type Collector struct {
	config   *Config
	logger   *zap.Logger
	session  *gocql.Session
	reader   *kafka.Reader
	mu       sync.Mutex
	stopped  bool
}

// NewCollector: create collector
func NewCollector(cfg *Config) (*Collector, error) {
	// Initialize logger
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

	// Initialize ScyllaDB session
	cluster := gocql.NewCluster(cfg.ScyllaAddr)
	cluster.Keyspace = "pulse"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ScyllaDB: %w", err)
	}

	// Initialize Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          "pulse-metrics",
		GroupID:        "pulse-collector",
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &Collector{
		config:  cfg,
		logger:  logger,
		session: session,
		reader:  reader,
	}, nil
}

// Start: begin consuming metrics
func (c *Collector) Start(ctx context.Context) error {
	c.logger.Info("Collector started", zap.String("scylla", c.config.ScyllaAddr))

	// Ensure keyspace exists
	if err := c.ensureKeyspace(); err != nil {
		c.logger.Error("Failed to ensure keyspace", zap.Error(err))
		return err
	}

	// Start consuming metrics
	for {
		select {
		case <-ctx.Done():
			return c.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				c.logger.Error("Failed to read message", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			// Parse protobuf
			var payload pb.MetricsPayload
			if err := proto.Unmarshal(msg.Value, &payload); err != nil {
				c.logger.Error("Failed to unmarshal metrics", zap.Error(err))
				continue
			}

			// Store metrics
			if err := c.storeMetrics(&payload); err != nil {
				c.logger.Error("Failed to store metrics",
					zap.Error(err),
					zap.String("service", payload.ServiceName),
				)
			}
		}
	}
}

// ensureKeyspace: create keyspace if not exists
func (c *Collector) ensureKeyspace() error {
	// Create keyspace
	if err := c.session.Query(`
		CREATE KEYSPACE IF NOT EXISTS pulse
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`).Exec(); err != nil {
		return fmt.Errorf("failed to create keyspace: %w", err)
	}

	// Switch to keyspace
	c.session.Query("USE pulse").Exec()

	// Create metrics table
	if err := c.session.Query(`
		CREATE TABLE IF NOT EXISTS metrics (
			service_name TEXT,
			metric_name TEXT,
			timestamp BIGINT,
			tags MAP<TEXT, TEXT>,
			value DOUBLE,
			PRIMARY KEY ((service_name, metric_name), timestamp)
		) WITH CLUSTERING ORDER BY (timestamp DESC)
			AND default_time_to_live = 2592000
	`).Exec(); err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	c.logger.Info("Keyspace and tables ensured")
	return nil
}

// storeMetrics: persist metrics to ScyllaDB
func (c *Collector) storeMetrics(payload *pb.MetricsPayload) error {
	if payload == nil || len(payload.Metrics) == 0 {
		return nil
	}

	for _, metric := range payload.Metrics {
		if err := c.session.Query(`
			INSERT INTO metrics (service_name, metric_name, timestamp, tags, value)
			VALUES (?, ?, ?, ?, ?)
		`,
			payload.ServiceName,
			metric.Name,
			metric.TimestampMillis,
			metric.Tags,
			metric.Value,
		).Exec(); err != nil {
			return fmt.Errorf("failed to insert metric: %w", err)
		}
	}

	if c.config.Debug {
		c.logger.Debug("Metrics stored",
			zap.String("service", payload.ServiceName),
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

	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			c.logger.Error("Failed to close Kafka reader", zap.Error(err))
		}
	}

	if c.session != nil {
		c.session.Close()
	}

	if err := c.logger.Sync(); err != nil {
		return err
	}

	c.logger.Info("Collector stopped gracefully")
	return nil
}
