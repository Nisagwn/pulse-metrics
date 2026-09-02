package agent

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	pb "github.com/nisah/pulse-metrics/internal/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config: agent configuration
type Config struct {
	ServiceName     string
	InstanceID      string
	KafkaBrokers    []string
	CollectInterval time.Duration
	Debug           bool
}

// Agent: main APM agent
type Agent struct {
	config   *Config
	logger   *zap.Logger
	producer *kafka.Writer
	metrics  *MetricsCollector
	mu       sync.Mutex
	stopped  bool
}

// NewAgent: create new agent instance
func NewAgent(cfg *Config) (*Agent, error) {
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

	// Initialize Kafka producer
	producer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        "pulse-metrics",
		Compression:  kafka.Snappy,
		WriteBackoffMin: 100 * time.Millisecond,
		WriteBackoffMax: 1 * time.Second,
		RequiredAcks:    kafka.RequireAll, // Wait for all replicas
	}

	// Initialize metrics collector
	mc := NewMetricsCollector()

	return &Agent{
		config:   cfg,
		logger:   logger,
		producer: producer,
		metrics:  mc,
	}, nil
}

// Start: begin collecting and sending metrics
func (a *Agent) Start(ctx context.Context) error {
	a.logger.Info("Agent started",
		zap.String("service", a.config.ServiceName),
		zap.String("instance", a.config.InstanceID),
		zap.Duration("interval", a.config.CollectInterval),
	)

	ticker := time.NewTicker(a.config.CollectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return a.Close()
		case <-ticker.C:
			if err := a.collectAndSend(ctx); err != nil {
				a.logger.Error("Failed to collect/send metrics", zap.Error(err))
			}
		}
	}
}

// collectAndSend: gather metrics and publish to Kafka
func (a *Agent) collectAndSend(ctx context.Context) error {
	// Collect system metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := []*pb.Metric{
		// Memory metrics
		{
			Name:            "process.memory.heap.alloc",
			Type:            pb.MetricType_GAUGE,
			Value:           float64(m.HeapAlloc),
			TimestampMillis: time.Now().UnixMilli(),
			Tags: map[string]string{
				"unit": "bytes",
			},
		},
		{
			Name:            "process.memory.heap.total",
			Type:            pb.MetricType_GAUGE,
			Value:           float64(m.HeapSys),
			TimestampMillis: time.Now().UnixMilli(),
			Tags: map[string]string{
				"unit": "bytes",
			},
		},
		{
			Name:            "process.memory.gc.runs",
			Type:            pb.MetricType_COUNTER,
			Value:           float64(m.NumGC),
			TimestampMillis: time.Now().UnixMilli(),
		},
		// Goroutine metrics
		{
			Name:            "process.runtime.goroutines",
			Type:            pb.MetricType_GAUGE,
			Value:           float64(runtime.NumGoroutine()),
			TimestampMillis: time.Now().UnixMilli(),
		},
	}

	// Add custom metrics from collector
	customMetrics := a.metrics.Collect()
	metrics = append(metrics, customMetrics...)

	// Create payload
	payload := &pb.MetricsPayload{
		ServiceName: a.config.ServiceName,
		InstanceId:  a.config.InstanceID,
		Metrics:     metrics,
		Timestamp:   timestamppb.Now(),
	}

	// Serialize to protobuf
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	// Send to Kafka
	msg := kafka.Message{
		Key:   []byte(a.config.ServiceName),
		Value: data,
	}

	if err := a.producer.WriteMessages(ctx, msg); err != nil {
		a.logger.Error("Failed to write to Kafka",
			zap.Error(err),
			zap.Int("metricCount", len(metrics)),
		)
		return err
	}

	if a.config.Debug {
		a.logger.Debug("Metrics published",
			zap.Int("count", len(metrics)),
			zap.Int("payload_size", len(data)),
		)
	}

	return nil
}

// Close: gracefully shutdown agent
func (a *Agent) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stopped {
		return nil
	}

	a.stopped = true

	if err := a.producer.Close(); err != nil {
		a.logger.Error("Failed to close Kafka producer", zap.Error(err))
		return err
	}

	if err := a.logger.Sync(); err != nil {
		return err
	}

	a.logger.Info("Agent stopped gracefully")
	return nil
}

// MetricsCollector: custom metrics collection
type MetricsCollector struct {
	customMetrics map[string]float64
	mu            sync.RWMutex
}

// NewMetricsCollector: create collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		customMetrics: make(map[string]float64),
	}
}

// RecordMetric: record custom metric
func (mc *MetricsCollector) RecordMetric(name string, value float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.customMetrics[name] = value
}

// Collect: get all custom metrics
func (mc *MetricsCollector) Collect() []*pb.Metric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var metrics []*pb.Metric
	for name, value := range mc.customMetrics {
		metrics = append(metrics, &pb.Metric{
			Name:            name,
			Type:            pb.MetricType_GAUGE,
			Value:           value,
			TimestampMillis: time.Now().UnixMilli(),
		})
	}
	return metrics
}
