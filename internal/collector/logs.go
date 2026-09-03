package collector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/nisah/pulse-metrics/pkg/logging"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// Log tarafi Kafka varsayilanlari.
const (
	DefaultLogsTopic   = "pulse-logs"
	DefaultLogsGroupID = "pulse-collector-logs"
)

// Sema aciklamalari icin scripts/init-scylla-phase3.cql dosyasina bak.
const (
	logsDDL = `
		CREATE TABLE IF NOT EXISTS pulse.logs (
			service_name TEXT,
			time_bucket  TEXT,
			timestamp_ms BIGINT,
			log_id       TEXT,
			level        TEXT,
			message      TEXT,
			trace_id     TEXT,
			span_id      TEXT,
			logger_name  TEXT,
			instance_id  TEXT,
			stack_trace  TEXT,
			attributes   MAP<TEXT, TEXT>,
			PRIMARY KEY ((service_name, time_bucket), timestamp_ms, log_id)
		) WITH CLUSTERING ORDER BY (timestamp_ms DESC, log_id ASC)
			AND default_time_to_live = 604800`

	traceLogsDDL = `
		CREATE TABLE IF NOT EXISTS pulse.trace_logs (
			trace_id     TEXT,
			timestamp_ms BIGINT,
			log_id       TEXT,
			service_name TEXT,
			span_id      TEXT,
			level        TEXT,
			message      TEXT,
			logger_name  TEXT,
			attributes   MAP<TEXT, TEXT>,
			PRIMARY KEY (trace_id, timestamp_ms, log_id)
		) WITH CLUSTERING ORDER BY (timestamp_ms ASC, log_id ASC)
			AND default_time_to_live = 604800`

	logServicesDDL = `
		CREATE TABLE IF NOT EXISTS pulse.log_services (
			service_name TEXT PRIMARY KEY,
			last_seen_ms BIGINT
		) WITH default_time_to_live = 604800`

	serviceEdgesDDL = `
		CREATE TABLE IF NOT EXISTS pulse.service_edges (
			caller_service  TEXT,
			callee_service  TEXT,
			time_bucket     TEXT,
			timestamp_ms    BIGINT,
			span_id         TEXT,
			duration_ms     BIGINT,
			has_error       BOOLEAN,
			PRIMARY KEY ((caller_service, callee_service, time_bucket), timestamp_ms, span_id)
		) WITH CLUSTERING ORDER BY (timestamp_ms DESC, span_id ASC)
			AND default_time_to_live = 604800`

	edgePairsDDL = `
		CREATE TABLE IF NOT EXISTS pulse.edge_pairs (
			time_bucket    TEXT,
			caller_service TEXT,
			callee_service TEXT,
			last_seen_ms   BIGINT,
			PRIMARY KEY (time_bucket, caller_service, callee_service)
		) WITH default_time_to_live = 604800`

	alertRulesDDL = `
		CREATE TABLE IF NOT EXISTS pulse.alert_rules (
			rule_id          TEXT PRIMARY KEY,
			name             TEXT,
			service_name     TEXT,
			metric_name      TEXT,
			condition        TEXT,
			duration_seconds INT,
			webhook_url      TEXT,
			enabled          BOOLEAN,
			severity         TEXT,
			created_at_ms    BIGINT
		)`

	alertsDDL = `
		CREATE TABLE IF NOT EXISTS pulse.alerts (
			service_name TEXT,
			time_bucket  TEXT,
			timestamp_ms BIGINT,
			alert_id     TEXT,
			rule_id      TEXT,
			rule_name    TEXT,
			metric_name  TEXT,
			condition    TEXT,
			metric_value DOUBLE,
			threshold    DOUBLE,
			severity     TEXT,
			message      TEXT,
			resolved     BOOLEAN,
			PRIMARY KEY ((service_name, time_bucket), timestamp_ms, alert_id)
		) WITH CLUSTERING ORDER BY (timestamp_ms DESC, alert_id ASC)
			AND default_time_to_live = 2592000`

	insertLog = `
		INSERT INTO logs (service_name, time_bucket, timestamp_ms, log_id, level, message,
		                  trace_id, span_id, logger_name, instance_id, stack_trace, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertTraceLog = `
		INSERT INTO trace_logs (trace_id, timestamp_ms, log_id, service_name, span_id,
		                        level, message, logger_name, attributes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertLogService = `INSERT INTO log_services (service_name, last_seen_ms) VALUES (?, ?)`

	insertServiceEdge = `
		INSERT INTO service_edges (caller_service, callee_service, time_bucket,
		                           timestamp_ms, span_id, duration_ms, has_error)
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	insertEdgePair = `
		INSERT INTO edge_pairs (time_bucket, caller_service, callee_service, last_seen_ms)
		VALUES (?, ?, ?, ?)`
)

// ensurePhase3Schema: log, kenar ve alarm tablolarini yaratir.
func ensurePhase3Schema(session *gocql.Session, logger *zap.Logger) error {
	for _, ddl := range []string{
		logsDDL, traceLogsDDL, logServicesDDL,
		serviceEdgesDDL, edgePairsDDL,
		alertRulesDDL, alertsDDL,
	} {
		if err := session.Query(ddl).Exec(); err != nil {
			return fmt.Errorf("faz 3 tablosu yaratilamadi: %w", err)
		}
	}
	logger.Info("Phase 3 schema ensured")
	return nil
}

// newID: kisa, benzersiz kimlik. Ayni milisaniyede gelen kayitlari ayirir.
func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// newLogReader: log topic'i icin Kafka okuyucusu.
func newLogReader(cfg *Config) *kafka.Reader {
	topic := cfg.LogsTopic
	if topic == "" {
		topic = DefaultLogsTopic
	}
	group := cfg.LogsGroupID
	if group == "" {
		group = DefaultLogsGroupID
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          topic,
		GroupID:        group,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
}

// consumeLogs: pulse-logs topic'inden okuyup ScyllaDB'ye yazar.
func (c *Collector) consumeLogs(ctx context.Context) error {
	if c.logReader == nil {
		<-ctx.Done()
		return nil
	}

	for {
		msg, err := c.logReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				c.logger.Info("Log consumer loop stopping")
				return nil
			}
			c.logger.Error("Failed to read log message", zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		var payload pb.LogsPayload
		if err := proto.Unmarshal(msg.Value, &payload); err != nil {
			c.logger.Error("Failed to unmarshal logs", zap.Error(err))
			continue
		}

		if err := c.storeLogs(ctx, &payload); err != nil {
			c.logger.Error("Failed to store logs",
				zap.Error(err), zap.String("service", payload.ServiceName))
		}
	}
}

// storeLogs: her log kaydini logs tablosuna, trace_id varsa ayrica
// trace_logs tablosuna yazar.
func (c *Collector) storeLogs(ctx context.Context, payload *pb.LogsPayload) error {
	if payload == nil || len(payload.Logs) == 0 {
		return nil
	}

	instanceID := payload.InstanceId
	if instanceID == "" {
		instanceID = "unknown"
	}

	stored := 0
	for _, rec := range payload.Logs {
		service := rec.ServiceName
		if service == "" {
			service = payload.ServiceName
		}
		if service == "" {
			continue
		}

		ts := rec.TimestampMs
		if ts == 0 {
			ts = time.Now().UnixMilli()
		}
		logID := newID()
		bucket := TimeBucket(time.UnixMilli(ts))
		level := logging.LevelName(rec.Level)

		if err := c.session.Query(insertLog,
			service, bucket, ts, logID, level, rec.Message,
			rec.TraceId, rec.SpanId, rec.LoggerName, instanceID,
			rec.StackTrace, rec.Attributes,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("log yazilamadi: %w", err)
		}

		// Trace korelasyonu: sadece trace baglami olan loglar icin.
		if rec.TraceId != "" {
			if err := c.session.Query(insertTraceLog,
				rec.TraceId, ts, logID, service, rec.SpanId,
				level, rec.Message, rec.LoggerName, rec.Attributes,
			).WithContext(ctx).Exec(); err != nil {
				return fmt.Errorf("trace_logs yazilamadi: %w", err)
			}
		}

		stored++
	}

	if stored > 0 {
		if err := c.session.Query(insertLogService,
			payload.ServiceName, time.Now().UnixMilli()).WithContext(ctx).Exec(); err != nil {
			c.logger.Warn("log_services yazilamadi", zap.Error(err))
		}
	}

	if c.config.Debug {
		c.logger.Debug("Logs stored",
			zap.String("service", payload.ServiceName),
			zap.Int("count", stored))
	}
	return nil
}

// storeServiceEdge: bir SERVER span'inden servis kenari cikarir.
//
// Faz 2'de topoloji sorgu aninda hesaplaniyordu. Artik burada, ingest
// sirasinda ve ek okuma yapmadan yaziliyor: cagiran servisin adi span'in
// peer.service ozniteliginde hazir duruyor (SDK onu tracestate ile tasidi).
func (c *Collector) storeServiceEdge(ctx context.Context, span *pb.Span, calleeService string) error {
	if span.Attributes == nil {
		return nil
	}
	caller := span.Attributes["peer.service"]
	if caller == "" || caller == calleeService {
		return nil // ic cagri ya da bilinmeyen cagiran
	}

	tsMs := span.StartTimeMicros / 1000
	bucket := TimeBucket(time.UnixMilli(tsMs))
	durationMs := (span.EndTimeMicros - span.StartTimeMicros) / 1000
	if durationMs < 0 {
		durationMs = 0
	}
	hasError := span.Status != nil && span.Status.Code == pb.StatusCode_STATUS_CODE_ERROR

	if err := c.session.Query(insertServiceEdge,
		caller, calleeService, bucket, tsMs, span.SpanId, durationMs, hasError,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("service_edges yazilamadi: %w", err)
	}

	if err := c.session.Query(insertEdgePair,
		bucket, caller, calleeService, time.Now().UnixMilli(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("edge_pairs yazilamadi: %w", err)
	}

	return nil
}
