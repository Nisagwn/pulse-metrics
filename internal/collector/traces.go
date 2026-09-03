package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// Trace tarafi Kafka varsayilanlari.
const (
	DefaultTracesTopic   = "pulse-traces"
	DefaultTracesGroupID = "pulse-collector-traces"
)

// Sema aciklamalari icin scripts/init-scylla-traces.cql dosyasina bak.
const (
	spansDDL = `
		CREATE TABLE IF NOT EXISTS pulse.spans (
			trace_id          TEXT,
			start_time_micros BIGINT,
			span_id           TEXT,
			parent_span_id    TEXT,
			service_name      TEXT,
			operation_name    TEXT,
			end_time_micros   BIGINT,
			duration_micros   BIGINT,
			kind              TEXT,
			status_code       TEXT,
			status_message    TEXT,
			attributes        MAP<TEXT, TEXT>,
			events            TEXT,
			instance_id       TEXT,
			PRIMARY KEY (trace_id, start_time_micros, span_id)
		) WITH CLUSTERING ORDER BY (start_time_micros ASC, span_id ASC)
			AND default_time_to_live = 604800`

	traceIndexDDL = `
		CREATE TABLE IF NOT EXISTS pulse.trace_index (
			service_name      TEXT,
			time_bucket       TEXT,
			start_time_micros BIGINT,
			trace_id          TEXT,
			span_id           TEXT,
			operation_name    TEXT,
			duration_micros   BIGINT,
			has_error         BOOLEAN,
			root              BOOLEAN,
			PRIMARY KEY ((service_name, time_bucket), start_time_micros, trace_id, span_id)
		) WITH CLUSTERING ORDER BY (start_time_micros DESC, trace_id ASC, span_id ASC)
			AND default_time_to_live = 604800`

	serviceOpsDDL = `
		CREATE TABLE IF NOT EXISTS pulse.service_ops (
			service_name   TEXT,
			operation_name TEXT,
			last_seen_ms   BIGINT,
			PRIMARY KEY (service_name, operation_name)
		) WITH default_time_to_live = 604800`

	insertSpan = `
		INSERT INTO spans (trace_id, start_time_micros, span_id, parent_span_id,
		                   service_name, operation_name, end_time_micros, duration_micros,
		                   kind, status_code, status_message, attributes, events, instance_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertTraceIndex = `
		INSERT INTO trace_index (service_name, time_bucket, start_time_micros,
		                         trace_id, span_id, operation_name, duration_micros,
		                         has_error, root)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertServiceOp = `
		INSERT INTO service_ops (service_name, operation_name, last_seen_ms)
		VALUES (?, ?, ?)`
)

// TimeBucket: bir zaman damgasini saatlik partition kovasina cevirir.
//
// Kova, trace_index partition'larinin sinirsiz buyumesini engeller.
// Format 'YYYYMMDDHH' ve UTC: yerel saat kullanmak, yaz saati
// gecislerinde ayni kovanin iki kez olusmasina yol acardi.
func TimeBucket(t time.Time) string {
	return t.UTC().Format("2006010215")
}

// Kova tarama sinirlari. Bir sorgunun kac partition okuyabilecegini
// yukaridan baglar; yoksa hatali bir zaman araligi butun kumeyi tarar.
// Siniri veri TTL'ine gore secmek dogru olan: TTL'den eskisini aramanin
// anlami yok.
const (
	traceBucketLimit  = 24 * 8  // trace/log TTL'i 7 gun
	metricBucketLimit = 24 * 31 // metrik TTL'i 30 gun
)

// bucketsInRange: bir zaman araligini kapsayan tum kovalar (en yeniden eskiye).
func bucketsInRange(startMs, endMs int64) []string {
	return bucketsInRangeMax(startMs, endMs, traceBucketLimit)
}

// bucketsInRangeMax: ust sinir verilebilen surum.
func bucketsInRangeMax(startMs, endMs int64, maxBuckets int) []string {
	start := time.UnixMilli(startMs).UTC().Truncate(time.Hour)
	end := time.UnixMilli(endMs).UTC()

	var out []string
	// En yeni kovadan basla: sorgular genelde "son N sonucu" istiyor,
	// yani erken durabilmek icin yeni kovalar once gelmeli.
	for t := end.Truncate(time.Hour); !t.Before(start); t = t.Add(-time.Hour) {
		out = append(out, TimeBucket(t))
		if len(out) >= maxBuckets {
			break
		}
	}
	return out
}

// ensureTraceSchema: trace tablolarini yaratir.
func ensureTraceSchema(session *gocql.Session, logger *zap.Logger) error {
	for _, ddl := range []string{spansDDL, traceIndexDDL, serviceOpsDDL} {
		if err := session.Query(ddl).Exec(); err != nil {
			return fmt.Errorf("trace tablosu yaratilamadi: %w", err)
		}
	}
	logger.Info("Trace schema ensured")
	return nil
}

// consumeTraces: pulse-traces topic'inden span okuyup ScyllaDB'ye yazar.
//
// Metrik dongusuyle ayni desen ama ayri bir consumer group: trace tuketimi
// yavaslarsa metrik tuketimi etkilenmemeli.
func (c *Collector) consumeTraces(ctx context.Context) error {
	if c.traceReader == nil {
		<-ctx.Done()
		return nil
	}

	for {
		msg, err := c.traceReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				c.logger.Info("Trace consumer loop stopping")
				return nil
			}
			c.logger.Error("Failed to read trace message", zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		var payload pb.TracesPayload
		if err := proto.Unmarshal(msg.Value, &payload); err != nil {
			c.logger.Error("Failed to unmarshal traces", zap.Error(err))
			continue
		}

		if err := c.storeSpans(ctx, &payload); err != nil {
			c.logger.Error("Failed to store spans",
				zap.Error(err),
				zap.String("service", payload.ServiceName),
			)
		}
	}
}

// storeSpans: her span'i iki tabloya birden yazar.
//
// Ayni veriyi iki kez yazmak (denormalizasyon) Cassandra ailesinde
// bir kusur degil, tasarimin kendisidir: disk ucuz, tarama pahali.
func (c *Collector) storeSpans(ctx context.Context, payload *pb.TracesPayload) error {
	if payload == nil || len(payload.Spans) == 0 {
		return nil
	}

	instanceID := payload.InstanceId
	if instanceID == "" {
		instanceID = "unknown"
	}

	stored := 0
	for _, span := range payload.Spans {
		if span.TraceId == "" || span.SpanId == "" {
			c.logger.Warn("Skipping span with empty ids",
				zap.String("service", payload.ServiceName),
				zap.String("operation", span.OperationName))
			continue
		}

		serviceName := span.ServiceName
		if serviceName == "" {
			serviceName = payload.ServiceName
		}

		duration := span.EndTimeMicros - span.StartTimeMicros
		if duration < 0 {
			duration = 0
		}

		hasError := span.Status != nil &&
			span.Status.Code == pb.StatusCode_STATUS_CODE_ERROR

		// Olaylar seyrek ve serbest bicimli; JSON olarak tek kolonda
		// tutmak, her olay icin ayri tablo yazmaktan cok daha ucuz.
		eventsJSON := ""
		if len(span.Events) > 0 {
			if b, err := json.Marshal(toEventDTOs(span.Events)); err == nil {
				eventsJSON = string(b)
			}
		}

		statusCode, statusMsg := "STATUS_CODE_UNSET", ""
		if span.Status != nil {
			statusCode = span.Status.Code.String()
			statusMsg = span.Status.Description
		}

		if err := c.session.Query(insertSpan,
			span.TraceId, span.StartTimeMicros, span.SpanId, span.ParentSpanId,
			serviceName, span.OperationName, span.EndTimeMicros, duration,
			span.Kind.String(), statusCode, statusMsg,
			span.Attributes, eventsJSON, instanceID,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("span yazilamadi (%s): %w", span.SpanId, err)
		}

		bucket := TimeBucket(time.UnixMicro(span.StartTimeMicros))
		if err := c.session.Query(insertTraceIndex,
			serviceName, bucket, span.StartTimeMicros,
			span.TraceId, span.SpanId, span.OperationName, duration,
			hasError, span.ParentSpanId == "",
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("trace_index yazilamadi (%s): %w", span.SpanId, err)
		}

		// Servis topolojisi kenari: sadece SERVER span'lerinde anlamli,
		// cunku cagiran bilgisi (peer.service) orada bulunuyor.
		if span.Kind == pb.SpanKind_SPAN_KIND_SERVER {
			if err := c.storeServiceEdge(ctx, span, serviceName); err != nil {
				c.logger.Warn("servis kenari yazilamadi", zap.Error(err))
			}
		}

		if err := c.session.Query(insertServiceOp,
			serviceName, span.OperationName, time.Now().UnixMilli(),
		).WithContext(ctx).Exec(); err != nil {
			// Bu sadece arama formunu doldurmak icin; basarisiz olmasi
			// span'in kaydedilmesini gecersiz kilmamali.
			c.logger.Warn("service_ops yazilamadi", zap.Error(err))
		}

		stored++
	}

	if c.config.Debug {
		c.logger.Debug("Spans stored",
			zap.String("service", payload.ServiceName),
			zap.String("instance", instanceID),
			zap.Int("count", stored),
		)
	}
	return nil
}

// eventDTO: olaylarin JSON temsili.
type eventDTO struct {
	Name            string            `json:"name"`
	TimestampMicros int64             `json:"ts"`
	Attributes      map[string]string `json:"attrs,omitempty"`
}

func toEventDTOs(events []*pb.Event) []eventDTO {
	out := make([]eventDTO, 0, len(events))
	for _, e := range events {
		out = append(out, eventDTO{
			Name:            e.Name,
			TimestampMicros: e.TimestampMicros,
			Attributes:      e.Attributes,
		})
	}
	return out
}

func fromEventsJSON(s string) []*pb.Event {
	if s == "" {
		return nil
	}
	var dtos []eventDTO
	if err := json.Unmarshal([]byte(s), &dtos); err != nil {
		return nil
	}
	out := make([]*pb.Event, 0, len(dtos))
	for _, d := range dtos {
		out = append(out, &pb.Event{
			Name:            d.Name,
			TimestampMicros: d.TimestampMicros,
			Attributes:      d.Attributes,
		})
	}
	return out
}

// newTraceReader: trace topic'i icin Kafka okuyucusu.
func newTraceReader(cfg *Config) *kafka.Reader {
	topic := cfg.TracesTopic
	if topic == "" {
		topic = DefaultTracesTopic
	}
	group := cfg.TracesGroupID
	if group == "" {
		group = DefaultTracesGroupID
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          topic,
		GroupID:        group,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
}
