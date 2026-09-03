package collector

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nisah/pulse-metrics/pkg/logging"
	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

const (
	defaultLogLimit = 200
	maxLogLimit     = 5000
)

// LogServiceServer: pb.LogServiceServer uygulamasi.
type LogServiceServer struct {
	pb.UnimplementedLogServiceServer

	session *gocql.Session
	logger  *zap.Logger
}

// NewLogServiceServer: log okuma servisini olusturur.
func NewLogServiceServer(session *gocql.Session, logger *zap.Logger) *LogServiceServer {
	return &LogServiceServer{session: session, logger: logger}
}

// QueryLogs: servis + zaman araligi + seviye + metin filtresiyle arama.
//
// Metin aramasi Go tarafinda yapiliyor. ScyllaDB'de tam metin arama yok;
// gercek bir sistemde bu is Elasticsearch benzeri bir indekse verilir.
// Burada partition ve zaman araligiyla veri kumesini once daraltip
// sonra filtreliyoruz - kucuk olcekte dogru ve dogrulanabilir bir yaklasim.
func (s *LogServiceServer) QueryLogs(ctx context.Context, req *pb.LogsQueryRequest) (*pb.LogsQueryResponse, error) {
	if req.GetServiceName() == "" {
		return nil, status.Error(codes.InvalidArgument,
			"service_name zorunlu (time_bucket ile birlikte partition key'i olusturur)")
	}

	now := time.Now()
	end := req.GetEndTimeMs()
	if end <= 0 {
		end = now.UnixMilli()
	}
	start := req.GetStartTimeMs()
	if start <= 0 {
		start = now.Add(-time.Hour).UnixMilli()
	}
	if start > end {
		return nil, status.Error(codes.InvalidArgument, "start_time_ms, end_time_ms'den buyuk olamaz")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultLogLimit
	}
	if limit > maxLogLimit {
		limit = maxLogLimit
	}

	wantLevels := map[string]bool{}
	for _, l := range req.GetLevels() {
		wantLevels[logging.LevelName(l)] = true
	}

	needle := strings.ToLower(strings.TrimSpace(req.GetQuery()))
	began := time.Now()

	var out []*pb.LogRecord
	for _, bucket := range bucketsInRange(start, end) {
		if len(out) >= limit {
			break
		}

		iter := s.session.Query(`
			SELECT timestamp_ms, level, message, trace_id, span_id,
			       logger_name, stack_trace, attributes
			FROM logs
			WHERE service_name = ? AND time_bucket = ?
			  AND timestamp_ms >= ? AND timestamp_ms <= ?`,
			req.GetServiceName(), bucket, start, end,
		).WithContext(ctx).Iter()

		var (
			ts                          int64
			level, msg, traceID, spanID string
			loggerName, stack           string
			attrs                       map[string]string
		)
		for iter.Scan(&ts, &level, &msg, &traceID, &spanID, &loggerName, &stack, &attrs) {
			if len(wantLevels) > 0 && !wantLevels[level] {
				attrs = nil
				continue
			}
			if needle != "" && !matchesLog(needle, msg, attrs) {
				attrs = nil
				continue
			}

			out = append(out, &pb.LogRecord{
				TimestampMs: ts,
				Level:       logging.ParseLevel(level),
				Message:     msg,
				TraceId:     traceID,
				SpanId:      spanID,
				LoggerName:  loggerName,
				StackTrace:  stack,
				ServiceName: req.GetServiceName(),
				Attributes:  attrs,
			})
			attrs = nil

			if len(out) >= limit {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "loglar okunamadi: %v", err)
		}
	}

	// sort_order "asc" degilse en yeni once (varsayilan).
	if strings.ToLower(req.GetSortOrder()) == "asc" {
		sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampMs < out[j].TimestampMs })
	} else {
		sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampMs > out[j].TimestampMs })
	}

	return &pb.LogsQueryResponse{
		Logs:         out,
		QueryTimeMs:  time.Since(began).Milliseconds(),
		TotalMatches: int64(len(out)),
	}, nil
}

// matchesLog: mesajda ya da oznitelik degerlerinde arama metnini arar.
func matchesLog(needle, msg string, attrs map[string]string) bool {
	if strings.Contains(strings.ToLower(msg), needle) {
		return true
	}
	for k, v := range attrs {
		if strings.Contains(strings.ToLower(v), needle) ||
			strings.Contains(strings.ToLower(k), needle) {
			return true
		}
	}
	return false
}

// GetTraceLogs: bir trace boyunca uretilen tum loglar.
//
// Uc ayagi birlestiren sorgu budur: trace nerede yavasladigini,
// bu loglar neden basarisiz oldugunu soyler. Tek partition okumasi.
func (s *LogServiceServer) GetTraceLogs(ctx context.Context, req *pb.GetTraceLogsRequest) (*pb.LogsQueryResponse, error) {
	if req.GetTraceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "trace_id zorunlu")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultLogLimit
	}

	began := time.Now()

	iter := s.session.Query(`
		SELECT timestamp_ms, service_name, span_id, level, message, logger_name, attributes
		FROM trace_logs WHERE trace_id = ? LIMIT ?`,
		req.GetTraceId(), limit).WithContext(ctx).Iter()

	var (
		ts                             int64
		svc, spanID, level, msg, lname string
		attrs                          map[string]string
		out                            []*pb.LogRecord
	)
	for iter.Scan(&ts, &svc, &spanID, &level, &msg, &lname, &attrs) {
		out = append(out, &pb.LogRecord{
			TimestampMs: ts,
			Level:       logging.ParseLevel(level),
			Message:     msg,
			TraceId:     req.GetTraceId(),
			SpanId:      spanID,
			ServiceName: svc,
			LoggerName:  lname,
			Attributes:  attrs,
		})
		attrs = nil
	}
	if err := iter.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "trace loglari okunamadi: %v", err)
	}

	return &pb.LogsQueryResponse{
		Logs:         out,
		QueryTimeMs:  time.Since(began).Milliseconds(),
		TotalMatches: int64(len(out)),
	}, nil
}

// ListLogServices: log gonderen servisler.
func (s *LogServiceServer) ListLogServices(ctx context.Context, _ *pb.ListLogServicesRequest) (*pb.ListLogServicesResponse, error) {
	iter := s.session.Query(`SELECT service_name FROM log_services`).WithContext(ctx).Iter()

	var (
		svc string
		out []string
	)
	for iter.Scan(&svc) {
		out = append(out, svc)
	}
	if err := iter.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "servisler listelenemedi: %v", err)
	}
	sort.Strings(out)
	return &pb.ListLogServicesResponse{Services: out}, nil
}

// --- log kalibi tespiti -----------------------------------------------------

// Degisken parcalari maskelemek icin desenler. Sira onemli: once daha
// ozel olanlar (UUID, hex) sonra genel olanlar (sayi).
var (
	reUUID   = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	reHex    = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
	reIP     = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	reQuoted = regexp.MustCompile(`"[^"]*"`)
	reNumber = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
)

// Normalize: bir log mesajindaki degisken parcalari maskeler.
//
//	"kart reddedildi: ord-123456, tutar 42.50"
//	-> "kart reddedildi: ord-<N>, tutar <N>"
//
// Boylece milyonlarca satir, elle okunabilir bir avuc kalibba iner.
func Normalize(msg string) string {
	s := reUUID.ReplaceAllString(msg, "<UUID>")
	s = reIP.ReplaceAllString(s, "<IP>")
	s = reHex.ReplaceAllString(s, "<HEX>")
	s = reQuoted.ReplaceAllString(s, `"<STR>"`)
	s = reNumber.ReplaceAllString(s, "<N>")
	return strings.TrimSpace(s)
}

// DetectPatterns: tekrar eden log kaliplarini bulur ve hata korelasyonunu olcer.
//
// error_correlation: bu kalibi tasiyan kayitlarin yuzde kaci ERROR/FATAL?
// 1.0'a yakin bir kalip, incelenmesi gereken ilk yerdir.
func (s *LogServiceServer) DetectPatterns(ctx context.Context, req *pb.DetectPatternsRequest) (*pb.LogAnomalies, error) {
	if req.GetServiceName() == "" {
		return nil, status.Error(codes.InvalidArgument, "service_name zorunlu")
	}

	now := time.Now()
	end := req.GetEndTimeMs()
	if end <= 0 {
		end = now.UnixMilli()
	}
	start := req.GetStartTimeMs()
	if start <= 0 {
		start = now.Add(-time.Hour).UnixMilli()
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}

	began := time.Now()

	type acc struct {
		count   int64
		errors  int64
		example string
	}
	patterns := map[string]*acc{}
	scanned := 0

	for _, bucket := range bucketsInRange(start, end) {
		if scanned >= maxLogLimit {
			break
		}
		iter := s.session.Query(`
			SELECT level, message FROM logs
			WHERE service_name = ? AND time_bucket = ?
			  AND timestamp_ms >= ? AND timestamp_ms <= ?`,
			req.GetServiceName(), bucket, start, end).WithContext(ctx).Iter()

		var level, msg string
		for iter.Scan(&level, &msg) {
			scanned++
			key := Normalize(msg)
			a := patterns[key]
			if a == nil {
				a = &acc{example: msg}
				patterns[key] = a
			}
			a.count++
			if level == "ERROR" || level == "FATAL" {
				a.errors++
			}
			if scanned >= maxLogLimit {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "loglar okunamadi: %v", err)
		}
	}

	out := make([]*pb.LogPattern, 0, len(patterns))
	for key, a := range patterns {
		var corr float64
		if a.count > 0 {
			corr = float64(a.errors) / float64(a.count)
		}
		out = append(out, &pb.LogPattern{
			Pattern:          key,
			Count:            a.count,
			ExampleMessage:   a.example,
			AffectedServices: []string{req.GetServiceName()},
			ErrorCorrelation: corr,
		})
	}

	// Once hata korelasyonu yuksek olanlar, sonra sik olanlar.
	sort.Slice(out, func(i, j int) bool {
		if out[i].ErrorCorrelation != out[j].ErrorCorrelation {
			return out[i].ErrorCorrelation > out[j].ErrorCorrelation
		}
		return out[i].Count > out[j].Count
	})
	if len(out) > limit {
		out = out[:limit]
	}

	desc := "kalip bulunamadi"
	if len(out) > 0 {
		desc = fmt.Sprintf("%d farkli kalip, %d log satirindan cikarildi",
			len(patterns), scanned)
	}

	return &pb.LogAnomalies{
		Patterns:        out,
		DetectionTimeMs: time.Since(began).Milliseconds(),
		Description:     desc,
	}, nil
}
