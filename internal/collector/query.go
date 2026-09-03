package collector

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nisah/pulse-metrics/internal/obs"
	pb "github.com/nisah/pulse-metrics/internal/proto"
)

const (
	defaultQueryLimit = 1000
	maxQueryLimit     = 50000
	defaultLookback   = time.Hour
)

// MetricsService: pb.MetricsServiceServer'in ScyllaDB uzerinde calisan uygulamasi.
type MetricsService struct {
	pb.UnimplementedMetricsServiceServer

	session *gocql.Session
	logger  *zap.Logger
	started time.Time
}

// NewMetricsService: gRPC servis uygulamasini olusturur.
func NewMetricsService(session *gocql.Session, logger *zap.Logger, started time.Time) *MetricsService {
	return &MetricsService{session: session, logger: logger, started: started}
}

// Query: bir metrigin zaman araligindaki degerlerini instance bazinda dondurur.
//
// Her sorgu partition key'i TAM olarak belirler: (service_name, metric_name,
// time_bucket). Zaman araligi birden fazla saate yayiliyorsa kova basina bir
// okuma yapilir; hicbirinde tarama yok, hepsi dogrudan partition erisimi.
func (s *MetricsService) Query(ctx context.Context, req *pb.MetricsQueryRequest) (*pb.MetricsQueryResponse, error) {
	if req.GetServiceName() == "" || req.GetMetricName() == "" {
		return nil, status.Error(codes.InvalidArgument,
			"service_name ve metric_name zorunlu (ikisi birlikte partition key'i olusturur)")
	}

	now := time.Now()
	end := req.GetEndTimeMs()
	if end <= 0 {
		end = now.UnixMilli()
	}
	start := req.GetStartTimeMs()
	if start <= 0 {
		start = now.Add(-defaultLookback).UnixMilli()
	}
	if start > end {
		return nil, status.Error(codes.InvalidArgument, "start_time_ms, end_time_ms'den buyuk olamaz")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultQueryLimit
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}

	began := time.Now()
	defer func() {
		obs.QueryDuration.WithLabelValues("Query", "ok").Observe(time.Since(began).Seconds())
	}()

	// Faz 4: partition key'e time_bucket girdi. Sorgu artik tek bir
	// partition'a degil, araligi kapsayan saatlik kovalara iniyor.
	//
	// Ilk bakista gerileme gibi gorunuyor - bir okuma yerine N okuma.
	// Degil: Cassandra/Scylla'da coklu partition okumasi olagan ve
	// paralellestirilebilir bir erisim bicimi, cunku her kova farkli bir
	// dugumde olabilir. Kaybettigimiz sey tek istekte bitirme kolayligi;
	// kazandigimiz sey partition'larin sinirsiz buyumemesi. Ikincisi
	// olceklenen sistemde her zaman daha degerli.
	baseCQL := `SELECT timestamp, instance_id, type, tags, value FROM metrics
	            WHERE service_name = ? AND metric_name = ? AND time_bucket = ?
	              AND timestamp >= ? AND timestamp <= ?`

	// instance_id basina bir seri topluyoruz.
	type seriesAcc struct {
		instanceID string
		metricType string
		tags       map[string]string
		points     []*pb.DataPoint
	}
	order := []string{}
	byInstance := map[string]*seriesAcc{}
	total := 0

	for _, bucket := range bucketsInRangeMax(start, end, metricBucketLimit) {
		if total >= limit {
			break
		}

		cql := baseCQL
		args := []interface{}{req.GetServiceName(), req.GetMetricName(), bucket, start, end}

		// instance_id son clustering kolonu oldugu icin dogrudan esitlik
		// verirsek ALLOW FILTERING gerekiyor. Tek partition icinde
		// kaldigimiz ve zaman araligiyla sinirli oldugumuz icin bu
		// guvenli ve sinirli bir tarama.
		if inst := req.GetInstanceId(); inst != "" {
			cql += ` AND instance_id = ?`
			args = append(args, inst)
		}
		cql += ` LIMIT ?`
		args = append(args, limit-total)
		if req.GetInstanceId() != "" {
			cql += ` ALLOW FILTERING`
		}

		iter := s.session.Query(cql, args...).WithContext(ctx).Iter()

		var (
			ts         int64
			instanceID string
			metricType string
			tags       map[string]string
			value      float64
		)
		for iter.Scan(&ts, &instanceID, &metricType, &tags, &value) {
			acc, ok := byInstance[instanceID]
			if !ok {
				acc = &seriesAcc{instanceID: instanceID, metricType: metricType, tags: tags}
				byInstance[instanceID] = acc
				order = append(order, instanceID)
			}
			acc.points = append(acc.points, &pb.DataPoint{TimestampMs: ts, Value: value})
			total++
			// tags her satirda yeniden tahsis edilir; kopyayi sifirla.
			tags = nil
		}
		if err := iter.Close(); err != nil {
			s.logger.Error("Query failed", zap.Error(err), zap.String("metric", req.GetMetricName()))
			return nil, status.Errorf(codes.Internal, "sorgu basarisiz: %v", err)
		}
	}

	agg := strings.ToLower(strings.TrimSpace(req.GetAggregation()))
	out := make([]*pb.TimeSeriesData, 0, len(order))

	for _, id := range order {
		acc := byInstance[id]

		// CQL DESC dondurur; grafik icin eskiden yeniye siralamak daha kullanisli.
		sort.Slice(acc.points, func(i, j int) bool {
			return acc.points[i].TimestampMs < acc.points[j].TimestampMs
		})

		points := acc.points
		if agg != "" {
			p, err := aggregate(agg, acc.points)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			points = []*pb.DataPoint{p}
		}

		out = append(out, &pb.TimeSeriesData{
			MetricName: req.GetMetricName(),
			InstanceId: acc.instanceID,
			Tags:       acc.tags,
			Type:       parseMetricType(acc.metricType),
			Points:     points,
		})
	}

	return &pb.MetricsQueryResponse{
		Series:      out,
		QueryTimeMs: time.Since(began).Milliseconds(),
	}, nil
}

// ListSeries: bilinen (servis, metrik) ciftlerini dondurur.
//
// SELECT DISTINCT yalnizca partition key kolonlari uzerinde calisir; Scylla
// her partition icin tek satir dondurur, satirlarin icerigini okumaz.
func (s *MetricsService) ListSeries(ctx context.Context, req *pb.ListSeriesRequest) (*pb.ListSeriesResponse, error) {
	// DISTINCT tum partition key kolonlarini istemek zorunda; time_bucket
	// de partition key'in parcasi oldugu icin ayni (servis, metrik) cifti
	// her saat icin bir kez doner. Tekrarlari burada eliyoruz.
	iter := s.session.Query(`SELECT DISTINCT service_name, metric_name, time_bucket FROM metrics`).
		WithContext(ctx).Iter()

	var (
		svc, metric, bucket string
		out                 []*pb.SeriesRef
	)
	seen := map[string]bool{}
	filter := req.GetServiceName()
	for iter.Scan(&svc, &metric, &bucket) {
		if filter != "" && svc != filter {
			continue
		}
		key := svc + "\x00" + metric
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, &pb.SeriesRef{ServiceName: svc, MetricName: metric})
	}
	if err := iter.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "seriler listelenemedi: %v", err)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ServiceName != out[j].ServiceName {
			return out[i].ServiceName < out[j].ServiceName
		}
		return out[i].MetricName < out[j].MetricName
	})

	return &pb.ListSeriesResponse{Series: out}, nil
}

// Health: ScyllaDB'ye ulasilabiliyor mu?
func (s *MetricsService) Health(ctx context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	resp := &pb.HealthResponse{
		UptimeSeconds: int64(time.Since(s.started).Seconds()),
	}

	if err := s.session.Query("SELECT release_version FROM system.local").WithContext(ctx).Exec(); err != nil {
		resp.Ready = false
		resp.Detail = "scylladb: " + err.Error()
		return resp, nil
	}

	resp.Ready = true
	resp.Detail = "ok"
	return resp, nil
}

// aggregate: bir seriyi tek bir degere indirger.
// Zaman damgasi olarak serinin en yeni noktasinin damgasi kullanilir.
func aggregate(kind string, points []*pb.DataPoint) (*pb.DataPoint, error) {
	if len(points) == 0 {
		return &pb.DataPoint{}, nil
	}

	ts := points[len(points)-1].TimestampMs

	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}

	switch kind {
	case "avg":
		var sum float64
		for _, v := range values {
			sum += v
		}
		return &pb.DataPoint{TimestampMs: ts, Value: sum / float64(len(values))}, nil
	case "sum":
		var sum float64
		for _, v := range values {
			sum += v
		}
		return &pb.DataPoint{TimestampMs: ts, Value: sum}, nil
	case "min":
		m := math.Inf(1)
		for _, v := range values {
			m = math.Min(m, v)
		}
		return &pb.DataPoint{TimestampMs: ts, Value: m}, nil
	case "max":
		m := math.Inf(-1)
		for _, v := range values {
			m = math.Max(m, v)
		}
		return &pb.DataPoint{TimestampMs: ts, Value: m}, nil
	case "count":
		return &pb.DataPoint{TimestampMs: ts, Value: float64(len(values))}, nil
	case "last":
		return &pb.DataPoint{TimestampMs: ts, Value: values[len(values)-1]}, nil
	case "p50", "p95", "p99":
		q := map[string]float64{"p50": 0.50, "p95": 0.95, "p99": 0.99}[kind]
		return &pb.DataPoint{TimestampMs: ts, Value: percentile(values, q)}, nil
	default:
		return nil, fmt.Errorf("bilinmeyen aggregation %q (avg, sum, min, max, count, last, p50, p95, p99)", kind)
	}
}

// percentile: en yakin sira (nearest-rank) yontemiyle yuzdelik hesaplar.
// values yerinde siralanir, bu yuzden cagiran kopyayi verir.
func percentile(values []float64, q float64) float64 {
	sort.Float64s(values)
	if len(values) == 1 {
		return values[0]
	}
	// nearest-rank: ceil(q * N) -> 1 tabanli sira
	rank := int(math.Ceil(q * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(values) {
		rank = len(values)
	}
	return values[rank-1]
}

// parseMetricType: veritabanindaki metin degerini enum'a cevirir.
func parseMetricType(s string) pb.MetricType {
	if v, ok := pb.MetricType_value[strings.ToUpper(s)]; ok {
		return pb.MetricType(v)
	}
	return pb.MetricType_GAUGE
}
