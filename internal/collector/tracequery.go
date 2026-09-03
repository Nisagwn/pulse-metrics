package collector

import (
	"context"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

const (
	defaultTraceLimit = 50
	maxTraceLimit     = 500
)

// TraceService: pb.TraceServiceServer'in ScyllaDB uzerinde calisan uygulamasi.
type TraceService struct {
	pb.UnimplementedTraceServiceServer

	session *gocql.Session
	logger  *zap.Logger
}

// NewTraceService: trace okuma servisini olusturur.
func NewTraceService(session *gocql.Session, logger *zap.Logger) *TraceService {
	return &TraceService{session: session, logger: logger}
}

// QueryTraces: servis + zaman araligi + sure/hata filtreleriyle trace arar.
//
// Once trace_index'ten aday trace_id'leri toplar (saatlik kovalar uzerinde
// yeniden eskiye dogru), sonra her trace'i spans tablosundan yeniden kurar.
// Iki adimli olmasinin sebebi: arama deseni ile okuma deseni farkli, o yuzden
// iki ayri tabloya yaziyoruz.
func (s *TraceService) QueryTraces(ctx context.Context, req *pb.TraceQueryRequest) (*pb.TraceQueryResponse, error) {
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
		limit = defaultTraceLimit
	}
	if limit > maxTraceLimit {
		limit = maxTraceLimit
	}

	began := time.Now()

	traceIDs, err := s.findTraceIDs(ctx, req, start, end, limit)
	if err != nil {
		return nil, err
	}

	traces := make([]*pb.Trace, 0, len(traceIDs))
	for _, id := range traceIDs {
		tr, err := s.loadTrace(ctx, id)
		if err != nil {
			s.logger.Warn("trace yuklenemedi", zap.String("trace_id", id), zap.Error(err))
			continue
		}
		if tr != nil && len(tr.Spans) > 0 {
			traces = append(traces, tr)
		}
	}

	// En yeni trace once.
	sort.Slice(traces, func(i, j int) bool {
		return traces[i].StartTimeMicros > traces[j].StartTimeMicros
	})

	return &pb.TraceQueryResponse{
		Traces:       traces,
		QueryTimeMs:  time.Since(began).Milliseconds(),
		TotalMatches: int64(len(traces)),
	}, nil
}

// findTraceIDs: trace_index uzerinde kovalari gezerek aday trace_id'leri bulur.
func (s *TraceService) findTraceIDs(ctx context.Context, req *pb.TraceQueryRequest, startMs, endMs int64, limit int) ([]string, error) {
	startMicros := startMs * 1000
	endMicros := endMs * 1000

	minDurMicros := int64(req.GetMinDurationMs()) * 1000
	maxDurMicros := int64(req.GetMaxDurationMs()) * 1000

	seen := map[string]bool{}
	var ids []string

	for _, bucket := range bucketsInRange(startMs, endMs) {
		if len(ids) >= limit {
			break
		}

		iter := s.session.Query(`
			SELECT trace_id, operation_name, duration_micros, has_error
			FROM trace_index
			WHERE service_name = ? AND time_bucket = ?
			  AND start_time_micros >= ? AND start_time_micros <= ?`,
			req.GetServiceName(), bucket, startMicros, endMicros,
		).WithContext(ctx).Iter()

		var (
			traceID  string
			opName   string
			duration int64
			hasError bool
		)
		for iter.Scan(&traceID, &opName, &duration, &hasError) {
			if seen[traceID] {
				continue
			}
			// Filtreler Go tarafinda: clustering kolonlari uzerinde
			// olmadiklari icin CQL'de ALLOW FILTERING gerektirirlerdi
			// ve tek partition icinde kaldigimiz surece bu daha ucuz.
			if op := req.GetOperationName(); op != "" && opName != op {
				continue
			}
			if req.GetErrorsOnly() && !hasError {
				continue
			}
			if minDurMicros > 0 && duration < minDurMicros {
				continue
			}
			if maxDurMicros > 0 && duration > maxDurMicros {
				continue
			}

			seen[traceID] = true
			ids = append(ids, traceID)
			if len(ids) >= limit {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "trace_index okunamadi: %v", err)
		}
	}

	return ids, nil
}

// GetTrace: tek bir trace'in tum span'lerini dondurur.
func (s *TraceService) GetTrace(ctx context.Context, req *pb.GetTraceRequest) (*pb.Trace, error) {
	if req.GetTraceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "trace_id zorunlu")
	}

	tr, err := s.loadTrace(ctx, req.GetTraceId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "trace yuklenemedi: %v", err)
	}
	if tr == nil || len(tr.Spans) == 0 {
		return nil, status.Errorf(codes.NotFound, "trace bulunamadi: %s", req.GetTraceId())
	}
	return tr, nil
}

// loadTrace: spans tablosundan tek partition okumasiyla trace'i kurar.
func (s *TraceService) loadTrace(ctx context.Context, traceID string) (*pb.Trace, error) {
	iter := s.session.Query(`
		SELECT span_id, parent_span_id, service_name, operation_name,
		       start_time_micros, end_time_micros, duration_micros,
		       kind, status_code, status_message, attributes, events
		FROM spans WHERE trace_id = ?`, traceID,
	).WithContext(ctx).Iter()

	var (
		spanID, parentID, svc, op        string
		startMicros, endMicros, duration int64
		kind, statusCode, statusMsg      string
		attributes                       map[string]string
		eventsJSON                       string
	)

	tr := &pb.Trace{TraceId: traceID}
	var minStart, maxEnd int64

	for iter.Scan(&spanID, &parentID, &svc, &op, &startMicros, &endMicros,
		&duration, &kind, &statusCode, &statusMsg, &attributes, &eventsJSON) {

		span := &pb.Span{
			TraceId:         traceID,
			SpanId:          spanID,
			ParentSpanId:    parentID,
			OperationName:   op,
			ServiceName:     svc,
			StartTimeMicros: startMicros,
			EndTimeMicros:   endMicros,
			Kind:            parseSpanKind(kind),
			Status: &pb.SpanStatus{
				Code:        parseStatusCode(statusCode),
				Description: statusMsg,
			},
			Attributes: attributes,
			Events:     fromEventsJSON(eventsJSON),
		}
		tr.Spans = append(tr.Spans, span)

		if span.Status.Code == pb.StatusCode_STATUS_CODE_ERROR {
			tr.HasError = true
		}
		if minStart == 0 || startMicros < minStart {
			minStart = startMicros
		}
		if endMicros > maxEnd {
			maxEnd = endMicros
		}

		attributes = nil // her satirda yeniden tahsis edilsin
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	if len(tr.Spans) == 0 {
		return nil, nil
	}

	tr.StartTimeMicros = minStart
	tr.DurationMicros = maxEnd - minStart
	tr.SpanCount = int32(len(tr.Spans))
	return tr, nil
}

// ListOperations: arama formu icin servis ve operasyon listeleri.
func (s *TraceService) ListOperations(ctx context.Context, req *pb.ListOperationsRequest) (*pb.ListOperationsResponse, error) {
	resp := &pb.ListOperationsResponse{}

	// Servisler: partition key uzerinde DISTINCT, ucuz.
	svcIter := s.session.Query(`SELECT DISTINCT service_name FROM service_ops`).
		WithContext(ctx).Iter()
	var svc string
	for svcIter.Scan(&svc) {
		resp.Services = append(resp.Services, svc)
	}
	if err := svcIter.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "servisler listelenemedi: %v", err)
	}
	sort.Strings(resp.Services)

	if req.GetServiceName() != "" {
		opIter := s.session.Query(
			`SELECT operation_name FROM service_ops WHERE service_name = ?`,
			req.GetServiceName()).WithContext(ctx).Iter()
		var op string
		for opIter.Scan(&op) {
			resp.Operations = append(resp.Operations, op)
		}
		if err := opIter.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "operasyonlar listelenemedi: %v", err)
		}
		sort.Strings(resp.Operations)
	}

	return resp, nil
}

// GetTopology: servisler arasi cagri grafigi.
//
// Faz 2'de bu fonksiyon son N trace'i orneklemek, her birini spans
// tablosundan okumak ve ebeveyn-cocuk baglarini yurumek zorundaydi:
// tek bir cagri onlarca partition okumasi demekti.
//
// Faz 3'te kenarlar ingest sirasinda service_edges tablosuna yaziliyor
// (SDK cagiran servisin adini tracestate ile tasiyor, collector onu
// peer.service ozniteliginden okuyor). Burada sadece o kenarlari
// okuyup toplamak kaliyor - ornekleme yok, tahmin yok, tam sayim.
func (s *TraceService) GetTopology(ctx context.Context, req *pb.TopologyRequest) (*pb.ServiceTopology, error) {
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

	buckets := bucketsInRange(start, end)

	// 1) Hangi (cagiran, cagrilan) ciftleri var?
	type pair struct{ caller, callee string }
	pairs := map[pair]bool{}

	for _, bucket := range buckets {
		iter := s.session.Query(
			`SELECT caller_service, callee_service FROM edge_pairs WHERE time_bucket = ?`,
			bucket).WithContext(ctx).Iter()
		var caller, callee string
		for iter.Scan(&caller, &callee) {
			pairs[pair{caller, callee}] = true
		}
		if err := iter.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "edge_pairs okunamadi: %v", err)
		}
	}

	// 2) Her cift icin sureleri topla.
	type edgeAcc struct {
		calls     int64
		errors    int64
		latencies []int64
	}
	edges := map[pair]*edgeAcc{}

	type nodeAcc struct {
		calls   int64
		errors  int64
		totalMs float64
	}
	nodes := map[string]*nodeAcc{}

	touchNode := func(name string) *nodeAcc {
		n := nodes[name]
		if n == nil {
			n = &nodeAcc{}
			nodes[name] = n
		}
		return n
	}

	for pr := range pairs {
		acc := &edgeAcc{}
		for _, bucket := range buckets {
			iter := s.session.Query(`
				SELECT duration_ms, has_error FROM service_edges
				WHERE caller_service = ? AND callee_service = ? AND time_bucket = ?
				  AND timestamp_ms >= ? AND timestamp_ms <= ?`,
				pr.caller, pr.callee, bucket, start, end).WithContext(ctx).Iter()

			var (
				duration int64
				hasError bool
			)
			for iter.Scan(&duration, &hasError) {
				acc.calls++
				acc.latencies = append(acc.latencies, duration)
				if hasError {
					acc.errors++
				}
			}
			if err := iter.Close(); err != nil {
				return nil, status.Errorf(codes.Internal, "service_edges okunamadi: %v", err)
			}
		}
		if acc.calls == 0 {
			continue
		}
		edges[pr] = acc

		// Dugum istatistikleri kenarlardan turetilir: bir servisin
		// gecikmesi, ona yapilan cagrilarin gecikmesidir.
		callee := touchNode(pr.callee)
		callee.calls += acc.calls
		callee.errors += acc.errors
		for _, l := range acc.latencies {
			callee.totalMs += float64(l)
		}
		touchNode(pr.caller) // cagiran da grafikte gorunmeli
	}

	// 3) Hicbir kenari olmayan servisler de dugum olarak gorunmeli
	//    (ornegin sadece kendi icinde calisan bir servis).
	ops, err := s.ListOperations(ctx, &pb.ListOperationsRequest{})
	if err == nil {
		for _, svc := range ops.GetServices() {
			touchNode(svc)
		}
	}

	topo := &pb.ServiceTopology{TimestampMs: now.UnixMilli()}

	for name, n := range nodes {
		var errRate, avg float64
		if n.calls > 0 {
			errRate = float64(n.errors) / float64(n.calls)
			avg = n.totalMs / float64(n.calls)
		}
		topo.Nodes = append(topo.Nodes, &pb.ServiceNode{
			ServiceName:  name,
			ErrorRate:    errRate,
			AvgLatencyMs: avg,
		})
	}
	sort.Slice(topo.Nodes, func(i, j int) bool {
		return topo.Nodes[i].ServiceName < topo.Nodes[j].ServiceName
	})

	for pr, acc := range edges {
		sort.Slice(acc.latencies, func(i, j int) bool { return acc.latencies[i] < acc.latencies[j] })
		var errRate float64
		if acc.calls > 0 {
			errRate = float64(acc.errors) / float64(acc.calls)
		}
		topo.Edges = append(topo.Edges, &pb.ServiceDependency{
			CallerService: pr.caller,
			CalleeService: pr.callee,
			CallCount:     acc.calls,
			ErrorRate:     errRate,
			P50LatencyMs:  pickPercentile(acc.latencies, 0.50),
			P95LatencyMs:  pickPercentile(acc.latencies, 0.95),
			P99LatencyMs:  pickPercentile(acc.latencies, 0.99),
		})
	}
	sort.Slice(topo.Edges, func(i, j int) bool {
		if topo.Edges[i].CallerService != topo.Edges[j].CallerService {
			return topo.Edges[i].CallerService < topo.Edges[j].CallerService
		}
		return topo.Edges[i].CalleeService < topo.Edges[j].CalleeService
	})

	return topo, nil
}

// pickPercentile: siralanmis dilimden nearest-rank yuzdelik.
func pickPercentile(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted))*q + 0.999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func parseSpanKind(s string) pb.SpanKind {
	if v, ok := pb.SpanKind_value[s]; ok {
		return pb.SpanKind(v)
	}
	return pb.SpanKind_SPAN_KIND_UNSPECIFIED
}

func parseStatusCode(s string) pb.StatusCode {
	if v, ok := pb.StatusCode_value[s]; ok {
		return pb.StatusCode(v)
	}
	return pb.StatusCode_STATUS_CODE_UNSET
}
