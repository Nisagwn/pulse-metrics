package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	pb "github.com/nisah/pulse-metrics/pkg/pulsepb"
)

// --- JSON temsilleri --------------------------------------------------------

type spanJSON struct {
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId,omitempty"`
	Service      string            `json:"service"`
	Operation    string            `json:"operation"`
	StartMicros  int64             `json:"startMicros"`
	EndMicros    int64             `json:"endMicros"`
	DurationMs   float64           `json:"durationMs"`
	Kind         string            `json:"kind"`
	Status       string            `json:"status"`
	StatusMsg    string            `json:"statusMessage,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []eventJSON       `json:"events,omitempty"`
}

type eventJSON struct {
	Name       string            `json:"name"`
	TimeMicros int64             `json:"timeMicros"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type traceJSON struct {
	TraceID     string     `json:"traceId"`
	StartMicros int64      `json:"startMicros"`
	DurationMs  float64    `json:"durationMs"`
	SpanCount   int32      `json:"spanCount"`
	HasError    bool       `json:"hasError"`
	RootService string     `json:"rootService,omitempty"`
	RootName    string     `json:"rootOperation,omitempty"`
	Services    []string   `json:"services,omitempty"`
	Spans       []spanJSON `json:"spans,omitempty"`
}

func toSpanJSON(s *pb.Span) spanJSON {
	events := make([]eventJSON, 0, len(s.GetEvents()))
	for _, e := range s.GetEvents() {
		events = append(events, eventJSON{
			Name:       e.GetName(),
			TimeMicros: e.GetTimestampMicros(),
			Attributes: e.GetAttributes(),
		})
	}
	statusCode := "STATUS_CODE_UNSET"
	statusMsg := ""
	if s.GetStatus() != nil {
		statusCode = s.GetStatus().GetCode().String()
		statusMsg = s.GetStatus().GetDescription()
	}
	return spanJSON{
		SpanID:       s.GetSpanId(),
		ParentSpanID: s.GetParentSpanId(),
		Service:      s.GetServiceName(),
		Operation:    s.GetOperationName(),
		StartMicros:  s.GetStartTimeMicros(),
		EndMicros:    s.GetEndTimeMicros(),
		DurationMs:   float64(s.GetEndTimeMicros()-s.GetStartTimeMicros()) / 1000,
		Kind:         s.GetKind().String(),
		Status:       statusCode,
		StatusMsg:    statusMsg,
		Attributes:   s.GetAttributes(),
		Events:       events,
	}
}

func toTraceJSON(t *pb.Trace, includeSpans bool) traceJSON {
	out := traceJSON{
		TraceID:     t.GetTraceId(),
		StartMicros: t.GetStartTimeMicros(),
		DurationMs:  float64(t.GetDurationMicros()) / 1000,
		SpanCount:   t.GetSpanCount(),
		HasError:    t.GetHasError(),
	}

	seen := map[string]bool{}
	for _, s := range t.GetSpans() {
		if s.GetParentSpanId() == "" {
			out.RootService = s.GetServiceName()
			out.RootName = s.GetOperationName()
		}
		if !seen[s.GetServiceName()] {
			seen[s.GetServiceName()] = true
			out.Services = append(out.Services, s.GetServiceName())
		}
		if includeSpans {
			out.Spans = append(out.Spans, toSpanJSON(s))
		}
	}

	// Kok bulunamadiysa (ornegin kok span henuz gelmemisse) en erken
	// span'i kok gibi goster - liste bos kalmasin.
	if out.RootService == "" && len(t.GetSpans()) > 0 {
		out.RootService = t.GetSpans()[0].GetServiceName()
		out.RootName = t.GetSpans()[0].GetOperationName()
	}
	return out
}

// --- HTTP handler'lari ------------------------------------------------------

// /api/v1/traces?service=&operation=&range=15m&minDuration=&errorsOnly=&limit=
func (s *server) handleTraces(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	svc := q.Get("service")
	if svc == "" {
		writeError(w, http.StatusBadRequest, errors.New("service parametresi zorunlu"))
		return
	}

	now := time.Now()
	from, to := now.Add(-time.Hour).UnixMilli(), now.UnixMilli()
	if raw := q.Get("range"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			writeError(w, http.StatusBadRequest,
				fmt.Errorf("gecersiz range %q (ornek: 15m, 1h, 6h)", raw))
			return
		}
		from = now.Add(-d).UnixMilli()
	}

	limit, _ := strconv.Atoi(q.Get("limit"))
	minDur, _ := strconv.Atoi(q.Get("minDurationMs"))
	maxDur, _ := strconv.Atoi(q.Get("maxDurationMs"))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	resp, err := s.traces.QueryTraces(ctx, &pb.TraceQueryRequest{
		ServiceName:   svc,
		OperationName: q.Get("operation"),
		StartTimeMs:   from,
		EndTimeMs:     to,
		MinDurationMs: int32(minDur),
		MaxDurationMs: int32(maxDur),
		ErrorsOnly:    q.Get("errorsOnly") == "true",
		Limit:         int32(limit),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	out := make([]traceJSON, 0, len(resp.GetTraces()))
	for _, t := range resp.GetTraces() {
		out = append(out, toTraceJSON(t, false)) // listede span'ler gerekmez
	}

	writeJSON(w, map[string]interface{}{
		"queryTimeMs": resp.GetQueryTimeMs(),
		"total":       resp.GetTotalMatches(),
		"from":        from,
		"to":          to,
		"traces":      out,
	})
}

// /api/v1/trace?id=<trace_id>
func (s *server) handleTrace(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, errors.New("id parametresi zorunlu"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	tr, err := s.traces.GetTrace(ctx, &pb.GetTraceRequest{TraceId: id})
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, toTraceJSON(tr, true))
}

// /api/v1/operations?service=
func (s *server) handleOperations(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := s.traces.ListOperations(ctx, &pb.ListOperationsRequest{
		ServiceName: r.URL.Query().Get("service"),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, map[string]interface{}{
		"services":   resp.GetServices(),
		"operations": resp.GetOperations(),
	})
}

// /api/v1/topology?range=1h&sample=200
func (s *server) handleTopology(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	now := time.Now()
	from := now.Add(-time.Hour).UnixMilli()
	if raw := q.Get("range"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			from = now.Add(-d).UnixMilli()
		}
	}
	sample, _ := strconv.Atoi(q.Get("sample"))

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	topo, err := s.traces.GetTopology(ctx, &pb.TopologyRequest{
		StartTimeMs: from,
		EndTimeMs:   now.UnixMilli(),
		SampleLimit: int32(sample),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	type nodeJSON struct {
		Service      string  `json:"service"`
		ErrorRate    float64 `json:"errorRate"`
		AvgLatencyMs float64 `json:"avgLatencyMs"`
	}
	type edgeJSON struct {
		From      string  `json:"from"`
		To        string  `json:"to"`
		Calls     int64   `json:"calls"`
		ErrorRate float64 `json:"errorRate"`
		P50       int64   `json:"p50Ms"`
		P95       int64   `json:"p95Ms"`
		P99       int64   `json:"p99Ms"`
	}

	nodes := make([]nodeJSON, 0, len(topo.GetNodes()))
	for _, n := range topo.GetNodes() {
		nodes = append(nodes, nodeJSON{
			Service:      n.GetServiceName(),
			ErrorRate:    n.GetErrorRate(),
			AvgLatencyMs: n.GetAvgLatencyMs(),
		})
	}
	edges := make([]edgeJSON, 0, len(topo.GetEdges()))
	for _, e := range topo.GetEdges() {
		edges = append(edges, edgeJSON{
			From:      e.GetCallerService(),
			To:        e.GetCalleeService(),
			Calls:     e.GetCallCount(),
			ErrorRate: e.GetErrorRate(),
			P50:       e.GetP50LatencyMs(),
			P95:       e.GetP95LatencyMs(),
			P99:       e.GetP99LatencyMs(),
		})
	}

	writeJSON(w, map[string]interface{}{
		"nodes": nodes,
		"edges": edges,
		"from":  from,
		"to":    now.UnixMilli(),
	})
}
