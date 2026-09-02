package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nisah/pulse-metrics/internal/logging"
	pb "github.com/nisah/pulse-metrics/internal/proto"
)

// --- loglar -----------------------------------------------------------------

type logJSON struct {
	TimestampMs int64             `json:"t"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	Service     string            `json:"service"`
	TraceID     string            `json:"traceId,omitempty"`
	SpanID      string            `json:"spanId,omitempty"`
	Logger      string            `json:"logger,omitempty"`
	Stack       string            `json:"stack,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

func toLogJSON(r *pb.LogRecord) logJSON {
	return logJSON{
		TimestampMs: r.GetTimestampMs(),
		Level:       logging.LevelName(r.GetLevel()),
		Message:     r.GetMessage(),
		Service:     r.GetServiceName(),
		TraceID:     r.GetTraceId(),
		SpanID:      r.GetSpanId(),
		Logger:      r.GetLoggerName(),
		Stack:       r.GetStackTrace(),
		Attributes:  r.GetAttributes(),
	}
}

func logsResponse(resp *pb.LogsQueryResponse) map[string]interface{} {
	out := make([]logJSON, 0, len(resp.GetLogs()))
	for _, r := range resp.GetLogs() {
		out = append(out, toLogJSON(r))
	}
	return map[string]interface{}{
		"queryTimeMs": resp.GetQueryTimeMs(),
		"total":       resp.GetTotalMatches(),
		"logs":        out,
	}
}

// /api/v1/logs?service=&range=1h&q=&levels=ERROR,WARN&limit=
func (s *server) handleLogs(w http.ResponseWriter, r *http.Request) {
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

	var levels []pb.LogLevel
	if raw := q.Get("levels"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if lvl := logging.ParseLevel(strings.TrimSpace(part)); lvl != pb.LogLevel_LEVEL_UNSPECIFIED {
				levels = append(levels, lvl)
			}
		}
	}

	limit, _ := strconv.Atoi(q.Get("limit"))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	resp, err := s.logs.QueryLogs(ctx, &pb.LogsQueryRequest{
		ServiceName: svc,
		Query:       q.Get("q"),
		StartTimeMs: from,
		EndTimeMs:   to,
		Levels:      levels,
		Limit:       int32(limit),
		SortOrder:   q.Get("sort"),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	body := logsResponse(resp)
	body["from"] = from
	body["to"] = to
	writeJSON(w, body)
}

// /api/v1/trace-logs?id=<trace_id>
func (s *server) handleTraceLogs(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, errors.New("id parametresi zorunlu"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	resp, err := s.logs.GetTraceLogs(ctx, &pb.GetTraceLogsRequest{TraceId: id, Limit: 500})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, logsResponse(resp))
}

// /api/v1/log-services
func (s *server) handleLogServices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := s.logs.ListLogServices(ctx, &pb.ListLogServicesRequest{})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, map[string]interface{}{"services": resp.GetServices()})
}

// /api/v1/log-patterns?service=&range=1h
func (s *server) handleLogPatterns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	svc := q.Get("service")
	if svc == "" {
		writeError(w, http.StatusBadRequest, errors.New("service parametresi zorunlu"))
		return
	}

	now := time.Now()
	from := now.Add(-time.Hour).UnixMilli()
	if raw := q.Get("range"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			from = now.Add(-d).UnixMilli()
		}
	}
	limit, _ := strconv.Atoi(q.Get("limit"))

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	resp, err := s.logs.DetectPatterns(ctx, &pb.DetectPatternsRequest{
		ServiceName: svc,
		StartTimeMs: from,
		EndTimeMs:   now.UnixMilli(),
		Limit:       int32(limit),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	type patternJSON struct {
		Pattern          string  `json:"pattern"`
		Count            int64   `json:"count"`
		Example          string  `json:"example"`
		ErrorCorrelation float64 `json:"errorCorrelation"`
	}
	out := make([]patternJSON, 0, len(resp.GetPatterns()))
	for _, p := range resp.GetPatterns() {
		out = append(out, patternJSON{
			Pattern:          p.GetPattern(),
			Count:            p.GetCount(),
			Example:          p.GetExampleMessage(),
			ErrorCorrelation: p.GetErrorCorrelation(),
		})
	}

	writeJSON(w, map[string]interface{}{
		"patterns":        out,
		"description":     resp.GetDescription(),
		"detectionTimeMs": resp.GetDetectionTimeMs(),
	})
}

// --- alarmlar ---------------------------------------------------------------

type ruleJSON struct {
	RuleID          string `json:"ruleId"`
	Name            string `json:"name"`
	Service         string `json:"service"`
	Metric          string `json:"metric"`
	Condition       string `json:"condition"`
	DurationSeconds int32  `json:"durationSeconds"`
	WebhookURL      string `json:"webhookUrl,omitempty"`
	Enabled         bool   `json:"enabled"`
	Severity        string `json:"severity"`
	CreatedAtMs     int64  `json:"createdAtMs"`
	Firing          bool   `json:"firing"`
}

type alertJSON struct {
	RuleID      string  `json:"ruleId"`
	RuleName    string  `json:"ruleName"`
	Service     string  `json:"service"`
	Metric      string  `json:"metric"`
	Condition   string  `json:"condition"`
	Value       float64 `json:"value"`
	Threshold   float64 `json:"threshold"`
	Severity    string  `json:"severity"`
	Message     string  `json:"message"`
	TimestampMs int64   `json:"t"`
	Resolved    bool    `json:"resolved"`
}

func toAlertJSON(a *pb.Alert) alertJSON {
	return alertJSON{
		RuleID:      a.GetRuleId(),
		RuleName:    a.GetRuleName(),
		Service:     a.GetServiceName(),
		Metric:      a.GetMetricName(),
		Condition:   a.GetCondition(),
		Value:       a.GetMetricValue(),
		Threshold:   a.GetThreshold(),
		Severity:    a.GetSeverity().String(),
		Message:     a.GetMessage(),
		TimestampMs: a.GetTimestampMs(),
		Resolved:    a.GetResolved(),
	}
}

// /api/v1/rules   GET: listele, POST: olustur, DELETE: sil (?id=)
func (s *server) handleRules(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodGet:
		resp, err := s.alerts.ListRules(ctx, &pb.ListRulesRequest{
			ServiceName: r.URL.Query().Get("service"),
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}

		// Hangi kurallar su anda tetiklenmis?
		firing := map[string]bool{}
		if al, err := s.alerts.ListAlerts(ctx, &pb.ListAlertsRequest{Limit: 1}); err == nil {
			for _, id := range al.GetActiveRuleIds() {
				firing[id] = true
			}
		}

		out := make([]ruleJSON, 0, len(resp.GetRules()))
		for _, rule := range resp.GetRules() {
			out = append(out, ruleJSON{
				RuleID:          rule.GetRuleId(),
				Name:            rule.GetName(),
				Service:         rule.GetServiceName(),
				Metric:          rule.GetMetricName(),
				Condition:       rule.GetCondition(),
				DurationSeconds: rule.GetDurationSeconds(),
				WebhookURL:      rule.GetWebhookUrl(),
				Enabled:         rule.GetEnabled(),
				Severity:        rule.GetSeverity().String(),
				CreatedAtMs:     rule.GetCreatedAtMs(),
				Firing:          firing[rule.GetRuleId()],
			})
		}
		writeJSON(w, map[string]interface{}{"rules": out})

	case http.MethodPost:
		var body struct {
			Name            string `json:"name"`
			Service         string `json:"service"`
			Metric          string `json:"metric"`
			Condition       string `json:"condition"`
			DurationSeconds int32  `json:"durationSeconds"`
			WebhookURL      string `json:"webhookUrl"`
			Severity        string `json:"severity"`
			Enabled         *bool  `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("gecersiz JSON: %w", err))
			return
		}

		enabled := true
		if body.Enabled != nil {
			enabled = *body.Enabled
		}
		severity := pb.AlertSeverity_WARNING
		if v, ok := pb.AlertSeverity_value[strings.ToUpper(body.Severity)]; ok {
			severity = pb.AlertSeverity(v)
		}

		resp, err := s.alerts.CreateRule(ctx, &pb.AlertRule{
			Name:            body.Name,
			ServiceName:     body.Service,
			MetricName:      body.Metric,
			Condition:       body.Condition,
			DurationSeconds: body.DurationSeconds,
			WebhookUrl:      body.WebhookURL,
			Enabled:         enabled,
			Severity:        severity,
		})
		if err != nil {
			// Kosul dogrulama hatasi kullanicinin hatasi: 400 dondur.
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, map[string]interface{}{"ruleId": resp.GetRuleId()})

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			writeError(w, http.StatusBadRequest, errors.New("id parametresi zorunlu"))
			return
		}
		if _, err := s.alerts.DeleteRule(ctx, &pb.DeleteRuleRequest{RuleId: id}); err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, map[string]interface{}{"deleted": true})

	default:
		writeError(w, http.StatusMethodNotAllowed, errors.New("desteklenmeyen metot"))
	}
}

// /api/v1/alerts?service=&range=24h&firingOnly=true
func (s *server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	now := time.Now()
	from := now.Add(-24 * time.Hour).UnixMilli()
	if raw := q.Get("range"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			from = now.Add(-d).UnixMilli()
		}
	}
	limit, _ := strconv.Atoi(q.Get("limit"))

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	resp, err := s.alerts.ListAlerts(ctx, &pb.ListAlertsRequest{
		ServiceName: q.Get("service"),
		StartTimeMs: from,
		EndTimeMs:   now.UnixMilli(),
		Limit:       int32(limit),
		FiringOnly:  q.Get("firingOnly") == "true",
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	out := make([]alertJSON, 0, len(resp.GetAlerts()))
	for _, a := range resp.GetAlerts() {
		out = append(out, toAlertJSON(a))
	}
	writeJSON(w, map[string]interface{}{
		"alerts":        out,
		"activeRuleIds": resp.GetActiveRuleIds(),
	})
}

// /api/v1/evaluate  POST: kurallari hemen degerlendir
func (s *server) handleEvaluate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("POST bekleniyor"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	resp, err := s.alerts.EvaluateRules(ctx, &pb.EvaluateRulesRequest{
		RuleId: r.URL.Query().Get("id"),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	out := make([]alertJSON, 0, len(resp.GetFired()))
	for _, a := range resp.GetFired() {
		out = append(out, toAlertJSON(a))
	}
	writeJSON(w, map[string]interface{}{
		"evaluated": resp.GetEvaluated(),
		"fired":     out,
	})
}
