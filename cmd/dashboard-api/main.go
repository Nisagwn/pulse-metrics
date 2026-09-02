// dashboard-api: tarayici ile collector arasindaki kopru.
//
// Neden ayri bir servis? Tarayicilar ham gRPC konusamaz. Collector'in
// gRPC servisi veriyi verir, bu servis onu JSON'a cevirip statik paneli
// de sunar. Ayrica collector'i sadece veri alma isine odakli birakir:
// panel trafigi artarsa bu servisi ayrica olceklendirebilirsin.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nisah/pulse-metrics/internal/health"
	pb "github.com/nisah/pulse-metrics/internal/proto"
)

//go:embed web/index.html
var webFS embed.FS

type server struct {
	client pb.MetricsServiceClient
}

func newLogger() (*zap.Logger, error) {
	return zap.NewProduction()
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	collectorAddr := flag.String("collector", "localhost:50051", "Collector gRPC address")
	flag.Parse()

	logger, err := newLogger()
	if err != nil {
		log.Fatalf("logger olusturulamadi: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	conn, err := grpc.NewClient(*collectorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("collector'a baglanilamadi: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s := &server{client: pb.NewMetricsServiceClient(conn)}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/series", s.handleSeries)
	mux.HandleFunc("/api/v1/query", s.handleQuery)
	mux.HandleFunc("/", s.handleIndex)

	hs := health.New("", logger)
	hs.AddCheck("collector", s.pingCollector)
	hs.Handler(mux) // /healthz ve /readyz ayni portta

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("Dashboard API listening on %s (collector: %s)", *addr, *collectorAddr)
	log.Printf("Panel: http://localhost%s", *addr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP sunucusu durdu: %v", err)
	}
	log.Println("Dashboard API shut down cleanly")
}

func (s *server) pingCollector(ctx context.Context) error {
	resp, err := s.client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return err
	}
	if !resp.GetReady() {
		return fmt.Errorf("collector hazir degil: %s", resp.GetDetail())
	}
	return nil
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	page, err := webFS.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "panel yuklenemedi", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page)
}

type seriesItem struct {
	Service string `json:"service"`
	Metric  string `json:"metric"`
}

func (s *server) handleSeries(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := s.client.ListSeries(ctx, &pb.ListSeriesRequest{
		ServiceName: r.URL.Query().Get("service"),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	out := make([]seriesItem, 0, len(resp.GetSeries()))
	for _, sr := range resp.GetSeries() {
		out = append(out, seriesItem{Service: sr.GetServiceName(), Metric: sr.GetMetricName()})
	}
	writeJSON(w, map[string]interface{}{"series": out})
}

type point struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

type series struct {
	Metric   string            `json:"metric"`
	Instance string            `json:"instance"`
	Type     string            `json:"type"`
	Tags     map[string]string `json:"tags,omitempty"`
	Points   []point           `json:"points"`
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	svc, metric := q.Get("service"), q.Get("metric")
	if svc == "" || metric == "" {
		writeError(w, http.StatusBadRequest,
			errors.New("service ve metric parametreleri zorunlu"))
		return
	}

	now := time.Now()
	from, to := now.Add(-time.Hour).UnixMilli(), now.UnixMilli()

	// range=15m gibi goreli bir pencere, ya da from/to ile mutlak milisaniye.
	if raw := q.Get("range"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			writeError(w, http.StatusBadRequest,
				fmt.Errorf("gecersiz range %q (ornek: 15m, 1h, 24h)", raw))
			return
		}
		from = now.Add(-d).UnixMilli()
	} else {
		if v, err := parseMillis(q.Get("from")); err == nil {
			from = v
		}
		if v, err := parseMillis(q.Get("to")); err == nil {
			to = v
		}
	}

	limit := 0
	if v, err := strconv.Atoi(q.Get("limit")); err == nil {
		limit = v
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := s.client.Query(ctx, &pb.MetricsQueryRequest{
		ServiceName: svc,
		MetricName:  metric,
		InstanceId:  q.Get("instance"),
		StartTimeMs: from,
		EndTimeMs:   to,
		Limit:       int32(limit),
		Aggregation: q.Get("agg"),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	out := make([]series, 0, len(resp.GetSeries()))
	for _, sd := range resp.GetSeries() {
		pts := make([]point, 0, len(sd.GetPoints()))
		for _, p := range sd.GetPoints() {
			pts = append(pts, point{T: p.GetTimestampMs(), V: p.GetValue()})
		}
		out = append(out, series{
			Metric:   sd.GetMetricName(),
			Instance: sd.GetInstanceId(),
			Type:     sd.GetType().String(),
			Tags:     sd.GetTags(),
			Points:   pts,
		})
	}

	writeJSON(w, map[string]interface{}{
		"queryTimeMs": resp.GetQueryTimeMs(),
		"from":        from,
		"to":          to,
		"series":      out,
	})
}

func parseMillis(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("bos")
	}
	return strconv.ParseInt(s, 10, 64)
}

func writeJSON(w http.ResponseWriter, body interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
