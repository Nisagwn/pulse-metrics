// Package health: tum servislerin paylastigi saglik kontrolu HTTP sunucusu.
//
// Iki uc var ve ayrimlari onemli:
//
//	/healthz  liveness  - surec ayakta mi? Hep 200 doner.
//	                      Basarisiz olursa orkestrator sureci YENIDEN BASLATIR.
//	/readyz   readiness - is yapabilir durumda mi? Bagimliliklari kontrol eder.
//	                      Basarisiz olursa orkestrator trafigi keser ama oldurmez.
//
// Ayrimi kacirmak klasik bir hatadir: ScyllaDB gecici olarak dustugunde
// /healthz de basarisiz olursa, Kubernetes tum collector'lari sonsuz
// dongude yeniden baslatir ve veritabani geri geldiginde ayakta kimse kalmaz.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Check: bir bagimliligin erisilebilirligini sinar. nil = saglikli.
type Check func(context.Context) error

// Server: saglik kontrolu HTTP sunucusu.
type Server struct {
	addr    string
	logger  *zap.Logger
	started time.Time

	mu     sync.RWMutex
	checks map[string]Check
}

// New: verilen adreste dinleyecek bir saglik sunucusu olusturur.
func New(addr string, logger *zap.Logger) *Server {
	return &Server{
		addr:    addr,
		logger:  logger,
		started: time.Now(),
		checks:  make(map[string]Check),
	}
}

// AddCheck: /readyz tarafindan calistirilacak bir bagimlilik kontrolu ekler.
func (s *Server) AddCheck(name string, fn Check) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = fn
}

type response struct {
	Status  string            `json:"status"`
	Uptime  string            `json:"uptime"`
	Checks  map[string]string `json:"checks,omitempty"`
	Version string            `json:"version"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	// Liveness: surec cevap verebiliyorsa saglikli sayilir.
	writeJSON(w, http.StatusOK, response{
		Status:  "ok",
		Uptime:  time.Since(s.started).Round(time.Second).String(),
		Version: "week1",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	s.mu.RLock()
	checks := make(map[string]Check, len(s.checks))
	for k, v := range s.checks {
		checks[k] = v
	}
	s.mu.RUnlock()

	results := make(map[string]string, len(checks))
	ready := true
	for name, fn := range checks {
		if err := fn(ctx); err != nil {
			results[name] = err.Error()
			ready = false
			continue
		}
		results[name] = "ok"
	}

	status, code := "ready", http.StatusOK
	if !ready {
		status, code = "not ready", http.StatusServiceUnavailable
	}

	writeJSON(w, code, response{
		Status:  status,
		Uptime:  time.Since(s.started).Round(time.Second).String(),
		Checks:  results,
		Version: "week1",
	})
}

func writeJSON(w http.ResponseWriter, code int, body response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// Handler: ucları bir mux'a baglar. Kendi HTTP sunucusu olan servisler
// (dashboard-api gibi) Serve yerine bunu kullanabilir.
func (s *Server) Handler(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
}

// Serve: HTTP sunucusunu baslatir ve ctx iptal edilene kadar calistirir.
// Kapanista bekleyen istekleri tamamlamasi icin 5 saniye tanir.
func (s *Server) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	s.Handler(mux)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("Health server listening", zap.String("addr", s.addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
