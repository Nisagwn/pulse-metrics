package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

func newTestServer(t *testing.T) (*Server, *http.ServeMux) {
	t.Helper()
	s := New("", zap.NewNop())
	mux := http.NewServeMux()
	s.Handler(mux)
	return s, mux
}

func do(t *testing.T, mux *http.ServeMux, path string) (int, response) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body response
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("%s yaniti cozulemedi: %v", path, err)
	}
	return rec.Code, body
}

// Liveness, bagimliliklar bozuk olsa bile 200 donmeli. Aksi halde
// ScyllaDB kisa sureligine dustugunde orkestrator tum surecleri
// sonsuz dongude yeniden baslatir.
func TestHealthzBagimliliktanEtkilenmez(t *testing.T) {
	s, mux := newTestServer(t)
	s.AddCheck("scylladb", func(context.Context) error {
		return errors.New("baglanti reddedildi")
	})

	code, body := do(t, mux, "/healthz")
	if code != http.StatusOK {
		t.Errorf("/healthz kodu %d, 200 bekleniyordu", code)
	}
	if body.Status != "ok" {
		t.Errorf("status %q, \"ok\" bekleniyordu", body.Status)
	}
}

func TestReadyzKontrolsuzHazir(t *testing.T) {
	_, mux := newTestServer(t)

	code, body := do(t, mux, "/readyz")
	if code != http.StatusOK {
		t.Errorf("/readyz kodu %d, 200 bekleniyordu", code)
	}
	if body.Status != "ready" {
		t.Errorf("status %q, \"ready\" bekleniyordu", body.Status)
	}
}

func TestReadyzGecenKontrol(t *testing.T) {
	s, mux := newTestServer(t)
	s.AddCheck("scylladb", func(context.Context) error { return nil })

	code, body := do(t, mux, "/readyz")
	if code != http.StatusOK {
		t.Errorf("kod %d, 200 bekleniyordu", code)
	}
	if body.Checks["scylladb"] != "ok" {
		t.Errorf("scylladb kontrolu %q, \"ok\" bekleniyordu", body.Checks["scylladb"])
	}
}

func TestReadyzBasarisizKontrol(t *testing.T) {
	s, mux := newTestServer(t)
	s.AddCheck("scylladb", func(context.Context) error { return nil })
	s.AddCheck("kafka", func(context.Context) error {
		return errors.New("broker erisilemez")
	})

	code, body := do(t, mux, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Errorf("kod %d, 503 bekleniyordu", code)
	}
	if body.Status != "not ready" {
		t.Errorf("status %q, \"not ready\" bekleniyordu", body.Status)
	}
	if body.Checks["kafka"] != "broker erisilemez" {
		t.Errorf("kafka kontrolu %q, hata mesajini icermeliydi", body.Checks["kafka"])
	}
	// Gecen kontrol de raporlanmali; hangisinin bozuldugu gorulebilmeli.
	if body.Checks["scylladb"] != "ok" {
		t.Errorf("scylladb kontrolu %q, \"ok\" bekleniyordu", body.Checks["scylladb"])
	}
}
