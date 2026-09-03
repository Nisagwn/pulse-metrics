package tracing

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizePath(t *testing.T) {
	cases := map[string]string{
		// Degismemesi gerekenler: sabit yol parcalari.
		"/":                    "/",
		"/orders":              "/orders",
		"/api/v1/orders":       "/api/v1/orders",
		"/health":              "/health",
		"/orders/latest/items": "/orders/latest/items",

		// Saf sayi: en yaygin kimlik bicimi.
		"/orders/12345":       "/orders/{id}",
		"/orders/12345/items": "/orders/{id}/items",
		"/users/1/posts/2":    "/users/{id}/posts/{id}",

		// UUID.
		"/users/8f14e45f-ceea-467a-9c6a-1b2c3d4e5f60":         "/users/{uuid}",
		"/users/8F14E45F-CEEA-467A-9C6A-1B2C3D4E5F60/profile": "/users/{uuid}/profile",

		// Uzun onaltilik dizi: karma, jeton, icerik kimligi.
		"/files/a3f9c2b18d4e5f60": "/files/{hex}",

		// Karisik kimlik: yeterince rakam ve yeterince uzun.
		"/orders/order-2024-8891": "/orders/{id}",
	}

	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			if got := NormalizePath(in); got != want {
				t.Errorf("NormalizePath(%q) = %q, %q bekleniyordu", in, got, want)
			}
		})
	}
}

// Bu testin varlik sebebi: normallestirme olmadan her siparis kimligi
// ayri bir operasyon adi uretir. Kardinalite patlamasi, izleme
// sistemlerinin en yaygin oldurucu hatasidir.
func TestNormalizePathKardinaliteyiKirar(t *testing.T) {
	exp := NewMemoryExporter()
	tracer := NewTracer("test-svc", exp)

	h := tracer.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 100; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/orders/%d", i), nil)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	names := map[string]bool{}
	for _, span := range exp.Spans() {
		names[span.OperationName] = true
	}
	if len(names) != 1 {
		t.Fatalf("100 farkli yol tek operasyon adina inmeliydi, %d ad olustu: %v", len(names), names)
	}
	if !names["GET /orders/{id}"] {
		t.Errorf("beklenen ad yok: %v", names)
	}
}

// Tavan: normallestirme yetmediginde bile operasyon sayisi sinirli kalmali.
func TestOperasyonTavani(t *testing.T) {
	exp := NewMemoryExporter()
	tracer := NewTracer("test-svc", exp)

	// Normallestirmeyi bilerek devre disi birakip her istege benzersiz
	// bir ad veriyoruz: tavanin tek basina calistigini gormek icin.
	var n int
	h := tracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		WithOperationName(func(r *http.Request) string {
			n++
			return fmt.Sprintf("GET /essiz-%d", n)
		}),
		WithMaxOperations(5),
	)

	for i := 0; i < 50; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
	}

	names := map[string]int{}
	for _, span := range exp.Spans() {
		names[span.OperationName]++
	}

	// 5 essiz ad + tasma kovasi = en fazla 6.
	if len(names) > 6 {
		t.Errorf("tavan asildi: %d farkli ad", len(names))
	}
	if names["GET /other"] == 0 {
		t.Errorf("tasan adlar /other altinda toplanmaliydi: %v", names)
	}
}

func TestWithOperationNameYonlendiriciSablonu(t *testing.T) {
	exp := NewMemoryExporter()
	tracer := NewTracer("test-svc", exp)

	// Gercek dunyada bu deger yonlendiriciden gelir (chi RoutePattern gibi).
	h := tracer.Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		WithOperationName(func(r *http.Request) string { return "GET /orders/:id" }),
	)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders/99", nil))

	spans := exp.Spans()
	if len(spans) != 1 {
		t.Fatalf("1 span bekleniyordu, %d", len(spans))
	}
	if spans[0].OperationName != "GET /orders/:id" {
		t.Errorf("operasyon adi = %q", spans[0].OperationName)
	}
	// Ham yol oznitelikte korunmali: tekil bir istegi incelerken
	// gercek /orders/99'u gormek gerekir.
	if spans[0].Attributes["http.target"] != "/orders/99" {
		t.Errorf("http.target = %q, ham yol korunmaliydi", spans[0].Attributes["http.target"])
	}
}
