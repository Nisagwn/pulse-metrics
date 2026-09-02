// demo: PulseMetrics'i gorebilmek icin sahte bir mikroservis mimarisi.
//
// Tek binary icinde dort HTTP servisi ayaga kaldirir ve birbirlerini
// cagirtir:
//
//	gateway  ──▶ orders ──┬──▶ payments
//	                      └──▶ inventory ──▶ (yapay veritabani gecikmesi)
//
// Her servis kendi tracer'ina sahip ve kendi span'lerini uretiyor; aralarindaki
// baglanti tamamen W3C traceparent basligiyla kuruluyor. Yani bu dort servis
// ayri makinelerde de olsa trace yine birlesirdi.
//
// Dahili bir yuk ureteci gateway'e surekli istek atar, boylece panelde
// bakacak veri olur. Isteklerin bir kismi bilerek hata verir ve bir kismi
// yavastir - trace goruntuleyicinin ise yaradigi durumlar bunlar.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nisah/pulse-metrics/internal/logging"
	pb "github.com/nisah/pulse-metrics/internal/proto"
	"github.com/nisah/pulse-metrics/internal/tracing"
)

type service struct {
	name   string
	addr   string
	tracer *tracing.Tracer
	log    *logging.Logger
	client *http.Client
	server *http.Server
}

func newService(name, addr, kafkaAddr string, sampleRatio float64) *service {
	exporter := tracing.NewBatchExporter(tracing.BatchExporterConfig{
		KafkaBrokers:  []string{kafkaAddr},
		ServiceName:   name,
		InstanceID:    "demo-1",
		BatchSize:     32,
		FlushInterval: time.Second,
	})

	tracer := tracing.NewTracer(name, exporter,
		tracing.WithInstanceID("demo-1"),
		tracing.WithSampler(tracing.NewRatioSampler(sampleRatio)),
	)

	// Logger: context'teki aktif span'den trace_id/span_id otomatik alinir.
	logger := logging.New(logging.Config{
		KafkaBrokers:  []string{kafkaAddr},
		ServiceName:   name,
		InstanceID:    "demo-1",
		LoggerName:    name,
		MinLevel:      pb.LogLevel_LEVEL_INFO,
		BatchSize:     32,
		FlushInterval: time.Second,
	})

	return &service{
		name:   name,
		addr:   addr,
		tracer: tracer,
		log:    logger,
		client: tracer.Client(), // giden cagrilar otomatik enstrumante
	}
}

func (s *service) serve(mux *http.ServeMux) {
	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           s.tracer.Middleware(mux), // gelen istekler otomatik
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[%s] sunucu hatasi: %v", s.name, err)
		}
	}()
}

func (s *service) shutdown(ctx context.Context) {
	if s.server != nil {
		_ = s.server.Shutdown(ctx)
	}
	_ = s.tracer.Shutdown(ctx)
	if s.log != nil {
		_ = s.log.Shutdown(ctx)
	}
}

// get: baska bir servise enstrumante edilmis cagri.
// ctx'i tasimak sart - ebeveyn span oradan bulunuyor.
func (s *service) get(ctx context.Context, url string) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s -> HTTP %d", url, resp.StatusCode)
	}
	var out map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out, nil
}

func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// jitter: gercekci gorunen degisken gecikme.
func jitter(base, spread time.Duration) {
	time.Sleep(base + time.Duration(rand.Int63n(int64(spread)+1)))
}

func main() {
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka broker address")
	rps := flag.Int("rps", 3, "Yuk ureteci: saniyedeki istek sayisi")
	sampleRatio := flag.Float64("sample", 1.0, "Orneklem orani (0.0 - 1.0)")
	flag.Parse()

	base := 9101
	gateway := newService("gateway", fmt.Sprintf(":%d", base), *kafkaAddr, *sampleRatio)
	orders := newService("orders", fmt.Sprintf(":%d", base+1), *kafkaAddr, *sampleRatio)
	payments := newService("payments", fmt.Sprintf(":%d", base+2), *kafkaAddr, *sampleRatio)
	inventory := newService("inventory", fmt.Sprintf(":%d", base+3), *kafkaAddr, *sampleRatio)

	ordersURL := fmt.Sprintf("http://localhost:%d", base+1)
	paymentsURL := fmt.Sprintf("http://localhost:%d", base+2)
	inventoryURL := fmt.Sprintf("http://localhost:%d", base+3)

	// --- gateway: kullaniciya bakan uc ---
	gwMux := http.NewServeMux()
	gwMux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Handler kendi ic adimini da isaretleyebilir.
		ctx, span := gateway.tracer.Start(ctx, "validate-request")
		jitter(2*time.Millisecond, 4*time.Millisecond)
		span.SetAttribute("user.tier", []string{"free", "pro"}[rand.Intn(2)])
		span.End()

		gateway.log.Info(ctx, "checkout istegi alindi", map[string]string{
			"path": r.URL.Path,
		})

		result, err := gateway.get(ctx, ordersURL+"/orders")
		if err != nil {
			tracing.SpanFromContext(r.Context()).RecordError(err)
			gateway.log.Error(ctx, "checkout basarisiz", err, nil)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	gateway.serve(gwMux)

	// --- orders: payments ve inventory'yi cagirir ---
	ordMux := http.NewServeMux()
	ordMux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		jitter(3*time.Millisecond, 8*time.Millisecond)

		// Iki asagi servisi PARALEL cagiriyoruz. Trace goruntuleyicide
		// bu iki span'in yan yana (ust uste binen) gorunmesi gerekir -
		// serilestirilmis bir cagriyla arasindaki fark tam da burada gorulur.
		var (
			wg             sync.WaitGroup
			payErr, invErr error
			payRes, invRes map[string]interface{}
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			payRes, payErr = orders.get(ctx, paymentsURL+"/charge")
		}()
		go func() {
			defer wg.Done()
			invRes, invErr = orders.get(ctx, inventoryURL+"/reserve")
		}()
		wg.Wait()

		if payErr != nil {
			tracing.SpanFromContext(ctx).RecordError(payErr)
			orders.log.Error(ctx, "odeme adimi basarisiz", payErr,
				map[string]string{"step": "payments"})
			writeJSON(w, http.StatusPaymentRequired, map[string]string{"error": payErr.Error()})
			return
		}
		if invErr != nil {
			tracing.SpanFromContext(ctx).RecordError(invErr)
			orders.log.Error(ctx, "stok adimi basarisiz", invErr,
				map[string]string{"step": "inventory"})
			writeJSON(w, http.StatusConflict, map[string]string{"error": invErr.Error()})
			return
		}

		orderID := fmt.Sprintf("ord-%06d", rand.Intn(999999))
		orders.log.Info(ctx, "siparis olusturuldu "+orderID,
			map[string]string{"order_id": orderID})

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"order_id":  orderID,
			"payment":   payRes,
			"inventory": invRes,
		})
	})
	orders.serve(ordMux)

	// --- payments: bazen yavas, bazen hatali ---
	payMux := http.NewServeMux()
	payMux.HandleFunc("/charge", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		span := tracing.SpanFromContext(ctx)

		// %8 ihtimalle odeme saglayicisi yavas - "kuyruk gecikmesi".
		if rand.Float64() < 0.08 {
			span.AddEvent("provider.slow", map[string]string{"provider": "stripe-sim"})
			payments.log.Warn(ctx, "odeme saglayicisi yavas yanit veriyor",
				map[string]string{"provider": "stripe-sim"})
			jitter(180*time.Millisecond, 220*time.Millisecond)
		} else {
			jitter(8*time.Millisecond, 20*time.Millisecond)
		}

		// %6 ihtimalle kart reddedildi.
		if rand.Float64() < 0.06 {
			span.SetAttribute("payment.decline_code", "insufficient_funds")
			payments.log.Error(ctx, fmt.Sprintf("kart reddedildi: yetersiz bakiye, tutar %.2f",
				10+rand.Float64()*200), nil,
				map[string]string{"decline_code": "insufficient_funds"})
			writeJSON(w, http.StatusInternalServerError,
				map[string]string{"error": "kart reddedildi"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"status": "charged",
			"amount": fmt.Sprintf("%.2f", 10+rand.Float64()*200),
		})
	})
	payments.serve(payMux)

	// --- inventory: icinde bir "veritabani" adimi var ---
	invMux := http.NewServeMux()
	invMux.HandleFunc("/reserve", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Ic span: veritabani sorgusu. Gercek bir sistemde bunu
		// veritabani surucusu otomatik uretirdi.
		_, dbSpan := inventory.tracer.Start(ctx, "SELECT stock",
			tracing.WithSpanKind(pb.SpanKind_SPAN_KIND_CLIENT),
			tracing.WithAttributes(map[string]string{
				"db.system":    "scylladb",
				"db.operation": "SELECT",
				"db.statement": "SELECT qty FROM stock WHERE sku = ?",
			}),
		)
		jitter(4*time.Millisecond, 12*time.Millisecond)
		dbSpan.End()

		if rand.Float64() < 0.04 {
			sku := fmt.Sprintf("SKU-%04d", rand.Intn(9999))
			tracing.SpanFromContext(ctx).SetAttribute("inventory.sku", sku)
			inventory.log.Error(ctx, "stok yetersiz: "+sku, nil,
				map[string]string{"sku": sku})
			writeJSON(w, http.StatusInternalServerError,
				map[string]string{"error": "stok yetersiz"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"reserved": true,
			"sku":      fmt.Sprintf("SKU-%04d", rand.Intn(9999)),
		})
	})
	inventory.serve(invMux)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("Demo mimarisi ayakta:")
	log.Printf("  gateway   :%d  /checkout", base)
	log.Printf("  orders    :%d  /orders", base+1)
	log.Printf("  payments  :%d  /charge", base+2)
	log.Printf("  inventory :%d  /reserve", base+3)
	log.Printf("Yuk ureteci: %d istek/sn, orneklem %.0f%%", *rps, *sampleRatio*100)
	log.Printf("Trace'leri gormek icin: http://localhost:8080")

	// --- yuk ureteci ---
	interval := time.Second / time.Duration(max(1, *rps))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	plainClient := &http.Client{Timeout: 10 * time.Second}
	gatewayURL := fmt.Sprintf("http://localhost:%d/checkout", base)

	var sent, failed int
	logTicker := time.NewTicker(15 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Kapaniyor, bekleyen span'ler gonderiliyor...")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, s := range []*service{gateway, orders, payments, inventory} {
				s.shutdown(shutdownCtx)
			}
			log.Println("Demo temiz kapandi")
			return

		case <-logTicker.C:
			log.Printf("Gonderilen istek: %d (hatali: %d)", sent, failed)

		case <-ticker.C:
			go func() {
				// Yuk ureteci bilerek enstrumante EDILMEMIS bir istemci
				// kullaniyor: gercek bir kullanicinin tarayicisi gibi.
				// Trace, gateway'in middleware'inde basliyor.
				resp, err := plainClient.Get(gatewayURL)
				sent++
				if err != nil {
					failed++
					return
				}
				if resp.StatusCode >= 400 {
					failed++
				}
				_ = resp.Body.Close()
			}()
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
