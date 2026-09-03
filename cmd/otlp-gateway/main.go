// otlp-gateway: OpenTelemetry Protocol alicisi.
//
// PulseMetrics'in disa acilan kapisi. Herhangi bir dilin resmi
// OpenTelemetry SDK'si dogrudan buraya veri gonderebilir; gateway onu
// PulseMetrics tiplerine cevirip Kafka'ya yazar.
//
// Standart OTLP portlarini dinler:
//
//	4317  gRPC
//	4318  HTTP  (/v1/traces, /v1/metrics, /v1/logs)
//
// Bir Python uygulamasindan veri gondermek icin gereken tek sey:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
//	OTEL_SERVICE_NAME=odeme-servisi \
//	opentelemetry-instrument python app.py
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/nisah/pulse-metrics/internal/buildinfo"
	"github.com/nisah/pulse-metrics/internal/collector"
	"github.com/nisah/pulse-metrics/internal/config"
	"github.com/nisah/pulse-metrics/internal/health"
	"github.com/nisah/pulse-metrics/internal/obs"
	"github.com/nisah/pulse-metrics/internal/otlp"
)

func main() {
	kafkaAddr := flag.String("kafka", config.Env("PULSE_KAFKA_BROKERS", "localhost:9092"),
		"Kafka broker adresleri (virgulle)")
	grpcAddr := flag.String("grpc", config.Env("PULSE_OTLP_GRPC_ADDR", ":4317"),
		"OTLP/gRPC dinleme adresi")
	httpAddr := flag.String("http", config.Env("PULSE_OTLP_HTTP_ADDR", ":4318"),
		"OTLP/HTTP dinleme adresi")
	healthAddr := flag.String("health", config.Env("PULSE_HEALTH_ADDR", ":8085"),
		"Saglik/olcum HTTP adresi (/healthz, /readyz, /metrics)")
	instance := flag.String("instance", config.Env("PULSE_INSTANCE_ID", ""),
		"Bu gateway'in adi (bos ise hostname)")
	showVersion := flag.Bool("version", false, "Surumu yaz ve cik")
	flag.Parse()

	if *showVersion {
		fmt.Println("pulse-otlp-gateway", buildinfo.Get().String())
		return
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("logger olusturulamadi: %v", err)
	}
	defer func() { _ = logger.Sync() }()

	instanceID := config.InstanceID(*instance)
	obs.SetBuildInfo("otlp-gateway", instanceID)

	brokers := splitCSV(*kafkaAddr)
	receiver := otlp.NewReceiver(otlp.Config{
		KafkaBrokers: brokers,
		TracesTopic:  config.Env("PULSE_TRACES_TOPIC", collector.DefaultTracesTopic),
		MetricsTopic: config.Env("PULSE_METRICS_TOPIC", collector.DefaultTopic),
		LogsTopic:    config.Env("PULSE_LOGS_TOPIC", collector.DefaultLogsTopic),
		Logger:       logger,
	})
	defer func() { _ = receiver.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("pulse-otlp-gateway %s | instance=%s gRPC %s HTTP %s health %s",
		buildinfo.Get().String(), instanceID, *grpcAddr, *httpAddr, *healthAddr)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 3)
	var wg sync.WaitGroup

	// --- OTLP/gRPC ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- serveGRPC(ctx, *grpcAddr, receiver, logger)
	}()

	// --- OTLP/HTTP ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- serveHTTP(ctx, *httpAddr, receiver, logger)
	}()

	// --- saglik ve olcum ---
	hs := health.New(*healthAddr, logger)
	hs.AddCheck("kafka", func(ctx context.Context) error {
		// Broker'a TCP baglantisi kurulabiliyor mu? Ucuz ve yeterli:
		// Kafka erisilemezse gateway hicbir isteği kabul edemez, yani
		// trafigi kesmek dogru davranis.
		d := net.Dialer{Timeout: 3 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", brokers[0])
		if err != nil {
			return err
		}
		return conn.Close()
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- hs.Serve(ctx)
	}()

	err = <-errCh
	cancel()
	wg.Wait()

	if err != nil {
		log.Fatalf("OTLP gateway hatasi: %v", err)
	}
	log.Println("OTLP gateway duzgunce kapandi")
}

func serveGRPC(ctx context.Context, addr string, r *otlp.Receiver, logger *zap.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("OTLP/gRPC portu dinlenemedi: %w", err)
	}

	srv := grpc.NewServer(
		// OTel SDK'lari buyuk toplu gonderimler yapabiliyor; varsayilan
		// 4 MB siniri gercek yuk altinda dar kaliyor.
		grpc.MaxRecvMsgSize(16 << 20),
	)
	r.Register(srv)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	logger.Info("OTLP/gRPC dinliyor", zap.String("addr", addr))
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("OTLP/gRPC sunucusu durdu: %w", err)
	}
	return nil
}

func serveHTTP(ctx context.Context, addr string, r *otlp.Receiver, logger *zap.Logger) error {
	mux := http.NewServeMux()
	r.Handler(mux)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("OTLP/HTTP dinliyor", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		out = []string{s}
	}
	return out
}
