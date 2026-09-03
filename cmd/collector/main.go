package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nisah/pulse-metrics/internal/buildinfo"
	"github.com/nisah/pulse-metrics/internal/collector"
	"github.com/nisah/pulse-metrics/internal/config"
)

func main() {
	// Bayraklarin varsayilanlari ortam degiskenlerinden geliyor; boylece
	// oncelik sirasi dogru oluyor: bayrak > ortam > sabit varsayilan.
	// Konteynerde ortam degiskeni kullanilir, elle calistirirken bayrak.
	kafkaAddr := flag.String("kafka", config.Env("PULSE_KAFKA_BROKERS", "localhost:9092"),
		"Kafka broker adresleri (virgulle)")
	scyllaAddr := flag.String("scylla", config.Env("PULSE_SCYLLA_HOSTS", "localhost:9042"),
		"ScyllaDB adresleri (virgulle)")
	port := flag.String("port", config.Env("PULSE_GRPC_PORT", "50051"), "gRPC sunucu portu")
	healthAddr := flag.String("health", config.Env("PULSE_HEALTH_ADDR", ":8082"),
		"Saglik/olcum HTTP adresi (/healthz, /readyz, /metrics)")
	instance := flag.String("instance", config.Env("PULSE_INSTANCE_ID", ""),
		"Bu collector'in adi (bos ise hostname)")
	groupID := flag.String("group", config.Env("PULSE_CONSUMER_GROUP", ""),
		"Kafka consumer group (bos ise varsayilan)")
	debug := flag.Bool("debug", config.EnvBool("PULSE_DEBUG", false), "Ayrintili log")
	showVersion := flag.Bool("version", false, "Surumu yaz ve cik")

	flag.Parse()

	if *showVersion {
		fmt.Println("pulse-collector", buildinfo.Get().String())
		return
	}

	scylla := config.ScyllaFromEnv(*scyllaAddr)
	// Bayrak varsayilani zaten PULSE_SCYLLA_HOSTS'tan geliyordu; burada
	// virgullu listeyi ayirip host dizisine ceviriyoruz.
	scylla.Hosts = splitCSV(*scyllaAddr)
	if err := scylla.Validate(); err != nil {
		// Yanlis ayarla acilmaktansa hic acilmamak dogru: yanlis ayarla
		// acilan bir surec orkestratore "saglikliyim" der ve sorun
		// gorunmez kalir.
		log.Fatalf("Ayar hatasi: %v", err)
	}

	cfg := &collector.Config{
		KafkaBrokers: splitCSV(*kafkaAddr),
		ScyllaAddr:   scylla.Hosts[0],
		Scylla:       &scylla,
		GRPCPort:     *port,
		HealthAddr:   *healthAddr,
		InstanceID:   config.InstanceID(*instance),
		GroupID:      *groupID,
		Debug:        *debug,
	}

	c, err := collector.NewCollector(cfg)
	if err != nil {
		log.Fatalf("Collector baslatilamadi: %v", err)
	}

	// Ctrl+C ve SIGTERM: Kafka offset'lerinin commit edilmesi ve ScyllaDB
	// oturumunun duzgun kapanmasi icin sart.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("pulse-collector %s | instance=%s gRPC :%s health %s",
		buildinfo.Get().String(), c.InstanceID(), *port, *healthAddr)

	if err := c.Start(ctx); err != nil {
		log.Fatalf("Collector hatasi: %v", err)
	}

	log.Println("Collector duzgunce kapandi")
}

// splitCSV: "a:1, b:2" -> ["a:1", "b:2"]
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
