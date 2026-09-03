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
	"time"

	"github.com/nisah/pulse-metrics/internal/agent"
	"github.com/nisah/pulse-metrics/internal/buildinfo"
	"github.com/nisah/pulse-metrics/internal/config"
	"github.com/nisah/pulse-metrics/internal/obs"
)

func main() {
	// Bayrak varsayilanlari ortamdan geliyor: bayrak > ortam > sabit.
	kafkaAddr := flag.String("kafka", config.Env("PULSE_KAFKA_BROKERS", "localhost:9092"),
		"Kafka broker adresleri (virgulle)")
	serviceName := flag.String("service", config.Env("PULSE_SERVICE_NAME", "unknown-service"),
		"Bu ajanin izledigi servisin adi")
	instanceID := flag.String("instance", config.Env("PULSE_INSTANCE_ID", ""),
		"Instance kimligi (bos ise hostname)")
	interval := flag.Duration("interval", config.EnvDuration("PULSE_COLLECT_INTERVAL", 10*time.Second),
		"Olcum toplama araligi")
	healthAddr := flag.String("health", config.Env("PULSE_HEALTH_ADDR", ":8081"),
		"Saglik/olcum HTTP adresi (/healthz, /readyz, /metrics)")
	debug := flag.Bool("debug", config.EnvBool("PULSE_DEBUG", false), "Ayrintili log")
	showVersion := flag.Bool("version", false, "Surumu yaz ve cik")

	flag.Parse()

	if *showVersion {
		fmt.Println("pulse-agent", buildinfo.Get().String())
		return
	}

	if *serviceName == "unknown-service" {
		log.Println("UYARI: -service verilmedi, varsayilan kullaniliyor")
	}

	// instance_id veritabaninda clustering key'in parcasi: bos birakmak
	// ayni servisin iki kopyasinin birbirini ezmesine yol acardi.
	// Verilmediyse hostname makul bir varsayilan.
	*instanceID = config.InstanceID(*instanceID)
	obs.SetBuildInfo("agent", *instanceID)

	cfg := &agent.Config{
		ServiceName:     *serviceName,
		InstanceID:      *instanceID,
		KafkaBrokers:    splitCSV(*kafkaAddr),
		CollectInterval: *interval,
		HealthAddr:      *healthAddr,
		Debug:           *debug,
	}

	a, err := agent.NewAgent(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize agent: %v", err)
	}

	// Ctrl+C ve SIGTERM'i yakala. Bu olmadan isletim sistemi sureci dogrudan
	// oldurur ve Close() icindeki flush kodu hic calismaz - Kafka tamponunda
	// bekleyen son olcumler kaybolurdu.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("pulse-agent %s | service=%s instance=%s health %s",
		buildinfo.Get().String(), *serviceName, *instanceID, *healthAddr)

	if err := a.Start(ctx); err != nil {
		log.Fatalf("Agent error: %v", err)
	}

	log.Println("Agent shut down cleanly")
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
