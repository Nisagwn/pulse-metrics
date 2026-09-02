package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nisah/pulse-metrics/internal/agent"
)

func main() {
	// CLI flags
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka broker address")
	serviceName := flag.String("service", "unknown-service", "Service name for this agent")
	instanceID := flag.String("instance", "", "Instance ID (bos ise hostname kullanilir)")
	interval := flag.Duration("interval", 10*time.Second, "Metrics collection interval")
	healthAddr := flag.String("health", ":8081", "Health check HTTP address")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	if *serviceName == "unknown-service" {
		log.Println("WARNING: serviceName not set, using default")
	}

	// instance_id artik veritabaninda clustering key'in parcasi: bos birakmak
	// ayni servisin iki kopyasinin birbirini ezmesine yol acardi. Verilmediyse
	// hostname makul bir varsayilan.
	if *instanceID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "default"
		}
		*instanceID = host
		log.Printf("instance not set, using hostname: %s", host)
	}

	cfg := &agent.Config{
		ServiceName:     *serviceName,
		InstanceID:      *instanceID,
		KafkaBrokers:    []string{*kafkaAddr},
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

	log.Printf("Starting APM agent for service=%s instance=%s", *serviceName, *instanceID)

	if err := a.Start(ctx); err != nil {
		log.Fatalf("Agent error: %v", err)
	}

	log.Println("Agent shut down cleanly")
}
