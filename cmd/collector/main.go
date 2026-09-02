package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nisah/pulse-metrics/internal/collector"
)

func main() {
	// CLI flags
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka broker address")
	scyllaAddr := flag.String("scylla", "localhost:9042", "ScyllaDB address")
	port := flag.String("port", "50051", "gRPC server port")
	healthAddr := flag.String("health", ":8082", "Health check HTTP address")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	cfg := &collector.Config{
		KafkaBrokers: []string{*kafkaAddr},
		ScyllaAddr:   *scyllaAddr,
		GRPCPort:     *port,
		HealthAddr:   *healthAddr,
		Debug:        *debug,
	}

	c, err := collector.NewCollector(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize collector: %v", err)
	}

	// Ctrl+C ve SIGTERM: Kafka offset'lerinin commit edilmesi ve ScyllaDB
	// oturumunun duzgun kapanmasi icin sart.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("Starting metrics collector (gRPC :%s, health %s)", *port, *healthAddr)

	if err := c.Start(ctx); err != nil {
		log.Fatalf("Collector error: %v", err)
	}

	log.Println("Collector shut down cleanly")
}
