package main

import (
	"context"
	"flag"
	"log"

	"github.com/nisah/pulse-metrics/internal/collector"
)

func main() {
	// CLI flags
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka broker address")
	scyllaAddr := flag.String("scylla", "localhost:9042", "ScyllaDB address")
	port := flag.String("port", "50051", "gRPC server port")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	// Initialize collector
	cfg := &collector.Config{
		KafkaBrokers: []string{*kafkaAddr},
		ScyllaAddr:   *scyllaAddr,
		GRPCPort:     *port,
		Debug:        *debug,
	}

	c, err := collector.NewCollector(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize collector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Starting metrics collector on gRPC port %s", *port)

	if err := c.Start(ctx); err != nil {
		log.Fatalf("Collector error: %v", err)
	}
}
