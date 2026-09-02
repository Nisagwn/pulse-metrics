package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/nisah/pulse-metrics/internal/agent"
)

func main() {
	// CLI flags
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka broker address")
	serviceName := flag.String("service", "unknown-service", "Service name for this agent")
	instanceID := flag.String("instance", "default", "Instance ID (hostname/container-id)")
	interval := flag.Duration("interval", 10*time.Second, "Metrics collection interval")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	if *serviceName == "unknown-service" {
		log.Println("WARNING: serviceName not set, using default")
	}

	// Initialize agent
	cfg := &agent.Config{
		ServiceName:   *serviceName,
		InstanceID:    *instanceID,
		KafkaBrokers:  []string{*kafkaAddr},
		CollectInterval: *interval,
		Debug:         *debug,
	}

	a, err := agent.NewAgent(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Starting APM agent for service=%s instance=%s", *serviceName, *instanceID)

	// Start collecting metrics
	if err := a.Start(ctx); err != nil {
		log.Fatalf("Agent error: %v", err)
	}
}
