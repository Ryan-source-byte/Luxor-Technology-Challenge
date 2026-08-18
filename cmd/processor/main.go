package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"luxor-challenge/internal/queue"
	"luxor-challenge/internal/stats"
)

func main() {
	// This process is the bonus async worker. I kept it separate so the TCP
	// server can focus on connections and validation while this part handles DB
	// writes from RabbitMQ events.
	databaseURL := flag.String("database-url", env("DATABASE_URL", ""), "PostgreSQL connection URL")
	amqpURL := flag.String("amqp-url", env("AMQP_URL", "amqp://guest:guest@localhost:5672/"), "RabbitMQ AMQP URL")
	queueName := flag.String("queue", env("QUEUE_NAME", "luxor.submissions"), "RabbitMQ queue name")
	flag.Parse()

	logger := log.New(os.Stdout, "processor ", log.LstdFlags|log.Lmicroseconds)
	if *databaseURL == "" {
		logger.Fatal("DATABASE_URL is required")
	}

	// Let Ctrl+C flow through the context so the consumer can stop calmly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The consumer only acks after the recorder succeeds, so I set up Postgres
	// first and only then start reading from RabbitMQ.
	recorder, err := stats.NewPostgresRecorder(ctx, *databaseURL)
	if err != nil {
		logger.Fatalf("postgres setup failed: %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = recorder.Close(closeCtx)
	}()

	consumer, err := queue.NewConsumer(*amqpURL, *queueName)
	if err != nil {
		logger.Fatalf("queue setup failed: %v", err)
	}
	defer consumer.Close()

	logger.Printf("consuming queue %s", *queueName)
	if err := consumer.Run(ctx, recorder); err != nil && ctx.Err() == nil {
		logger.Fatalf("processor stopped: %v", err)
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
