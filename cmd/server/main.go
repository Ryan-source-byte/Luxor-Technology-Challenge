package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"luxor-challenge/internal/queue"
	"luxor-challenge/internal/server"
	"luxor-challenge/internal/stats"
)

func main() {
	addr := flag.String("addr", env("SERVER_ADDR", ":4000"), "TCP listen address")
	databaseURL := flag.String("database-url", env("DATABASE_URL", ""), "PostgreSQL connection URL")
	amqpURL := flag.String("amqp-url", env("AMQP_URL", ""), "RabbitMQ AMQP URL")
	queueName := flag.String("queue", env("QUEUE_NAME", "luxor.submissions"), "RabbitMQ queue name")
	statsMode := flag.String("stats-mode", env("STATS_MODE", "direct"), "statistics mode: direct, queue, noop")
	allowedUsers := flag.String("allowed-users", env("ALLOWED_USERS", "admin"), "comma-separated allowed usernames")
	jobInterval := flag.Duration("job-interval", durationEnv("JOB_INTERVAL", 30*time.Second), "job refresh interval")
	flag.Parse()

	logger := log.New(os.Stdout, "server ", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	recorder, err := buildRecorder(ctx, *statsMode, *databaseURL, *amqpURL, *queueName, logger)
	if err != nil {
		logger.Fatalf("stats setup failed: %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = recorder.Close(closeCtx)
	}()

	cfg := server.DefaultConfig()
	cfg.Addr = *addr
	cfg.AllowedUsers = server.ParseAllowedUsers(*allowedUsers)
	cfg.JobInterval = *jobInterval
	cfg.Logger = logger

	srv := server.New(cfg, recorder)
	logger.Printf("listening on %s", *addr)
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Fatalf("server stopped: %v", err)
	}
}

func buildRecorder(ctx context.Context, mode, databaseURL, amqpURL, queueName string, logger *log.Logger) (stats.Recorder, error) {
	switch mode {
	case "direct":
		if databaseURL == "" {
			logger.Printf("DATABASE_URL is empty; using noop stats recorder for protocol-only runs")
			return stats.NoopRecorder{}, nil
		}
		return stats.NewPostgresRecorder(ctx, databaseURL)
	case "queue":
		if amqpURL == "" {
			return nil, fmt.Errorf("AMQP_URL is required in queue mode")
		}
		return queue.NewPublisher(amqpURL, queueName)
	case "noop":
		return stats.NoopRecorder{}, nil
	default:
		return nil, fmt.Errorf("unknown stats mode %q", mode)
	}
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
