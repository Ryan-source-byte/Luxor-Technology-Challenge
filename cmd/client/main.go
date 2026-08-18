package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"luxor-challenge/internal/client"
)

func main() {
	// I keep main small on purpose: read config, check the obvious input, and
	// hand the real TCP work to internal/client.
	addr := flag.String("addr", env("SERVER_ADDR", "127.0.0.1:4000"), "server TCP address")
	username := flag.String("username", env("USERNAME", "admin"), "username")
	submitInterval := flag.Duration("submit-interval", durationEnv("SUBMIT_INTERVAL", time.Second), "submit interval, from 1s to 60s")
	flag.Parse()

	logger := log.New(os.Stdout, "client ", log.LstdFlags|log.Lmicroseconds)
	// The challenge gives a clear submit-rate range, so I reject bad values
	// before the client loop starts.
	if *submitInterval < time.Second || *submitInterval > time.Minute {
		logger.Fatalf("submit interval must be between 1s and 60s")
	}

	// Ctrl+C becomes a context cancellation. That feels cleaner than forcing
	// the process to exit while the client still owns a socket.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := client.New(client.Config{
		Addr:           *addr,
		Username:       *username,
		SubmitInterval: *submitInterval,
		Logger:         logger,
	})
	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Fatalf("client stopped: %v", err)
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
