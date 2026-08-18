package server

import (
	"log"
	"os"
	"strings"
	"time"
)

type Config struct {
	Addr              string
	AllowedUsers      map[string]struct{}
	JobInterval       time.Duration
	JobHistoryLimit   int
	NonceHistoryLimit int
	SubmitMinInterval time.Duration
	Logger            *log.Logger
	Now               func() time.Time
	NonceBytes        int
}

// DefaultConfig keeps the challenge defaults in one place. I wanted the CLI to
// stay light while the server still has sensible values for a quick local run.
func DefaultConfig() Config {
	return Config{
		Addr:              ":4000",
		AllowedUsers:      ParseAllowedUsers("admin"),
		JobInterval:       30 * time.Second,
		JobHistoryLimit:   64,
		NonceHistoryLimit: 10000,
		SubmitMinInterval: time.Second,
		Logger:            log.New(os.Stdout, "server ", log.LstdFlags|log.Lmicroseconds),
		Now:               time.Now,
		NonceBytes:        16,
	}
}

// ParseAllowedUsers turns a comma-separated value into a set. The server only
// asks "is this username allowed?", so a set is the simplest shape.
func ParseAllowedUsers(raw string) map[string]struct{} {
	users := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		username := strings.TrimSpace(part)
		if username == "" {
			continue
		}
		users[username] = struct{}{}
	}
	return users
}

// withDefaults lets callers override just the knobs they care about without
// rebuilding the whole config by hand.
func (c Config) withDefaults() Config {
	defaults := DefaultConfig()
	if c.Addr == "" {
		c.Addr = defaults.Addr
	}
	if len(c.AllowedUsers) == 0 {
		c.AllowedUsers = defaults.AllowedUsers
	}
	if c.JobInterval <= 0 {
		c.JobInterval = defaults.JobInterval
	}
	if c.JobHistoryLimit <= 0 {
		c.JobHistoryLimit = defaults.JobHistoryLimit
	}
	if c.NonceHistoryLimit <= 0 {
		c.NonceHistoryLimit = defaults.NonceHistoryLimit
	}
	if c.SubmitMinInterval <= 0 {
		c.SubmitMinInterval = defaults.SubmitMinInterval
	}
	if c.Logger == nil {
		c.Logger = defaults.Logger
	}
	if c.Now == nil {
		c.Now = defaults.Now
	}
	if c.NonceBytes <= 0 {
		c.NonceBytes = defaults.NonceBytes
	}
	return c
}
