package main

import (
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	logger := newLogger()
	token := strings.TrimSpace(os.Getenv("BRIDGE_TOKEN"))
	bridge, err := newServer(token, newLibGMProtocol(logger), logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("invalid bridge configuration")
	}
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           bridge.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       3 * time.Minute,
		WriteTimeout:      3 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	logger.Info().Str("port", port).Msg("Google Messages bridge listening")
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Err(err).Msg("bridge stopped")
	}
}

func newLogger() zerolog.Logger {
	level, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))))
	if err != nil || level == zerolog.NoLevel {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	return zerolog.New(os.Stdout).With().Timestamp().Str("service", "google-messages-bridge").Logger()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
