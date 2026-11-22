package logging

import (
	"context"
	"log/slog"
	"os"
)

// contextKey is the type for context values.
type contextKey string

const correlationKey contextKey = "correlation_id"

// WithCorrelation adds a correlation ID to the context.
func WithCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

// CorrelationFrom extracts the correlation ID from the context.
func CorrelationFrom(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}

// New creates a new slog.Logger with the given level, format, and output.
func New(level, format, output string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	var handler slog.Handler
	w := os.Stdout
	switch output {
	case "stderr":
		w = os.Stderr
	}
	if format == "text" {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}
	return slog.New(handler)
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
