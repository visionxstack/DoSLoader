package logger

import (
	"log/slog"
	"os"
)

// InitLogger configures the global slog logger.
func InitLogger(verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	// For CLI tools, a text handler is generally much more readable than a JSON handler.
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
