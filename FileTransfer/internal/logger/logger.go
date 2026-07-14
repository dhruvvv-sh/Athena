// Package logger configures two rotating log sinks:
//
//   - the server log (stdlib log.*): all server activity + errors
//   - the transfer log: a dedicated audit trail of every file transfer
//
// Both rotate by size (with backups/age limits) via lumberjack.
package logger

import (
	"io"
	"log"
	"os"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"

	"filetransfer/internal/config"
)

var transferLog *log.Logger

// Init wires the stdlib logger to the rotating server-log file (also mirrored to stderr
// so `start.sh` captures early startup output), and opens the rotating transfer log.
// role is "master" or "worker" — it prefixes every line for multi-process clarity.
func Init(cfg *config.Config, role string) error {
	if err := os.MkdirAll(cfg.Paths.LogDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Paths.TempDir, 0o755); err != nil {
		return err
	}

	roll := func(path string) *lumberjack.Logger {
		return &lumberjack.Logger{
			Filename:   path,
			MaxSize:    cfg.Logging.MaxSizeMB,
			MaxBackups: cfg.Logging.MaxBackups,
			MaxAge:     cfg.Logging.MaxAgeDays,
			Compress:   cfg.Logging.Compress,
		}
	}

	// Server log → rotating file + stderr.
	serverW := io.MultiWriter(os.Stderr, roll(cfg.Logging.ServerFile))
	log.SetOutput(serverW)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[" + role + "] ")

	// Dedicated transfer log (no stderr mirroring; it's an audit file).
	transferLog = log.New(roll(cfg.Logging.TransferFile), "", log.LstdFlags|log.LUTC)
	return nil
}

// Transfer writes one line to the dedicated transfer audit log. Falls back to the
// server log if Init hasn't run (e.g. in tests).
func Transfer(format string, args ...any) {
	if transferLog == nil {
		log.Printf("[transfer] "+format, args...)
		return
	}
	transferLog.Printf(format, args...)
}
