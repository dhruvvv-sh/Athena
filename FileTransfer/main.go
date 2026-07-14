// Command filetransfer is a single binary that runs as either a master server or a
// worker node in a highly-available, distributed file-transfer cluster.
//
//	filetransfer master --config config/config.yml
//	filetransfer worker --config config/config.yml --master http://lb.internal:8080
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"filetransfer/internal/config"
	"filetransfer/internal/master"
	"filetransfer/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	role := os.Args[1]

	// Help / unknown-role handling happens before any config is loaded.
	switch role {
	case "-h", "--help", "help":
		usage()
		return
	case "master", "worker":
		// fall through to run
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q\n\n", role)
		usage()
		os.Exit(2)
	}

	fs := flag.NewFlagSet(role, flag.ExitOnError)
	cfgPath := fs.String("config", "config/config.yml", "path to the YAML config file")
	masterURL := fs.String("master", "", "master/load-balancer base URL (worker only; overrides config)")
	_ = fs.Parse(os.Args[2:])

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *masterURL != "" {
		cfg.Worker.MasterURL = *masterURL
	}

	// Context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch role {
	case "master":
		if err := master.Run(ctx, cfg); err != nil {
			log.Fatalf("master: %v", err)
		}
	case "worker":
		if err := worker.Run(ctx, cfg); err != nil {
			log.Fatalf("worker: %v", err)
		}
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `filetransfer — distributed file-transfer cluster

Usage:
  filetransfer master [--config path]
  filetransfer worker [--config path] [--master URL]

Roles:
  master   Run the tracking UI, REST API and task scheduler.
  worker   Claim transfer tasks and move files in chunks.
`)
}
