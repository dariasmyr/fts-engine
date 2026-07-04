package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
)

func ensureDir(p string) {
	_ = os.MkdirAll(p, 0755)
}

func prepareAppDirs() {
	ensureDir("data")
	ensureDir("data/fts")
}

func mustLoadConfig() (*config.Config, string) {
	configPathFlag := flag.String("config", "", "Path to the config file")
	flag.Parse()

	cfg, cfgSource, err := config.Load(*configPathFlag)
	if err != nil {
		panic(err)
	}
	return cfg, cfgSource
}

func newShutdownContext(log *slog.Logger) (context.Context, context.CancelFunc, context.Context, context.CancelFunc) {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-rootCtx.Done()
		stop()
		log.Info("Received shutdown signal, shutting down...")

		time.Sleep(_readinessDrainDelay)
		log.Info("Readiness check propagated, now waiting for ongoing processes to finish.")

		cancel()
	}()

	return rootCtx, stop, ctx, cancel
}
