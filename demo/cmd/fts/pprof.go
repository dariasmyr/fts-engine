package main

import (
	"log/slog"
	"net/http"
	_ "net/http/pprof"

	"github.com/dariasmyr/fts-engine/demo/internal/lib/logger/sl"
)

func startPprofServer(log *slog.Logger) {
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Error("pprof server stopped", "error", sl.Err(err), "addr", "localhost:6060")
		}
	}()
}
