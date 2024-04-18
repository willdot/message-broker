package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/willdot/messagebroker/internal/server"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		slog.Info("PORT env not set, using default", "default", "3000")
		port = "3000"
	}
	slog.Info("using port", "port", port)
	srv, err := server.New(fmt.Sprintf("0.0.0.0:%s", port), time.Second, time.Second*2)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = srv.Shutdown()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	<-signals
}
