package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/willdot/messagebroker/server"
)

func main() {
	srv, err := server.New(":3000", time.Second, time.Second*2, server.NewMemoryStore())
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
