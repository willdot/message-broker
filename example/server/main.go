package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/willdot/messagebroker/server"
)

func main() {
	srv, err := server.New(":3000")
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
