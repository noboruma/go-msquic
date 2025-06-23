package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/noboruma/go-msquic/pkg/quic"
)

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	conn, err := quic.DialAddr(ctx, "127.0.0.1:9090", quic.Config{
		MaxIncomingStreams: 1000,
		MaxIdleTimeout:     5 * time.Second,
		KeepAlivePeriod:    2 * time.Second,
		Alpn:               "go-msquic-sample",
	})

	go func() {
		defer cancel()
		defer conn.Close()
		if err != nil {
			panic(err.Error())
		}
		<-time.After(5 * time.Second)

		err := conn.SendDatagram([]byte("hello"))
		if err != nil {
			println(err.Error())
		}
		<-time.After(5 * time.Second)
	}()

	<-ctx.Done()
}
