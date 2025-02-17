package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"sync"
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

		var wg sync.WaitGroup
		wg.Add(1)

		// Test Open a stream
		go func() {
			defer wg.Done()
			stream, err := conn.OpenStream()
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			buffer := []byte("Hello")
			_, err = stream.Write(buffer)
			println("written")
			if err != nil && err != io.EOF {
				panic(err.Error())
			}

			<-time.After(1 * time.Second)

		}()

		wg.Wait()
	}()

	<-ctx.Done()
}
