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

		// Test Open a stream
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := conn.OpenStream()
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			bs := make([]byte, 512*1024)
			pattern := []byte("Hello")
			for i := 0; i < len(bs); i++ {
				bs[i] = pattern[i%len(pattern)]
			}
			n, err := stream.Write(bs)
			if err != nil && err != io.EOF {
				panic(err.Error())
			}

			println("[Client] Hello sent:", n)

			totalN := 0
			stream.SetReadDeadline(time.Now().Add(10 * time.Second))
			for {
				n, err = stream.Read(bs)
				totalN += n
				if err != nil {
					break
				}
			}
			println("[Client] bye received:", totalN)
		}()

		// Test stream accept
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			var buffer [512 * 1024]byte
			totalN := 0
			stream.SetReadDeadline(time.Now().Add(5 * time.Second))
			for {
				n, err := stream.Read(buffer[:])
				totalN += n
				if err != nil {
					break
				}
			}
			println("[Client] hello received:", totalN)

			bs := make([]byte, 512*1024)
			pattern := []byte("Bye!")
			for i := 0; i < len(bs); i++ {
				bs[i] = pattern[i%len(pattern)]
			}
			n, err := stream.Write(bs)
			if err != nil && err != io.EOF {
				panic(err.Error())
			}
			println("[Client] Bye sent", n)

		}()

		wg.Wait()
	}()

	<-ctx.Done()
}
