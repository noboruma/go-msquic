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
	c, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	l, err := quic.ListenAddr("0.0.0.0:9090", quic.Config{
		MaxIncomingStreams: 1000,
		MaxIdleTimeout:     5 * time.Second,
		/*
			Generate key & cert via:
			`openssl req -nodes -new -x509 -keyout /tmp/server.key -out /tmp/server.cert`
		*/
		CertFile: "/tmp/server.cert",
		KeyFile:  "/tmp/server.key",
		Alpn:     "go-msquic-sample",
	})
	if err != nil {
		panic(err.Error())
	}
	defer l.Close()
	handleConnection := func(conn quic.Connection) {
		defer conn.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			b := make([]byte, len("Hello"))
			n, err := stream.Read(b)

			if err != nil && err != io.EOF {
				panic(err.Error())
			}

			println("[Server] received:", string(b[:n]))

			_, err = stream.Write([]byte("Bye!"))

			if err != nil && err != io.EOF {
				panic(err.Error())
			}
			println("[Server] Bye! sent")
		}()

		go func() {
			defer wg.Done()
			stream, err := conn.OpenStream()
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			_, err = stream.Write([]byte("Hello"))

			if err != nil && err != io.EOF {
				panic(err.Error())
			}
			println("[Server] Hello sent")

			b := make([]byte, len("Hello"))
			n, err := stream.Read(b)

			if err != nil && err != io.EOF {
				panic(err.Error())
			}

			println("[Server] received:", string(b[:n]))

		}()
		wg.Wait()
	}
	go func() {
		for c.Err() == nil {
			conn, err := l.Accept(context.Background())
			if err != nil {
				panic(err.Error())
			}
			go handleConnection(conn)
		}
	}()
	<-c.Done()
}
