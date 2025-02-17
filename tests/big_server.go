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

		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				panic(err.Error())
			}
			defer stream.Close()

			totalN := 0
			bs := make([]byte, 512*1024)
			stream.SetReadDeadline(time.Now().Add(5 * time.Second))
			for {
				n, err := stream.Read(bs)
				totalN += n
				if err != nil {
					break
				}
			}

			println("[Server] hello received:", totalN)

			pattern := []byte("Bye!")
			for i := 0; i < len(bs); i++ {
				bs[i] = pattern[i%len(pattern)]
			}
			n, err := stream.Write(bs)

			if err != nil && err != io.EOF {
				panic(err.Error())
			}
			println("[Server] Bye! sent", n)

		}()

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
			println("[Server] Hello sent", n)

			totalN := 0
			stream.SetReadDeadline(time.Now().Add(10 * time.Second))
			for {
				n, err = stream.Read(bs)

				totalN += n
				if err != nil {
					break
				}
			}

			println("[Server] bye received:", totalN)

		}()
		wg.Wait()
		<-time.After(1 * time.Second) // Keep the connection open to avoid stream abort
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
