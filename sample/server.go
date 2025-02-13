package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/noboruma/go-msquic/pkg/quic"
)

func main() {
	c, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	l, err := quic.ListenAddr("0.0.0.0:9999", quic.Config{
		MaxIncomingStreams: 1000,
		MaxIdleTimeout:     5000,
		CertFile:           "/home/zackel/workspace/proxies/go-msquic/server.cert",
		KeyFile:            "/home/zackel/workspace/proxies/go-msquic/server.key",
	})
	if err != nil {
		panic(err)
	}
	go func() {

		for c.Err() == nil {
			conn, err := l.Accept(context.Background())
			if err != nil {
				panic(err)
			}
			println("[GO] Got new conn")
			stream, err := conn.OpenStream()
			println("[GO] Got new stream")
			if err != nil {
				panic(err)
			}
			stream.Write([]byte("Bye!"))
			b := make([]byte, 1024)
			n, err := stream.Read(b)
			if err != nil {
				panic("Read error")
			}
			println("received:", string(b[:n]))
		}
	}()
	<-c.Done()
	println("yay")
}
