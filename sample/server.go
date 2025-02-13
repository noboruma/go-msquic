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
		/*
			Generate key & cert via:
			`openssl req -nodes -new -x509 -keyout /tmp/server.key -out /tmp/server.cert`
		*/
		CertFile: "/tmp/server.cert",
		KeyFile:  "/tmp/server.key",
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
			stream, err := conn.OpenStream()
			if err != nil {
				panic(err)
			}
			b := make([]byte, 1024)
			n, err := stream.Read(b)
			if err != nil {
				panic(err.Error())
			}
			println("received:", string(b[:n]))
			stream.Write([]byte("Bye!"))
		}
	}()
	<-c.Done()
}
