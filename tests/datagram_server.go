package main

import (
	"context"
	"os"
	"os/signal"
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
		CertFile:              "/tmp/server.cert",
		KeyFile:               "/tmp/server.key",
		Alpn:                  "go-msquic-sample",
		EnableDatagramReceive: true,
	})
	if err != nil {
		panic(err.Error())
	}
	defer l.Close()
	handleConnection := func(conn quic.Connection) {
		defer conn.Close()
		msg, err := conn.ReceiveDatagram(context.Background())
		if err != nil {
			println(err.Error())
		} else {
			println("received", string(msg))
		}

		<-time.After(1 * time.Second)
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
