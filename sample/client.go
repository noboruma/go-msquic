package main

import (
	"context"

	"github.com/noboruma/go-msquic/pkg/quic"
)

func main() {

	conn, err := quic.DialAddr("127.0.0.1:9999", quic.Config{
		MaxIncomingStreams: 1000,
		MaxIdleTimeout:     5000,
	})
	if err != nil {
		panic(err)
	}
	println("[GO] got a conn")
	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		panic(err)
	}
	println("[GO] got a stream")
	buffer := []byte("Hello world")
	stream.Write([]byte("Hello world"))
	buffer = []byte("           ")
	stream.Read(buffer)
	println("read:", string(buffer))
}
