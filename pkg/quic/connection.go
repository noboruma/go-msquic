package quic

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
)

// #include "msquic.h"
import "C"

const streamAcceptQueueSize = 1024

type Connection interface {
	OpenStream() (MsQuicStream, error)
	AcceptStream(ctx context.Context) (MsQuicStream, error)
	Close() error
	RemoteAddr() net.Addr
	Context() context.Context
}

type Config struct {
	MaxIncomingStreams int64
	MaxIdleTimeout     int64
	KeepAlivePeriod    int64
	CertFile           string
	KeyFile            string
	Alpn               string
}

type MsQuicConn struct {
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
	remoteAddr        net.UDPAddr
	shutdown          *atomic.Bool
}

func newMsQuicConn(c C.HQUIC) MsQuicConn {
	ctx, cancel := context.WithCancel(context.Background())

	ip, port := getRemoteAddr(c)

	return MsQuicConn{
		conn:              c,
		acceptStreamQueue: make(chan MsQuicStream, streamAcceptQueueSize),
		ctx:               ctx,
		cancel:            cancel,
		remoteAddr:        net.UDPAddr{IP: ip, Port: port},
		shutdown:          new(atomic.Bool),
	}
}

func (mqc MsQuicConn) Close() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) remoteClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	if mqc.shutdown.Load() {
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := cOpenStream(mqc.conn)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	totalOpenedStreams.Add(1)
	res := newMsQuicStream(stream, mqc.ctx)
	streams.Store(stream, res)
	return res, nil
}

func (mqc MsQuicConn) AcceptStream(ctx context.Context) (MsQuicStream, error) {
	select {
	case <-ctx.Done():
	case <-mqc.ctx.Done():
	case s, open := <-mqc.acceptStreamQueue:
		if !open {
			return MsQuicStream{}, fmt.Errorf("closed connection")
		}
		return s, nil
	}
	return MsQuicStream{}, fmt.Errorf("closed connection")
}

func (mqc MsQuicConn) Context() context.Context {
	return mqc.ctx
}

func (mqc MsQuicConn) RemoteAddr() net.Addr {
	return &mqc.remoteAddr
}
