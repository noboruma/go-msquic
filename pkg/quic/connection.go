package quic

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// #include "msquic.h"
import "C"

const streamAcceptQueueSize = 100

type Connection interface {
	OpenStream() (MsQuicStream, error)
	AcceptStream(ctx context.Context) (MsQuicStream, error)
	Close() error
	RemoteAddr() net.Addr
	Context() context.Context
}

type Config struct {
	MaxIncomingStreams            int64
	MaxIdleTimeout                time.Duration
	KeepAlivePeriod               time.Duration
	CertFile                      string
	KeyFile                       string
	Alpn                          string
	MaxBindingStatelessOperations int64
	MaxStatelessOperations        int64
	TracePerfCounts               func([]string, []uint64)
	TracePerfCountReport          time.Duration
}

type MsQuicConn struct {
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
	remoteAddr        net.UDPAddr
	shutdown          *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
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
		streams:           new(sync.Map),
	}
}

func (mqc MsQuicConn) Close() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.streams.Range(func(k, v any) bool {
			v.(MsQuicStream).Close()
			return true
		})
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) remoteClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.streams.Range(func(k, v any) bool {
			v.(MsQuicStream).remoteClose()
			return true
		})
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	if mqc.shutdown.Load() {
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := cCreateStream(mqc.conn)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(stream, mqc.ctx)
	mqc.streams.Store(stream, res)
	cStartStream(stream)
	totalOpenedStreams.Add(1)
	return res, nil
}

func (mqc MsQuicConn) AcceptStream(ctx context.Context) (MsQuicStream, error) {

	select {
	case <-ctx.Done():
	case <-mqc.ctx.Done():
	case s, ok := <-mqc.acceptStreamQueue:
		if ok {
			return s, nil
		}
	}
	return MsQuicStream{}, fmt.Errorf("closed connection")
}

func (mqc MsQuicConn) Context() context.Context {
	return mqc.ctx
}

func (mqc MsQuicConn) RemoteAddr() net.Addr {
	return &mqc.remoteAddr
}
