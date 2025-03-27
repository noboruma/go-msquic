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
	FailOnOpenStream              bool
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
	failOpenStream    bool
}

func newMsQuicConn(c C.HQUIC, failOnOpen bool) MsQuicConn {
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
		failOpenStream:    failOnOpen,
	}
}

func (mqc MsQuicConn) Close() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.streams.Range(func(k, v any) bool {
			v.(MsQuicStream).abortClose()
			return true
		})
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) appClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.streams.Range(func(k, v any) bool {
			v.(MsQuicStream).abortClose()
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
	var added int64
	if mqc.failOpenStream {
		added = cStartStream(stream, C.int8_t(1))
	} else {
		added = cStartStream(stream, C.int8_t(0))
	}
	if added == -1 {
		mqc.streams.Delete(stream)
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
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
