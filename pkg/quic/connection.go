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
	EnableDatagramReceive         bool
	DisableSendBuffering          bool
	MaxBytesPerKey                int64
}

type MsQuicConn struct {
	remoteAddr        net.UDPAddr
	ctx               context.Context
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	cancel            context.CancelFunc
	shutdown          *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
	openStream        *sync.RWMutex
	startSignal       chan struct{}
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
		openStream:        new(sync.RWMutex),
		startSignal:       make(chan struct{}, 1),
	}
}

func (mqc MsQuicConn) waitStart() bool {
	select {
	case <-mqc.startSignal:
		return true
	case <-mqc.ctx.Done():
	}
	return false
}

func (mqc MsQuicConn) Close() error {
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	if !mqc.shutdown.Swap(true) {

		closed := 0
		mqc.streams.Range(func(k, v any) bool {
			go v.(MsQuicStream).shutdownClose()
			closed += 1
			return true
		})
		if closed != 0 {
			println("PANIC1:", closed)
		}
		close(mqc.acceptStreamQueue)
		closed = 0
		for s := range mqc.acceptStreamQueue {
			go s.shutdownClose()
			closed += 1
		}
		if closed != 0 {
			println("PANIC2:", closed)
		}
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) peerClose() error {
	mqc.cancel()
	return nil
}

func (mqc MsQuicConn) appClose() error {
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	if !mqc.shutdown.Swap(true) {
		closed := 0
		mqc.streams.Range(func(k, v any) bool {
			v.(MsQuicStream).abortClose()
			closed += 1
			return true
		})
		if closed != 0 {
			println("PANIC3:", closed)
		}
		close(mqc.acceptStreamQueue)
		closed = 0
		for s := range mqc.acceptStreamQueue {
			println("PANIC3")
			closed += 1
			s.abortClose()
		}
		if closed != 0 {
			println("PANIC4:", closed)
		}
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	mqc.openStream.RLock()

	if mqc.ctx.Err() != nil {
		mqc.openStream.RUnlock()
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := cCreateStream(mqc.conn)
	if stream == nil {
		mqc.openStream.RUnlock()
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(stream, mqc.ctx)
	var enable C.int8_t
	if mqc.failOpenStream {
		enable = C.int8_t(1)
	} else {
		enable = C.int8_t(0)
	}
	_, loaded := mqc.streams.LoadOrStore(stream, res)
	if loaded {
		println("PANIC")
	}
	if cStartStream(stream, enable) == -1 {
		mqc.openStream.RUnlock()
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
	mqc.openStream.RUnlock()
	if !res.waitStart() {
		return MsQuicStream{}, fmt.Errorf("stream start failed")
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
