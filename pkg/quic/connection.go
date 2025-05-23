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
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
	remoteAddr        net.UDPAddr
	shutdown          *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
	failOpenStream    bool
	openStream        *sync.Mutex
	startSignal       chan struct{}
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
		openStream:        new(sync.Mutex),
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
		mqc.streams.Range(func(k, v any) bool {
			println("PANIC1")
			go v.(MsQuicStream).shutdownClose()
			return true
		})
		close(mqc.acceptStreamQueue)
		for s := range mqc.acceptStreamQueue {
			println("PANIC12")
			go s.shutdownClose()
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
		mqc.streams.Range(func(k, v any) bool {
			println("PANIC2")
			v.(MsQuicStream).abortClose()
			return true
		})
		close(mqc.acceptStreamQueue)
		for s := range mqc.acceptStreamQueue {
			println("PANIC3")
			s.abortClose()
		}
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	mqc.openStream.Lock()

	if mqc.ctx.Err() != nil {
		mqc.openStream.Unlock()
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := cCreateStream(mqc.conn)
	if stream == nil {
		mqc.openStream.Unlock()
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
		mqc.openStream.Unlock()
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
	mqc.openStream.Unlock()
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
