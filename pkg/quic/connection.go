package quic

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// #include "inc/msquic.h"
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
	noAlloc           bool
}

func newMsQuicConn(c C.HQUIC, failOnOpen, noAlloc bool) MsQuicConn {
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
		noAlloc:           noAlloc,
	}
}

func (mqc MsQuicConn) waitStart(ctx context.Context) bool {
	select {
	case <-mqc.startSignal:
		println("1")
		return true
	case <-mqc.ctx.Done():
		println("2")
	case <-ctx.Done():
		println("3")
	}
	return false
}

func (mqc MsQuicConn) Close() error {
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) peerClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		//mqc.openStream.Lock()
		//defer mqc.openStream.Unlock()
	}
	return nil
}

func (mqc MsQuicConn) appClose() error {
	mqc.shutdown.Store(true)
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	connections.Delete(mqc.conn)

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
	res := newMsQuicStream(stream, mqc.ctx, mqc.noAlloc)
	_, loaded := mqc.streams.LoadOrStore(stream, res)
	if loaded {
		println("PANIC")
	}
	var enable C.int8_t
	if mqc.failOpenStream {
		enable = C.int8_t(1)
	} else {
		enable = C.int8_t(0)
	}
	if cStartStream(stream, enable) == -1 {
		mqc.openStream.RUnlock()
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
	mqc.openStream.RUnlock()
	if mqc.failOpenStream {
		if !res.waitStart() {
			return MsQuicStream{}, fmt.Errorf("stream start failed")
		}
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
