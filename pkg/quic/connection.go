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
	SendDatagram(msg []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)
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
	//FailOnOpenStream       bool // deprecated in favor of DisableFailOnOpenStream
	DisableFailOnOpenStream bool
	EnableDatagramReceive   bool
	DisableSendBuffering    bool // Do not allocate & copy sent buffers
	MaxBytesPerKey          int64
	EnableAppBuffering      bool // This flags is global across all listeners & dialers
}

type MsQuicConn struct {
	remoteAddr        net.UDPAddr
	ctx               context.Context
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	datagrams         chan []byte
	cancel            context.CancelFunc
	shutdown          *atomic.Bool
	streams           *sync.Map //map[C.HQUIC]MsQuicStream
	openStream        *sync.RWMutex
	startSignal       chan struct{}
	failOpenStream    bool
	noAlloc           bool
	useAppBuffers     bool // Receive side only
}

func newMsQuicConn(c C.HQUIC, failOnOpen, noAlloc, useAppBuffers bool) MsQuicConn {

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
		useAppBuffers:     useAppBuffers,
		datagrams:         make(chan []byte),
	}
}

func (mqc MsQuicConn) waitStart(ctx context.Context) bool {
	select {
	case <-mqc.startSignal:
		return true
	case <-mqc.ctx.Done():
	case <-ctx.Done():
	}
	return false
}

func (mqc MsQuicConn) Close() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.openStream.Lock()
		defer mqc.openStream.Unlock()
		cShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) peerClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		mqc.openStream.Lock()
		defer mqc.openStream.Unlock()
	}
	return nil
}

func (mqc MsQuicConn) appClose() error {
	mqc.shutdown.Store(true)
	mqc.cancel()
	mqc.openStream.Lock()
	defer mqc.openStream.Unlock()
	mqc.streams.Range(func(key, value any) bool {
		println("PANIC Lingering stream")
		if s, has := mqc.streams.LoadAndDelete(key); has {
			s.(MsQuicStream).abortClose()
			s.(MsQuicStream).operationsBarrier()
		}
		return true
	})
	connections.Delete(mqc.conn)

	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	mqc.openStream.RLock()
	defer mqc.openStream.RUnlock()

	if mqc.ctx.Err() != nil {
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	useAppBuffers := C.int8_t(0)
	if mqc.useAppBuffers {
		useAppBuffers = C.int8_t(1)
	}
	stream := cCreateStream(mqc.conn, useAppBuffers)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(mqc.conn, stream, mqc.ctx, mqc.noAlloc, mqc.useAppBuffers)
	_, loaded := mqc.streams.LoadOrStore(stream, res)
	if loaded {
		println("PANIC")
	}
	enable := C.int8_t(0)
	if mqc.failOpenStream {
		enable = C.int8_t(1)
	}

	if mqc.useAppBuffers {
		for range initBufs {
			initBuf := provideAppBuffer(res)
			if initBuf == nil || cAttachAppBuffer(stream, initBuf) == -1 {
				mqc.streams.Delete(stream)
				res.releaseBuffers()
				cFreeStream(stream)
				return MsQuicStream{}, fmt.Errorf("stream buffer attach error")
			}
			res.state.recvTotal.Add(uint32(len(initBuf)))
		}
	}

	if cStartStream(stream, enable, useAppBuffers) == -1 {
		if !mqc.failOpenStream {
			mqc.streams.Delete(stream)
			res.releaseBuffers()
			cFreeStream(stream)
		}
		return MsQuicStream{}, fmt.Errorf("stream start error")
	}
	if mqc.failOpenStream {
		if !res.waitStart() {
			//res.releaseBuffers()
			//mqc.streams.Delete(stream)
			//cFreeStream(stream)
			startFail.Add(1)
			return MsQuicStream{}, fmt.Errorf("stream start failed")
		}
	}
	startSucc.Add(1)
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

func (c MsQuicConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case msg := <-c.datagrams:
		return msg, nil
	}
}

func (c MsQuicConn) SendDatagram(msg []byte) error {
	if cDatagramSendConnection(c.conn, msg) != 0 {
		return fmt.Errorf("error encountered while sending datagram")
	}
	return nil
}
