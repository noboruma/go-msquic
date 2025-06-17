package quic

import (
	"context"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// #include "inc/msquic.h"
import "C"

type MsQuicListener struct {
	listener, config C.HQUIC
	acceptQueue      chan MsQuicConn
	// deallocate
	key, cert, alpn  *C.char
	shutdown         *atomic.Bool
	failOnOpenStream bool
	noAllocStream    bool
	appBuffers       bool
}

func newMsQuicListener(c C.HQUIC, config C.HQUIC, key, cert, alpn *C.char, failOnOpenStream, noAlloc, appBuffers bool) MsQuicListener {
	return MsQuicListener{
		listener:         c,
		acceptQueue:      make(chan MsQuicConn, 1_000),
		key:              key,
		cert:             cert,
		alpn:             alpn,
		config:           config,
		shutdown:         new(atomic.Bool),
		failOnOpenStream: failOnOpenStream,
		noAllocStream:    noAlloc,
		appBuffers:       appBuffers,
	}
}

func (mql MsQuicListener) Close() error {

	if !mql.shutdown.Swap(true) {
		cCloseListener(mql.listener, mql.config)
		C.free(unsafe.Pointer(mql.key))
		C.free(unsafe.Pointer(mql.cert))
		C.free(unsafe.Pointer(mql.alpn))
	}
	return nil
}

func (mql MsQuicListener) Accept(ctx context.Context) (MsQuicConn, error) {
	select {
	case c, open := <-mql.acceptQueue:
		if !open {
			return MsQuicConn{}, fmt.Errorf("closed listener")
		}
		return c, nil
	case <-ctx.Done():
		return MsQuicConn{}, fmt.Errorf("closed context")
	}
}
