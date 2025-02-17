package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "msquic.h"
import "C"

type Stream interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	Close() error
	SetDeadline(ttl time.Time) error
	SetReadDeadline(ttl time.Time) error
	SetWriteDeadline(ttl time.Time) error
	Context() context.Context
}

var streamStatePool = sync.Pool{
	New: func() any {
		return &streamState{
			readBuffer:    bytes.Buffer{},
			readSignal:    make(chan struct{}, 1),
			writeSignal:   make(chan struct{}, 1),
			readDeadline:  time.Time{},
			writeDeadline: time.Time{},
			shutdown:      atomic.Bool{},
		}
	},
}

type streamState struct {
	shutdown         atomic.Bool
	readBufferAccess sync.RWMutex
	readBuffer       bytes.Buffer
	readSignal       chan struct{}
	writeSignal      chan struct{}
	readDeadline     time.Time
	writeDeadline    time.Time
}

func (ss *streamState) Reset() {
	ss.shutdown.Store(false)
	ss.readBuffer.Reset()
	ss.readSignal = make(chan struct{}, 1)
	ss.writeSignal = make(chan struct{}, 1)
}

func (ss *streamState) hasReadData() bool {
	ss.readBufferAccess.RLock()
	defer ss.readBufferAccess.RUnlock()
	return ss.readBuffer.Len() != 0
}

type MsQuicStream struct {
	stream C.HQUIC
	ctx    context.Context
	cancel context.CancelFunc
	state  *streamState
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	state := streamStatePool.Get().(*streamState)
	state.Reset()
	res := MsQuicStream{
		stream: s,
		ctx:    ctx,
		cancel: cancel,
		state:  state,
	}

	return res
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	state := mqs.state
	if state.shutdown.Load() {
		return 0, io.EOF
	}

	deadline := state.readDeadline
	if !state.hasReadData() {
		ctx := mqs.ctx
		if !deadline.IsZero() {
			if time.Now().After(deadline) {
				return 0, os.ErrDeadlineExceeded
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		if !mqs.WaitRead(ctx) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.EOF
		}
	}

	state.readBufferAccess.Lock()
	defer state.readBufferAccess.Unlock()
	n, err := state.readBuffer.Read(data)
	if n == 0 { // ignore io.EOF
		return 0, nil
	}

	return n, err
}

func (mqs MsQuicStream) WaitRead(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-mqs.state.readSignal:
		return true
	}
}

func (mqs MsQuicStream) WaitWrite(ctx context.Context) bool {
	select {
	case <-mqs.ctx.Done():
		return false
	case <-mqs.state.writeSignal:
		return true
	}
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	state := mqs.state
	if state.shutdown.Load() {
		return 0, io.EOF
	}
	ctx := mqs.ctx
	deadline := state.writeDeadline
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	offset := 0
	size := len(data)
	for offset != len(data) {
		cArray := (*C.uint8_t)(unsafe.Pointer(&data[offset]))
		n := cStreamWrite(mqs.stream, cArray, C.int64_t(size))
		offset += int(n)
		size -= int(n)
		if n == -1 {
			return int(n), fmt.Errorf("write stream error")
		}
		if !mqs.WaitWrite(ctx) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				cAbortStream(mqs.stream)
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.ErrUnexpectedEOF
		}
	}
	runtime.KeepAlive(data)
	return int(offset), nil
}

func (mqs MsQuicStream) SetDeadline(ttl time.Time) error {
	err := mqs.SetReadDeadline(ttl)
	err2 := mqs.SetWriteDeadline(ttl)
	return errors.Join(err, err2)
}

func (mqs MsQuicStream) SetReadDeadline(ttl time.Time) error {
	mqs.state.readDeadline = ttl
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	mqs.state.writeDeadline = ttl
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx

}

func (mqs MsQuicStream) Close() error {
	if !mqs.state.shutdown.Swap(true) {
		mqs.cancel()
		cShutdownStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) remoteClose() error {
	if !mqs.state.shutdown.Swap(true) {
		mqs.cancel()
	}
	return nil
}
