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

type streamState struct {
	shutdown         atomic.Bool
	readBufferAccess sync.Mutex
	readBuffer       bytes.Buffer
	readDeadline     time.Time
	writeDeadline    time.Time
	closeAccess      sync.Mutex
}

func (ss *streamState) hasReadData() bool {
	ss.readBufferAccess.Lock()
	defer ss.readBufferAccess.Unlock()
	return ss.readBuffer.Len() != 0
}

type MsQuicStream struct {
	stream                  C.HQUIC
	ctx                     context.Context
	cancel                  context.CancelFunc
	state                   *streamState
	readSignal, writeSignal chan struct{}
	peerSignal              chan struct{}
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)

	res := MsQuicStream{
		stream: s,
		ctx:    ctx,
		cancel: cancel,
		state: &streamState{
			readBuffer:    bytes.Buffer{},
			readDeadline:  time.Time{},
			writeDeadline: time.Time{},
			shutdown:      atomic.Bool{},
		},
		readSignal:  make(chan struct{}, 1),
		writeSignal: make(chan struct{}, 1),
		peerSignal:  make(chan struct{}, 1),
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
	case <-mqs.readSignal:
		return true
	}
}

func (mqs MsQuicStream) WaitWrite(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-mqs.writeSignal:
		return true
	}
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	state := mqs.state
	state.closeAccess.Lock()
	defer state.closeAccess.Unlock()
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
		if n == -1 {
			return int(offset), fmt.Errorf("write stream error %x %v", mqs.stream, size)
		}
		if !mqs.WaitWrite(ctx) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return int(offset), os.ErrDeadlineExceeded
			}
			return int(offset), io.ErrUnexpectedEOF
		}
		offset += int(n)
		size -= int(n)
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

// Close is a definitive operation
// The stream cannot be receive anything after that call
func (mqs MsQuicStream) Close() error {
	return mqs.shutdownClose()
}

func (mqs MsQuicStream) appClose() error {
	mqs.peerClose()
	mqs.cancel()
	mqs.state.closeAccess.Lock()
	defer mqs.state.closeAccess.Unlock()
	mqs.state.shutdown.Store(true)
	return nil
}

func (mqs MsQuicStream) shutdownClose() error {
	mqs.state.closeAccess.Lock()
	defer mqs.state.closeAccess.Unlock()
	mqs.cancel()

	if !mqs.state.shutdown.Swap(true) {
		cShutdownStream(mqs.stream)
		select {

		case <-mqs.peerSignal:
		case <-time.After(3 * time.Second):
			cAbortStream(mqs.stream)
		}
	}
	return nil
}

func (mqs MsQuicStream) abortClose() error {
	mqs.state.closeAccess.Lock()
	defer mqs.state.closeAccess.Unlock()
	mqs.cancel()
	if !mqs.state.shutdown.Swap(true) {
		cAbortStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) peerClose() {
	select {
	case mqs.peerSignal <- struct{}{}:
	default:
	}
}
