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
	ReadFrom(r io.Reader) (int64, error)
	WriteTo(w io.Writer) (int64, error)
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
	writeAccess      sync.Mutex
	startSignal      chan struct{}
}

func (ss *streamState) hasReadData() bool {
	ss.readBufferAccess.Lock()
	defer ss.readBufferAccess.Unlock()
	return ss.readBuffer.Len() != 0
}

type MsQuicStream struct {
	stream     C.HQUIC
	ctx        context.Context
	cancel     context.CancelFunc
	state      *streamState
	readSignal chan struct{}
	peerSignal chan struct{}
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
			startSignal:   make(chan struct{}, 1),
		},
		readSignal: make(chan struct{}, 1),
		peerSignal: make(chan struct{}, 1),
	}
	return res
}

func (mqs MsQuicStream) waitStart() bool {
	select {
	case <-mqs.state.startSignal:
		return true
	case <-mqs.Context().Done():
	}
	return false
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	state := mqs.state
	if mqs.ctx.Err() != nil {
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
		if !mqs.waitRead(ctx) {
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

func (mqs MsQuicStream) waitRead(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-mqs.readSignal:
		return true
	}
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	state := mqs.state
	state.writeAccess.Lock()
	defer state.writeAccess.Unlock()
	ctx := mqs.ctx
	if ctx.Err() != nil {
		return 0, io.EOF
	}
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
	for offset != len(data) && ctx.Err() == nil {
		n := cStreamWrite(mqs.stream, (*C.uint8_t)(unsafe.SliceData(data[offset:])), C.int64_t(size))
		if n == -1 {
			return int(offset), fmt.Errorf("write stream error %v/%v", offset, size)
		}
		offset += int(n)
		size -= int(n)
	}
	runtime.KeepAlive(data)
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return int(offset), os.ErrDeadlineExceeded
		}
		return int(offset), io.ErrUnexpectedEOF
	}
	return len(data), nil
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
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	mqs.state.shutdown.Store(true)
	return nil
}

func (mqs MsQuicStream) shutdownClose() error {
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
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
	mqs.peerClose()
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
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

func (mqs MsQuicStream) WriteTo(w io.Writer) (int64, error) {
	var buffer [32 * 1024]byte
	n := int64(0)
	for mqs.ctx.Err() == nil {
		bn, err := mqs.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = w.Write(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			return n, err
		}
	}
	return n, io.EOF
}

func (mqs MsQuicStream) ReadFrom(r io.Reader) (n int64, err error) {
	var buffer [32 * 1024]byte
	for mqs.ctx.Err() == nil {
		bn, err := r.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = mqs.Write(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			return n, err
		}
	}
	return n, io.EOF
}
