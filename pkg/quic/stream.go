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

type ReadBuffer struct {
	buffer        bytes.Buffer
	m             sync.Mutex
	signal        chan struct{}
	readDeadline  time.Time
	writeDeadline time.Time
}

func (rb *ReadBuffer) Reset() {
	rb.buffer.Reset()
	rb.signal = make(chan struct{}, 1)
	rb.readDeadline = time.Time{}
	rb.writeDeadline = time.Time{}
}

var readBufferPool = sync.Pool{
	New: func() any {
		return &ReadBuffer{
			buffer: bytes.Buffer{},
			signal: make(chan struct{}, 1),
		}
	},
}

type MsQuicStream struct {
	stream   C.HQUIC
	buffer   *ReadBuffer
	ctx      context.Context
	cancel   context.CancelFunc
	shutdown *atomic.Bool
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	b := readBufferPool.Get().(*ReadBuffer)
	b.Reset()
	res := MsQuicStream{
		stream:   s,
		buffer:   b,
		ctx:      ctx,
		cancel:   cancel,
		shutdown: new(atomic.Bool),
	}

	return res
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	if mqs.shutdown.Load() {
		return 0, io.EOF
	}

	mqs.buffer.m.Lock()
	for mqs.buffer.buffer.Len() == 0 {
		ctx := mqs.ctx
		if !mqs.buffer.readDeadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, mqs.buffer.readDeadline)
			defer cancel()
		}

		mqs.buffer.m.Unlock()

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.EOF
		case <-mqs.buffer.signal:
			mqs.buffer.m.Lock()
		}
	}
	defer mqs.buffer.m.Unlock()

	return mqs.buffer.buffer.Read(data)
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	if mqs.shutdown.Load() {
		return 0, io.EOF
	}
	if !mqs.buffer.writeDeadline.IsZero() {
		if time.Now().After(mqs.buffer.writeDeadline) {
			return 0, os.ErrDeadlineExceeded
		}
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
	mqs.buffer.m.Lock()
	defer mqs.buffer.m.Unlock()
	mqs.buffer.readDeadline = ttl
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	mqs.buffer.m.Lock()
	defer mqs.buffer.m.Unlock()
	mqs.buffer.writeDeadline = ttl
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx
}

func (mqs MsQuicStream) Close() error {
	if !mqs.shutdown.Swap(true) {
		mqs.cancel()
		cShutdownStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) remoteClose() error {
	if !mqs.shutdown.Swap(true) {
		mqs.cancel()
	}
	return nil
}
