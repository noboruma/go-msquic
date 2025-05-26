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

type multiBuffers struct {
	multiBufs chan bytes.Buffer
	leftover bytes.Buffer
	outterIndex int
	total atomic.Int64
	ctx context.Context
}
func (mb *multiBuffers) Len() int64 {
	return mb.total.Load()
}

func (mb *multiBuffers) BatchWrite(b [][]byte) (int, error) {
	bb := bytes.Buffer{}
	total := 0
	for i := range b {
		total += len(b[i])
	}
	bb.Grow(total)
	for i := range b {
		bb.Write(b[i])
	}
	select {
	case mb.multiBufs <- bb:
	default:
	return 0, errors.New("no buffer space")
	}
	return total, nil
}

func (mb *multiBuffers) Read(b []byte) (int, error) {
	n := 0
	wait := true
	if mb.leftover.Len() != 0 {
		nn, _ := mb.leftover.Read(b[n:])
		n += nn
		wait = false
	}
	loop: for len(b[n:]) > 0 {
		if !wait {
			select {
			case mb.leftover = <-mb.multiBufs:
			case <-mb.ctx.Done():
				return n, mb.ctx.Err()
			default:
			break loop
			}
		} else {
			select {
			case mb.leftover = <-mb.multiBufs:
			case <-mb.ctx.Done():
				return n, mb.ctx.Err()
			}
			wait = false
		}
		nn, _ := mb.leftover.Read(b[n:])
		n += nn
		wait = false
	}
	return n, nil
}

type streamState struct {
	shutdown         atomic.Bool
	readBufferAccess sync.Mutex
	readBuffer       multiBuffers
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
			readBuffer:    multiBuffers{
				multiBufs:   make(chan bytes.Buffer, 50),
			},
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
	ctx := mqs.ctx
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	state.readBuffer.ctx = ctx
	n, err := state.readBuffer.Read(data)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, os.ErrDeadlineExceeded
		}
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
