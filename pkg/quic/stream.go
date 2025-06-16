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

// #include "inc/msquic.h"
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

type ChainedBuffers struct {
	current *ChainedBuffer
	tail    *ChainedBuffer
}

func (cbs *ChainedBuffers) HasData() bool {
	return cbs.current.HasData()
}

func (cbs *ChainedBuffers) Write(batch [][]byte) {
	next := ChainedBuffer{}
	total := 0
	for _, b := range batch {
		total += len(b)
	}
	next.readBuffer.Grow(total)
	for _, b := range batch {
		next.readBuffer.Write(b)
	}
	cbs.tail.next.Store(&next)
	cbs.tail = &next
}

func (cbs *ChainedBuffers) Read(output []byte) (int, error) {
	n := 0
	for {
		if cbs.current.empty.Load() {
			next := cbs.current.next.Swap(nil)
			if next == nil {
				break
			}
			cbs.current = next
		}
		nn, _ := cbs.current.readBuffer.Read(output[n:])
		n += nn
		if len(output) == n {
			break
		}
		cbs.current.empty.Store(true)
	}
	return n, nil
}

type ChainedBuffer struct {
	readBuffer bytes.Buffer
	next       atomic.Pointer[ChainedBuffer]
	empty      atomic.Bool
}

func (cb *ChainedBuffer) HasData() bool {
	if !cb.empty.Load() {
		return true
	}
	next := cb.next.Load()
	return next != nil
}

type streamState struct {
	readBuffers   ChainedBuffers
	readDeadline  time.Time
	writeDeadline time.Time
	writeAccess   sync.Mutex
	startSignal   chan struct{}
	shutdown      atomic.Bool
}

func (ss *streamState) hasReadData() bool {
	return ss.readBuffers.HasData()
}

type MsQuicStream struct {
	ctx        context.Context
	stream     C.HQUIC
	cancel     context.CancelFunc
	state      *streamState
	readSignal chan struct{}
	peerSignal chan struct{}
	noAlloc    bool
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context, noAlloc bool) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		stream: s,
		ctx:    ctx,
		cancel: cancel,
		state: &streamState{
			readBuffers:   ChainedBuffers{},
			readDeadline:  time.Time{},
			writeDeadline: time.Time{},
			shutdown:      atomic.Bool{},
			startSignal:   make(chan struct{}, 1),
		},
		readSignal: make(chan struct{}, 1),
		peerSignal: make(chan struct{}, 1),
		noAlloc:    noAlloc,
	}
	res.state.readBuffers.current = &ChainedBuffer{}
	res.state.readBuffers.tail = res.state.readBuffers.current
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
	now := time.Now()
	defer func() {
		time10.Add(time.Since(now).Milliseconds())
	}()
	state := mqs.state
	if !state.hasReadData() {
		if mqs.ctx.Err() != nil {
			time11.Add(time.Since(now).Milliseconds())
			return 0, io.EOF
		}

		deadline := state.readDeadline
		ctx := mqs.ctx
		if !deadline.IsZero() {
			if time.Now().After(deadline) {
				time11.Add(time.Since(now).Milliseconds())
				return 0, os.ErrDeadlineExceeded
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		if !mqs.waitRead(ctx) {
			time11.Add(time.Since(now).Milliseconds())
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.EOF
		}
		time12.Add(time.Since(now).Milliseconds())
	}

	return state.readBuffers.Read(data)
}

func (mqs MsQuicStream) waitRead(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-mqs.readSignal:
		return true
	}
}

var sendBuffers sync.Map //map[uintptr][]byte
var sendBuffersSize atomic.Int64
var sendBuffersCount atomic.Int64

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	now := time.Now()
	defer func() {
		time9.Add(time.Since(now).Milliseconds())
	}()
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
	}
	cNoAlloc := C.uint8_t(0)
	if mqs.noAlloc {
		cNoAlloc = C.uint8_t(1)
		idx := (uintptr)(unsafe.Pointer(unsafe.SliceData(data)))
		sendBuffers.Store(idx, data)
		sendBuffersSize.Add(1)
		sendBuffersCount.Add(1)
	}
	n := cStreamWrite(mqs.stream,
		(*C.uint8_t)(unsafe.SliceData(data[:])),
		C.int64_t(len(data)),
		cNoAlloc)
	if n == -1 {
		return 0, fmt.Errorf("write stream error %v", len(data))
	}
	runtime.KeepAlive(data)
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
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	return mqs.abortClose()
}

func (mqs MsQuicStream) peerClose() error {
	if !mqs.state.shutdown.Swap(true) {
		mqs.peerCloseACK()
		mqs.cancel()
	}
	return nil
}

func (mqs MsQuicStream) appClose() error {
	mqs.state.shutdown.Store(true)
	mqs.peerCloseACK()
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	return nil
}

//func (mqs MsQuicStream) shutdownClose() error {
//	mqs.cancel()
//	mqs.state.writeAccess.Lock()
//	defer mqs.state.writeAccess.Unlock()
//	if !mqs.state.shutdown.Swap(true) {
//		//cAbortStream(mqs.stream)
//		cShutdownStream(mqs.stream)
//		select {
//		case <-mqs.peerSignal:
//		case <-time.After(3 * time.Second):
//			cAbortStream(mqs.stream)
//		}
//	}
//	return nil
//}

func (mqs MsQuicStream) abortClose() error {
	if !mqs.state.shutdown.Swap(true) {
		mqs.peerClose()
		mqs.cancel()
		cAbortStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) peerCloseACK() {
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
	if mqs.noAlloc {
		return mqs.dynaReadFrom(r)
	} else {
		return mqs.staticReadFrom(r)
	}
}

func (mqs MsQuicStream) staticReadFrom(r io.Reader) (n int64, err error) {
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

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 32*1024)
	},
}

func (mqs MsQuicStream) dynaReadFrom(r io.Reader) (n int64, err error) {
	for mqs.ctx.Err() == nil {
		buffer := bufferPool.Get().([]byte)
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
