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
	copy    bool
	buffers *sync.Map
}

func (cbs *ChainedBuffers) HasData() bool {
	return cbs.current.HasData()
}

func (cbs *ChainedBuffers) Write(batch []byte) {
	next := ChainedBuffer{}
	if cbs.copy {
		next.copyReadBuffer.Grow(len(batch))
		next.copyReadBuffer.Write(batch)
	} else {
		next.noCopyReadBuffer = batch
	}
	cbs.tail.next.Store(&next)
	cbs.tail = &next
}

func findBuffer(subBuf []byte, buffers *sync.Map) []byte {
	current := unsafe.Pointer(unsafe.SliceData(subBuf))
	//currentEnd := unsafe.Add(current, len(subBuf))
	var buffer []byte
	var offsetStart int
	buffers.Range(func(key, value any) bool {
		tmp := value.(recvBuffer)
		goBuffer := *tmp.goBuffer
		start := unsafe.Pointer(unsafe.SliceData(goBuffer))
		end := unsafe.Add(start, len(goBuffer))
		if uintptr(current) >= uintptr(start) && uintptr(current) < uintptr(end) {
			buffer = goBuffer
			offsetStart = int(uintptr(current) - uintptr(start))
			return false
		}
		return true
	})
	if buffer != nil {
		return buffer[offsetStart : offsetStart+len(subBuf)]
	}
	println("buffer never found!")
	return buffer
}

func sliceEnd(subBuf []byte) uintptr {
	current := unsafe.Pointer(unsafe.SliceData(subBuf))
	currentEnd := unsafe.Add(current, len(subBuf))
	return uintptr(currentEnd)
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
		var nn int
		if cbs.copy {
			nn, _ = cbs.current.copyReadBuffer.Read(output[n:])
		} else {
			nn = copy(output[n:], cbs.current.noCopyReadBuffer[:])
			cbs.current.noCopyReadBuffer = cbs.current.noCopyReadBuffer[nn:]
		}
		n += nn
		if len(output) == n {
			break
		}
		cbs.current.empty.Store(true)
		if !cbs.copy {
			end := sliceEnd(cbs.current.noCopyReadBuffer)
			if buf, has := cbs.buffers.LoadAndDelete(end); has {
				b := buf.(recvBuffer)
				bufferPool.Put(b.goBuffer)
				C.free(unsafe.Pointer(b.cBuffer))
				receiveBuffers.Add(-1)
				b.pinner.Unpin()
			}
		}
	}
	return n, nil
}

type ChainedBuffer struct {
	noCopyReadBuffer []byte
	copyReadBuffer   bytes.Buffer
	next             atomic.Pointer[ChainedBuffer]
	empty            atomic.Bool
	copy             bool
}

func (cb *ChainedBuffer) HasData() bool {
	if !cb.empty.Load() {
		return true
	}
	next := cb.next.Load()
	return next != nil
}

type recvBuffer struct {
	cBuffer  *C.QUIC_BUFFER
	goBuffer *[]byte
	pinner   runtime.Pinner
}

type sendBuffer struct {
	goBuffer *[]byte
	pinner   runtime.Pinner
}

type streamState struct {
	readBuffers   ChainedBuffers
	writeDeadline time.Time
	writeAccess   sync.Mutex
	startSignal   chan struct{}
	shutdown      atomic.Bool
	recvCount     atomic.Uint32
	recvTotal     atomic.Uint32

	readDeadlineContext context.Context
	readDeadlineCancel  context.CancelFunc

	recvBuffers sync.Map //map[uintptr]recvBuffer
	sendBuffers sync.Map //map[uintptr]sendBuffer
}

func (ss *streamState) needMoreBuffer() bool {
	return ss.recvCount.Load()+bufferSize+(bufferSize/4) >= ss.recvTotal.Load() // always keep 1 extra bufferSize
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

func newMsQuicStream(s C.HQUIC, connCtx context.Context, noAlloc, appBuffers bool) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		stream: s,
		ctx:    ctx,
		cancel: cancel,
		state: &streamState{
			readBuffers: ChainedBuffers{
				copy: !appBuffers,
			},
			readDeadlineContext: ctx,
			writeDeadline:       time.Time{},
			shutdown:            atomic.Bool{},
			startSignal:         make(chan struct{}, 1),
		},
		readSignal: make(chan struct{}, 1),
		peerSignal: make(chan struct{}, 1),
		noAlloc:    noAlloc,
	}
	res.state.readBuffers.current = &ChainedBuffer{}
	res.state.readBuffers.tail = res.state.readBuffers.current
	res.state.readBuffers.buffers = &res.state.recvBuffers
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

		ctx := state.readDeadlineContext
		if !mqs.waitRead(ctx) {
			time11.Add(time.Since(now).Milliseconds())
			if state.readDeadlineCancel != nil {
				state.readDeadlineCancel()
				state.readDeadlineCancel = nil
			}
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
		pinner := runtime.Pinner{}
		pinner.Pin(unsafe.SliceData(data))
		defer pinner.Unpin()
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
	if mqs.state.readDeadlineCancel != nil {
		mqs.state.readDeadlineCancel()
		mqs.state.readDeadlineCancel = nil
	}
	if !ttl.IsZero() {
		mqs.state.readDeadlineContext, mqs.state.readDeadlineCancel = context.WithDeadline(mqs.ctx, ttl)
	} else {
		mqs.state.readDeadlineContext = mqs.ctx
	}
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
	buf := bufferPool.Get().(*[]byte)
	buffer := *buf
	defer bufferPool.Put(buf)
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
	buf := bufferPool.Get().(*[]byte)
	buffer := *buf
	defer bufferPool.Put(buf)
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

func (mqs MsQuicStream) dynaReadFrom(r io.Reader) (n int64, err error) {
	for mqs.ctx.Err() == nil {
		buf := bufferPool.Get().(*[]byte)
		buffer := *buf
		pinner := runtime.Pinner{}
		pinner.Pin(unsafe.Pointer(unsafe.SliceData(buffer)))
		idx := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
		if _, has := mqs.state.sendBuffers.Load(idx); has {
			println("Error: buffer reused")
		}
		sendBuffersSize.Add(1)
		sendBuffersCount.Add(1)
		bn, err := r.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = mqs.Write(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			sendBuffersSize.Add(-1)
			bufferPool.Put(buf)
			pinner.Unpin()
			return n, err
		}
		mqs.state.sendBuffers.Store(idx, sendBuffer{
			goBuffer: buf,
			pinner:   pinner,
		})
	}
	return n, io.EOF
}
