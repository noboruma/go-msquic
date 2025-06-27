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
	current     *ChainedBuffer
	tail        *ChainedBuffer
	copy        bool
	recvBuffers *sync.Map
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

func findBuffer(current uintptr, length int, buffers *sync.Map) []byte {
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
		pinRecv.Add(1)
		return buffer[offsetStart : offsetStart+length]
	}
	//println("buffer never found!")
	pinSend.Add(1)
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
			nn = copy(output[n:], cbs.current.noCopyReadBuffer[cbs.current.noCopyReadIndex:])
			cbs.current.noCopyReadIndex += nn
		}
		n += nn
		if len(output) == n {
			break
		}
		cbs.current.empty.Store(true)
		if !cbs.copy {
			end := sliceEnd(cbs.current.noCopyReadBuffer)
			if buf, has := cbs.recvBuffers.LoadAndDelete(end); has {
				b := buf.(recvBuffer)
				b.pinner.Unpin()
				bufferPool.Put(b.goBuffer)
				C.free(unsafe.Pointer(b.cBuffer))
				receiveBuffers.Add(-1)
			}
		}
	}
	return n, nil
}

type ChainedBuffer struct {
	noCopyReadBuffer []byte
	noCopyReadIndex  int // use index as p[len(p):] reset to the first address
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

	bufferReleaseAccess sync.RWMutex
	recvBuffers         sync.Map //map[uintptr]recvBuffer
	sendBuffers         sync.Map //map[uintptr]sendBuffer
}

func (ss *streamState) needMoreBuffer() bool {
	return ss.recvCount.Load()+bufferSize+(bufferSize/4) >= ss.recvTotal.Load() // always keep 1 extra bufferSize
}

func (ss *streamState) hasReadData() bool {
	return ss.readBuffers.HasData()
}

type MsQuicStream struct {
	ctx          context.Context
	conn, stream C.HQUIC
	cancel       context.CancelFunc
	state        *streamState
	readSignal   chan struct{}
	noAlloc      bool
}

func newMsQuicStream(c, s C.HQUIC, connCtx context.Context, noAlloc, appBuffers bool) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		conn:   c,
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
		noAlloc:    noAlloc,
	}
	res.state.readBuffers.current = &ChainedBuffer{}
	res.state.readBuffers.tail = res.state.readBuffers.current
	res.state.readBuffers.recvBuffers = &res.state.recvBuffers
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
var recvBuffersCount atomic.Int64

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	if !mqs.noAlloc {
		return mqs.copyLessWrite(data)
	}
	return mqs.copyWrite(data)
}

func (mqs MsQuicStream) copyWrite(data []byte) (int, error) {
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
	var n C.int64_t
	bufNum := len(data) / bufferSize
	if len(data)%bufferSize != 0 {
		bufNum += 1
	}
	for range bufNum {
		buf := bufferPool.Get().(*[]byte)
		buffer := *buf
		pinner := runtime.Pinner{}
		m := copy(buffer, data[n:])
		pinner.Pin(unsafe.Pointer(unsafe.SliceData(buffer)))
		idx := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
		mqs.state.bufferReleaseAccess.RLock()
		if mqs.ctx.Err() == nil {
			mqs.state.sendBuffers.Store(idx, sendBuffer{
				goBuffer: buf,
				pinner:   pinner,
			})
			sendBuffersSize.Add(1)
			sendBuffersCount.Add(1)
		} else {
			mqs.state.bufferReleaseAccess.RUnlock()
			mqs.releaseBuffers()
			return int(n), io.ErrUnexpectedEOF
		}
		mqs.state.bufferReleaseAccess.RUnlock()
		sendBuffersCount.Add(1)
		cNoAlloc := C.uint8_t(1)
		nn := cStreamWrite(mqs.conn, mqs.stream,
			(*C.uint8_t)(unsafe.SliceData(buffer[:m])),
			C.int64_t(m),
			cNoAlloc)
		if nn == -1 {
			mqs.releaseBuffers()
			return int(n), fmt.Errorf("write stream error %v", len(data))
		}
		n += nn
	}
	runtime.KeepAlive(data)
	return int(n), nil
}

// copyLessWrite is exclusively used by ReadFrom
// We optimize away the extra copy needed when we skip C alloc
func (mqs MsQuicStream) copyLessWrite(data []byte) (int, error) {
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
	var n C.int64_t
	cNoAlloc := C.uint8_t(0)
	n = cStreamWrite(mqs.conn, mqs.stream,
		(*C.uint8_t)(unsafe.SliceData(data[:])),
		C.int64_t(len(data)),
		cNoAlloc)
	if n == -1 {
		return int(n), fmt.Errorf("write stream error %v", len(data))
	}
	runtime.KeepAlive(data)
	return int(n), nil
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

func (mqs MsQuicStream) appClose() error {
	mqs.state.shutdown.Store(true)
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	mqs.releaseBuffers()
	return nil
}

func (mqs MsQuicStream) abortClose() error {
	if !mqs.state.shutdown.Swap(true) {
		mqs.cancel()
		cAbortStream(mqs.stream)
		mqs.releaseBuffers()
	}
	return nil
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
	if !mqs.noAlloc {
		return mqs.staticReadFrom(r)
	}
	return mqs.dynaReadFrom(r)
}

func (mqs MsQuicStream) staticReadFrom(r io.Reader) (n int64, err error) {
	buf := bufferPool.Get().(*[]byte)
	buffer := *buf
	defer bufferPool.Put(buf)
	for mqs.ctx.Err() == nil {
		bn, err := r.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = mqs.copyLessWrite(buffer[:bn])
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
		mqs.state.bufferReleaseAccess.RLock()
		if mqs.ctx.Err() == nil {
			mqs.state.sendBuffers.Store(idx, sendBuffer{
				goBuffer: buf,
				pinner:   pinner,
			})
			sendBuffersSize.Add(1)
			sendBuffersCount.Add(1)
		} else {
			mqs.state.bufferReleaseAccess.RUnlock()
			mqs.releaseBuffers()
			return n, err
		}
		mqs.state.bufferReleaseAccess.RUnlock()
		bn, err := r.Read(buffer[:])
		if bn != 0 && err == nil {
			var nn int
			nn, err = mqs.copyLessWrite(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			mqs.releaseBuffers()
			return n, err
		}
	}
	return n, io.EOF
}

func (res MsQuicStream) releaseBuffers() {
	res.state.bufferReleaseAccess.Lock()
	defer res.state.bufferReleaseAccess.Unlock()

	res.state.recvBuffers.Range(func(key, _ any) bool {
		if value, has := res.state.recvBuffers.LoadAndDelete(key); has {
			v := value.(recvBuffer)
			v.pinner.Unpin()
			bufferPool.Put(v.goBuffer)
			C.free(unsafe.Pointer(v.cBuffer))
			receiveBuffers.Add(-1)
		}
		return true
	})

	res.state.sendBuffers.Range(func(key, _ any) bool {
		if value, has := res.state.sendBuffers.LoadAndDelete(key); has {
			v := value.(sendBuffer)
			v.pinner.Unpin()
			bufferPool.Put(v.goBuffer)
			sendBuffersSize.Add(-1)
		}
		return true
	})
}
