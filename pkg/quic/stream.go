package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
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
	current         *ChainedBuffer
	tail            *ChainedBuffer
	copy            bool
	readAccess      *sync.RWMutex
	ctx             context.Context
	attachedBuffers *attachedBuffers
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

func findBuffer(current uintptr, length int, buffers *attachedBuffers) []byte {
	var buffer []byte
	var offsetStart int
	buffers.access.RLock()
	defer buffers.access.RUnlock()
	currentEnd := current + uintptr(length)
	for _, buf := range buffers.buffers {
		start := buf.start
		end := buf.end
		if current >= start && current < end && currentEnd > start && currentEnd <= end {
			if rBuffer, has := recvBuffers.Load(end); has {
				buffer = *rBuffer.(escapingBuffer).goBuffer
				offsetStart = int(uintptr(current) - uintptr(start))
			}
			break
		}
	}
	if buffer != nil {
		pinRecv.Add(1)
		return buffer[offsetStart : offsetStart+length]
	}
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
	cbs.readAccess.RLock()
	defer cbs.readAccess.RUnlock()
	if cbs.ctx.Err() != nil {
		return 0, io.EOF
	}
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
			if freeRecvBuffer(end) {
				cbs.attachedBuffers.access.Lock()
				cbs.attachedBuffers.buffers = slices.DeleteFunc(cbs.attachedBuffers.buffers, func(b sliceAddresses) bool {
					return b.end == end
				})
				cbs.attachedBuffers.access.Unlock()
			}
		}
	}
	return n, nil
}

func freeRecvBuffer(end uintptr) bool {
	if buf, has := recvBuffers.LoadAndDelete(end); has {
		b := buf.(escapingBuffer)
		b.pinner.Unpin()
		bufferPool.Put(b.goBuffer)
		receiveBuffers.Add(-1)
		return true
	}
	return false
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

type escapingBuffer struct {
	goBuffer *[]byte
	pinner   runtime.Pinner
}

type streamState struct {
	readBuffers   ChainedBuffers
	writeDeadline time.Time
	writeAccess   sync.RWMutex
	startSignal   chan struct{}
	shutdown      atomic.Bool
	recvCount     atomic.Uint32
	recvTotal     atomic.Uint32

	readAccess          sync.RWMutex
	readDeadlineContext context.Context
	readDeadlineCancel  context.CancelFunc

	attachedRecvBuffers attachedBuffers

	closingAccess sync.Mutex
}

type attachedBuffers struct {
	access  sync.RWMutex
	buffers []sliceAddresses
}

type sliceAddresses struct {
	start uintptr
	end   uintptr
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
			attachedRecvBuffers: attachedBuffers{
				buffers: []sliceAddresses{},
			},
		},
		readSignal: make(chan struct{}, 1),
		noAlloc:    noAlloc,
	}
	res.state.readBuffers.current = &ChainedBuffer{}
	res.state.readBuffers.tail = res.state.readBuffers.current
	res.state.readBuffers.ctx = ctx
	res.state.readBuffers.readAccess = &res.state.readAccess
	res.state.readBuffers.attachedBuffers = &res.state.attachedRecvBuffers
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
		return mqs.cCopyWrite(data)
	}
	return mqs.goCopyWrite(data)
}

func (mqs MsQuicStream) goCopyWrite(data []byte) (int, error) {
	now := time.Now()
	defer func() {
		time9.Add(time.Since(now).Milliseconds())
	}()
	state := mqs.state
	ctx := mqs.ctx
	mqs.state.writeAccess.RLock()
	defer mqs.state.writeAccess.RUnlock()
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
		buffer := getSendBuffer()
		m := copy(buffer, data[n:])
		sendBuffersCount.Add(1)
		cNoAlloc := C.uint8_t(1)
		nn := cStreamWrite(mqs.conn, mqs.stream,
			(*C.uint8_t)(unsafe.SliceData(buffer)),
			C.int64_t(m),
			cNoAlloc)
		runtime.KeepAlive(buffer)
		if nn == -1 {
			idx := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
			releaseSendBuffer(idx)
			return int(n), fmt.Errorf("write stream error %v", len(data))
		}
		n += nn
	}
	return int(n), nil
}

func (mqs MsQuicStream) cCopyWrite(data []byte) (int, error) {
	now := time.Now()
	defer func() {
		time9.Add(time.Since(now).Milliseconds())
	}()
	state := mqs.state
	ctx := mqs.ctx
	mqs.state.writeAccess.RLock()
	defer mqs.state.writeAccess.RUnlock()
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
		(*C.uint8_t)(unsafe.SliceData(data)),
		C.int64_t(len(data)),
		cNoAlloc)
	runtime.KeepAlive(data)
	if n == -1 {
		return int(n), fmt.Errorf("write stream error %v", len(data))
	}
	return int(n), nil
}

// copyLessWrite is exclusively used by ReadFrom
// We optimize away the extra copy needed when we skip C alloc
func (mqs MsQuicStream) noCopyWrite(data []byte) (int, error) {
	now := time.Now()
	defer func() {
		time9.Add(time.Since(now).Milliseconds())
	}()
	state := mqs.state
	ctx := mqs.ctx
	mqs.state.writeAccess.RLock()
	defer mqs.state.writeAccess.RUnlock()
	if ctx.Err() != nil {
		return 0, io.EOF
	}
	deadline := state.writeDeadline
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
	}
	cNoAlloc := C.uint8_t(1)
	n := cStreamWrite(mqs.conn, mqs.stream,
		(*C.uint8_t)(unsafe.SliceData(data)),
		C.int64_t(len(data)),
		cNoAlloc)
	runtime.KeepAlive(data)
	if n == -1 {
		return int(n), fmt.Errorf("write stream error %v", len(data))
	}
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
	return mqs.abortClose()
}

func (mqs MsQuicStream) release() error {
	mqs.state.closingAccess.Lock()
	defer mqs.state.closingAccess.Unlock()
	mqs.state.shutdown.Store(true)
	mqs.cancel()
	mqs.operationsBarrier()
	mqs.releaseBuffers()

	return nil
}

func (mqs MsQuicStream) operationsBarrier() {
	mqs.state.writeAccess.Lock()
	mqs.state.writeAccess.Unlock()
	mqs.state.readAccess.Lock()
	mqs.state.readAccess.Unlock()
}

func (mqs MsQuicStream) abortClose() error {
	mqs.state.closingAccess.Lock()
	defer mqs.state.closingAccess.Unlock()
	if !mqs.state.shutdown.Swap(true) {
		mqs.cancel()
		cAbortStream(mqs.stream)
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
			nn, err = mqs.cCopyWrite(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			return n, err
		}
	}
	return n, io.EOF
}

func getSendBuffer() []byte {
	buf := smallBufferPool.Get().(*[]byte)
	buffer := *buf
	pinner := runtime.Pinner{}
	pinner.Pin(unsafe.Pointer(unsafe.SliceData(buffer)))
	idx := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
	if _, loaded := sendBuffers.LoadOrStore(idx, escapingBuffer{
		goBuffer: buf,
		pinner:   pinner,
	}); loaded {
		println("PANIC send buffer corruption")
	}
	sendBuffersSize.Add(1)
	sendBuffersCount.Add(1)
	return buffer
}

func releaseSendBuffer(idx uintptr) {
	if sBuffer, has := sendBuffers.LoadAndDelete(idx); has {
		v := sBuffer.(escapingBuffer)
		v.pinner.Unpin()
		smallBufferPool.Put(v.goBuffer)
		sendBuffersSize.Add(-1)
	} else {
		println("PANIC no buffer to free")
	}
}

func (mqs MsQuicStream) dynaReadFrom(r io.Reader) (n int64, err error) {
	for mqs.ctx.Err() == nil {
		buffer := getSendBuffer()
		bn, err := r.Read(buffer[:])
		if bn != 0 && err == nil {
			var nn int
			nn, err = mqs.noCopyWrite(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			idx := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
			releaseSendBuffer(idx)
			return n, err
		}
	}
	return n, io.EOF
}

var sendBuffers sync.Map //map[uintptr]escapingBuffer
var recvBuffers sync.Map //map[uintptr]escapingBuffer

func (res MsQuicStream) releaseBuffers() {

	res.state.attachedRecvBuffers.access.Lock()
	defer res.state.attachedRecvBuffers.access.Unlock()
	for _, buf := range res.state.attachedRecvBuffers.buffers {
		freeRecvBuffer(buf.end)
	}
	res.state.attachedRecvBuffers.buffers = nil

	//res.state.sendBuffers.Range(func(key, _ any) bool {
	//	if value, has := res.state.sendBuffers.LoadAndDelete(key); has {
	//		println("INFO moving to lingering")
	//		lingeringSends.Store(key, value)
	//	}
	//	return true
	//})
}
