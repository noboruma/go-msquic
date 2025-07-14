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
	CloseRead() error
	SetDeadline(ttl time.Time) error
	SetReadDeadline(ttl time.Time) error
	SetWriteDeadline(ttl time.Time) error
	Context() context.Context
}

type chainedBuffers struct {
	current         *chainedBuffer
	tail            *chainedBuffer
	copy            bool
	ctx             context.Context
	attachedBuffers *attachedBuffers
}

func (cbs *chainedBuffers) HasData() bool {
	return cbs.current.HasData()
}

func (cbs *chainedBuffers) Write(batch []byte) {
	next := chainedBuffer{}
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
	var buffer *[]byte
	var offsetStart int
	buffers.access.RLock()
	defer buffers.access.RUnlock()
	currentEnd := current + uintptr(length) - 1
	for _, buf := range buffers.buffers {
		start := buf.start
		end := buf.end
		if current >= start && current < end && currentEnd > start && currentEnd <= end {
			if rBuffer, has := recvBuffers.Load(end); has {
				buffer = rBuffer.(escapingBuffer).goBuffer
				offsetStart = int(uintptr(current) - uintptr(start))
			}
			break
		}
	}
	if buffer != nil {
		bufFound.Add(1)
		return (*buffer)[offsetStart : offsetStart+length]
	}
	bufNotfound.Add(1)
	return nil
}

func sliceEnd(subBuf []byte) uintptr {
	current := unsafe.Pointer(unsafe.SliceData(subBuf))
	currentEnd := unsafe.Add(current, len(subBuf)-1)
	return uintptr(currentEnd)
}

func (cbs *chainedBuffers) Clear() {
	cur := cbs.current
	for cur != nil {
		cur.noCopyReadBuffer = nil
		cur = cur.next.Swap(nil)
	}
}

func (cbs *chainedBuffers) Read(output []byte) (int, error) {
	n := 0
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
		recvBufferPool.Put(b.goBuffer)
		receiveBuffers.Add(-1)
		return true
	}
	return false
}

type chainedBuffer struct {
	noCopyReadBuffer []byte
	noCopyReadIndex  int // use index as p[len(p):] reset to the first address
	copyReadBuffer   bytes.Buffer
	next             atomic.Pointer[chainedBuffer]
	empty            atomic.Bool
	copy             bool
}

func (cb *chainedBuffer) HasData() bool {
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
	readBuffers   chainedBuffers
	writeDeadline time.Time
	startSignal   chan struct{}
	shutdown      atomic.Bool
	readShutdown  atomic.Bool
	recvCount     atomic.Uint32
	recvTotal     atomic.Uint32

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
	bufferSize := uint32(receiveBufferSize)
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
			readBuffers: chainedBuffers{
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
	res.state.readBuffers.current = &chainedBuffer{}
	res.state.readBuffers.tail = res.state.readBuffers.current
	res.state.readBuffers.ctx = ctx
	res.state.readBuffers.attachedBuffers = &res.state.attachedRecvBuffers
	return res
}

func (mqs MsQuicStream) waitStart() bool {
	select {
	case <-mqs.state.startSignal:
		return true
	case <-mqs.ctx.Done():
	}
	return false
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	read.Add(1)
	defer read.Add(-1)
	now := time.Now()
	defer func() {
		time10.Add(time.Since(now).Milliseconds())
	}()
	state := mqs.state
	if mqs.state.readShutdown.Load() {
		return 0, io.EOF
	}
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
	wait.Add(1)
	defer wait.Add(-1)
	select {
	case <-ctx.Done():
		return false
	case <-mqs.readSignal:
		return !mqs.state.readShutdown.Load()
	}
}

var sendBuffersSize atomic.Int64
var sendBuffersCount atomic.Int64
var recvBuffersCount atomic.Int64

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	write.Add(1)
	defer write.Add(-1)
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
	bufNum := len(data) / sendBufferSize
	if len(data)%sendBufferSize != 0 {
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
		return 0, fmt.Errorf("write stream error %v", len(data))
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
		return 0, fmt.Errorf("write stream error %v", len(data))
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

func (mqs MsQuicStream) CloseRead() error {
	return mqs.sendClose()
}

func (mqs MsQuicStream) release() error {
	release.Add(1)
	defer release.Add(-1)
	mqs.state.closingAccess.Lock()
	defer mqs.state.closingAccess.Unlock()
	mqs.state.shutdown.Store(true)
	mqs.cancel()
	mqs.releaseBuffers()
	return nil
}

func (mqs MsQuicStream) sendClose() error {
	mqs.state.closingAccess.Lock()
	defer mqs.state.closingAccess.Unlock()
	if !mqs.state.shutdown.Load() {
		cAbortSendStream(mqs.stream)
		mqs.state.readShutdown.Store(true)
		select {
		case mqs.readSignal <- struct{}{}:
		default:
		}
	}
	return nil
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
	buf := recvBufferPool.Get().(*[]byte)
	buffer := *buf
	defer recvBufferPool.Put(buf)
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
	buf := recvBufferPool.Get().(*[]byte)
	buffer := *buf
	defer recvBufferPool.Put(buf)
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
	buf := sendBufferPool.Get().(*[]byte)
	pinner := runtime.Pinner{}
	pinner.Pin(unsafe.Pointer(unsafe.SliceData(*buf)))
	idx := uintptr(unsafe.Pointer(unsafe.SliceData(*buf)))
	if _, loaded := sendBuffers.LoadOrStore(idx, escapingBuffer{
		goBuffer: buf,
		pinner:   pinner,
	}); loaded {
		println("PANIC send buffer corruption")
	}
	sendBuffersSize.Add(1)
	sendBuffersCount.Add(1)
	return *buf
}

func releaseSendBuffer(idx uintptr) {
	if sBuffer, has := sendBuffers.LoadAndDelete(idx); has {
		v := sBuffer.(escapingBuffer)
		v.pinner.Unpin()
		sendBufferPool.Put(v.goBuffer)
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
	res.state.readBuffers.Clear()
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
