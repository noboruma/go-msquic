/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "inc/msquic.h"
import "C"

var time1, time2, time3, time4, time5, time6, time7, time8, time9, time10, time11, time12, time13, receiveBuffers, pinSend, pinRecv atomic.Int64

func init() {
	if os.Getenv("QUIC_DEBUG") != "" {
		go func() {
			for {
				<-time.After(15 * time.Second)
				println("newConnectionCa", time1.Swap(0),
					"closeConnection", time2.Swap(0),
					"closePeerConnec", time3.Swap(0),
					"newReadCallback", time4.Swap(0),
					"newStreamCallba", time5.Swap(0),
					"closeStreamCall", time6.Swap(0),
					"startStreamCall", time7.Swap(0),
					"startConnection", time8.Swap(0),
					"streamWrite", time9.Swap(0), "\n",
					"streamRead", time10.Swap(0),
					"streamReadWait", time11.Swap(0),
					"streamReadDataArrived", time12.Swap(0),
					"finalStream", time13.Swap(0), "\n",
					"sendSize", sendBuffersSize.Load(),
					"sendReqs", sendBuffersCount.Swap(0),
					"recvSize", receiveBuffers.Load(),
					"recvReqs", recvBuffersCount.Swap(0),
					"pinSend", pinSend.Swap(0),
					"pinRecv", pinRecv.Swap(0))
			}
		}()
	}
}

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	now := time.Now()
	defer func() {
		time1.Add(time.Since(now).Milliseconds())
	}()
	listener, has := listeners.Load(l)
	if !has {
		return // already closed
	}
	res := newMsQuicConn(c,
		listener.(MsQuicListener).failOnOpenStream,
		listener.(MsQuicListener).noAllocStream,
		listener.(MsQuicListener).appBuffers)

	select {
	case listener.(MsQuicListener).acceptQueue <- res:
		connections.Store(c, res)
	default:
		cAbortConnection(c)
	}
}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	now := time.Now()
	defer func() {
		time2.Add(time.Since(now).Milliseconds())
	}()
	res, has := connections.Load(c)
	if !has {
		return // already closed
	}

	res.(MsQuicConn).appClose()
}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, recvBuffers *C.QUIC_BUFFER, bufferCount C.uint32_t) C.uint32_t {

	now := time.Now()
	defer func() {
		time4.Add(time.Since(now).Milliseconds())
	}()
	rawConn, has := connections.Load(c)
	if !has {
		//cAbortConnection(c)
		//cAbortStream(s)
		return 0 // already closed
	}
	conn := rawConn.(MsQuicConn)
	rawStream, has := conn.streams.Load(s)
	if !has {
		//cAbortStream(s)
		return 0 // already closed
	}

	stream := rawStream.(MsQuicStream)
	state := stream.state

	n := C.uint32_t(0)
	for _, buffer := range unsafe.Slice(recvBuffers, bufferCount) {
		var subBuf []byte
		stream.state.bufferReleaseAccess.RLock()
		if conn.useAppBuffers {
			subBuf = findBuffer(uintptr(unsafe.Pointer(buffer.Buffer)),
				int(buffer.Length),
				&state.recvBuffers)
			if subBuf == nil {
				stream.state.bufferReleaseAccess.RUnlock()
				break
			}
		} else {
			subBuf = unsafe.Slice((*byte)(buffer.Buffer), buffer.Length)
		}
		n += buffer.Length
		state.readBuffers.Write(subBuf)
		stream.state.bufferReleaseAccess.RUnlock()
	}
	select {
	case stream.readSignal <- struct{}{}:
	default:
	}
	if conn.useAppBuffers {
		state.recvCount.Add(uint32(n))
		if state.needMoreBuffer() {
			err := provideAndAttachAppBuffer(s, stream)
			if err != nil {
				stream.releaseBuffers()
				return 0
			}
		}
	} else {
		n = 0
	}
	return n
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	now := time.Now()
	defer func() {
		time5.Add(time.Since(now).Milliseconds())
	}()
	rawConn, has := connections.Load(c)
	if !has {
		//cAbortConnection(c)
		cAbortStream(s)
		return // already closed
	}
	conn := rawConn.(MsQuicConn)
	conn.openStream.RLock()
	defer conn.openStream.RUnlock()
	if conn.ctx.Err() != nil {
		cAbortStream(s)
		return
	}

	res := newMsQuicStream(c, s, conn.ctx, conn.noAlloc, conn.useAppBuffers)

	if conn.useAppBuffers {
		for range initBufs {
			err := provideAndAttachAppBuffer(s, res)
			if err != nil {
				res.releaseBuffers()
				cAbortStream(s)
				return
			}
		}
	}

	select {
	case conn.acceptStreamQueue <- res:
		rawConn.(MsQuicConn).streams.Store(s, res)
	default:
		res.releaseBuffers()
		cAbortStream(s)
	}
}

var attachErr = errors.New("buffer attach error")

func provideAndAttachAppBuffer(s C.HQUIC, res MsQuicStream) error {
	initBuf := provideAppBuffer(res)
	if initBuf == nil || cAttachAppBuffer(s, initBuf) == -1 {
		return attachErr
	}
	res.state.recvTotal.Add(uint32(initBuf.Length))
	return nil
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {
	now := time.Now()
	defer func() {
		time13.Add(time.Since(now).Milliseconds())
	}()
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC already closed connection")
		//cAbortConnection(c)
		//cAbortStream(s)
		return // already closed
	}

	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		//cAbortStream(s)
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.appClose()

	res.(MsQuicStream).releaseBuffers()
}

//export abortStreamCallback
func abortStreamCallback(c, s C.HQUIC) {
	now := time.Now()

	defer func() {
		time6.Add(time.Since(now).Milliseconds())
	}()
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC already closed connection 2")
		//cAbortConnection(c)
		//cAbortStream(s)
		return // already closed
	}

	conn := rawConn.(MsQuicConn)
	res, has := conn.streams.Load(s)
	if !has {
		println("PANIC cannot find stream to abort")
		//cAbortStream(s)
		return // already closed
	}

	res.(MsQuicStream).abortClose()
}

//export startStreamCallback
func startStreamCallback(c, s C.HQUIC) {

	now := time.Now()
	defer func() {
		time7.Add(time.Since(now).Milliseconds())
	}()

	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start stream")
		cAbortConnection(c)
		cAbortStream(s)
		return // already closed
	}
	if rawConn.(MsQuicConn).ctx.Err() != nil {
		cAbortStream(s)
		return
	}
	res, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		println("PANIC no stream for start stream")
		cAbortStream(s)
		return // already closed
	}

	select {
	case res.(MsQuicStream).state.startSignal <- struct{}{}:
	default:
	}
}

//export startConnectionCallback
func startConnectionCallback(c C.HQUIC) {

	now := time.Now()

	defer func() {
		time8.Add(time.Since(now).Milliseconds())
	}()
	res, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start conn")
		cAbortConnection(c)
		return // already closed
	}

	select {
	case res.(MsQuicConn).startSignal <- struct{}{}:
	default:

	}
}

//export freeSendBuffer
func freeSendBuffer(c, s C.HQUIC, buffer *C.uint8_t) {
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for free send")
		//cAbortConnection(c)
		//cAbortStream(s)
		return // already closed
	}
	res, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		println("PANIC cannot find stream to release")
		//cAbortStream(s)
		return // already closed
	}

	idx := uintptr(unsafe.Pointer(buffer))
	if sBuffer, has := res.(MsQuicStream).state.sendBuffers.LoadAndDelete(idx); has {
		v := sBuffer.(sendBuffer)
		v.pinner.Unpin()
		bufferPool.Put(v.goBuffer)
		sendBuffersSize.Add(-1)
	}
}

const bufferSize = 32 * 1024
const initBufs = 2

var bufferPool = sync.Pool{
	New: func() any {
		res := make([]byte, bufferSize)
		return &res
	},
}

func provideAppBuffer(s MsQuicStream) *C.QUIC_BUFFER {

	s.state.bufferReleaseAccess.RLock()
	defer s.state.bufferReleaseAccess.RUnlock()

	if s.ctx.Err() != nil {
		return nil
	}

	goSlicePool := bufferPool.Get().(*[]byte)
	goSlice := *goSlicePool
	sliceUnder := unsafe.Pointer(unsafe.SliceData(goSlice))

	pinner := runtime.Pinner{}
	pinner.Pin(sliceUnder)

	receiveBuffers.Add(1)
	recvBuffersCount.Add(1)
	endAddr := uintptr(unsafe.Add(sliceUnder, len(goSlice)))

	size := unsafe.Sizeof(C.QUIC_BUFFER{})
	res := (*C.QUIC_BUFFER)(C.malloc(C.size_t(size)))
	pinner.Pin(res)
	res.Buffer = (*C.uint8_t)(sliceUnder)
	res.Length = C.uint32_t(len(goSlice))

	s.state.recvBuffers.Store(endAddr, recvBuffer{
		goBuffer: goSlicePool,
		cBuffer:  res,
		pinner:   pinner,
	})

	return res
}

//export newDatagramCallback
func newDatagramCallback(c C.HQUIC, recvBuffer *C.QUIC_BUFFER) {
	rawConn, has := connections.Load(c)
	if !has {
		cAbortConnection(c)
		return // already closed
	}

	conn := rawConn.(MsQuicConn)

	subBuf := unsafe.Slice((*byte)(recvBuffer.Buffer), recvBuffer.Length)
	msg := make([]byte, recvBuffer.Length)
	copy(msg, subBuf)

	conn.datagrams <- msg
}

//export closePeerConnectionCallback
func closePeerConnectionCallback(c C.HQUIC) {

	now := time.Now()
	defer func() {
		time3.Add(time.Since(now).Milliseconds())
	}()
	res, has := connections.Load(c)

	if !has {
		cAbortConnection(c)
		return // already closed
	}

	res.(MsQuicConn).peerClose()
}
