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

var time1, time2, time3, time4, time5, time6, time7, time8, time9, time10, time11, time12, time13, receiveBuffers, bufFound, bufNotfound, startSucc, startFail, open, wait, read, write, release atomic.Int64

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
					"bufFound", bufFound.Swap(0),
					"bufNotFound", bufNotfound.Swap(0),
					"startSucc", startSucc.Swap(0),
					"startFail", startFail.Swap(0),
					"open:", open.Load(),
					"wait", wait.Load(),
					"release:", release.Load(),
					"read:", read.Load(),
					"werite", write.Load())
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
		println("PANIC new no listener")
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
		println("WARNING rejecting connection")
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
		println("PANIC close conn no conn")
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
		println("PANIC new read stream no conn")
		//cAbortConnection(c)
		//cAbortStream(s)
		return 0 // already closed
	}
	conn := rawConn.(MsQuicConn)
	rawStream, has := conn.state.streams.Load(s)
	if !has {
		//cAbortStream(s)
		println("PANIC new read stream no stream")
		return 0 // already closed
	}

	stream := rawStream.(MsQuicStream)
	state := stream.state

	state.readAccess.RLock()
	defer state.readAccess.RUnlock()
	if stream.ctx.Err() != nil {
		return 0
	}

	n := C.uint32_t(0)
	for _, buffer := range unsafe.Slice(recvBuffers, bufferCount) {
		var subBuf []byte
		if conn.useAppBuffers {
			subBuf = findBuffer(uintptr(unsafe.Pointer(buffer.Buffer)),
				int(buffer.Length),
				&state.attachedRecvBuffers)
			if subBuf == nil {
				//abortStreamCallback(c, s)
				return 0
			}
		} else {
			subBuf = unsafe.Slice((*byte)(buffer.Buffer), buffer.Length)
		}
		n += buffer.Length
		state.readBuffers.Write(subBuf)
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
				//abortStreamCallback(c, s)
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
		println("PANIC new stream no conn")
		//cAbortConnection(c)
		cAbortStream(s)
		return // already closed
	}
	conn := rawConn.(MsQuicConn)
	conn.state.openStream.RLock()
	defer conn.state.openStream.RUnlock()
	if conn.ctx.Err() != nil {
		println("PANIC new stream but closing conn")
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
		rawConn.(MsQuicConn).state.streams.Store(s, res)
	default:
		println("WARNING rejecting stream")
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
	res.state.recvTotal.Add(uint32(len(initBuf)))
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

	res, has := rawConn.(MsQuicConn).state.streams.LoadAndDelete(s)
	if !has {
		println("PANIC already close stream")
		//cAbortStream(s)
		return // already closed

	}

	stream := res.(MsQuicStream)
	stream.release()
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
	res, has := conn.state.streams.Load(s)
	if !has {
		println("PANIC cannot find stream to abort")
		//cAbortStream(s)
		return // already closed
	}

	res.(MsQuicStream).abortClose()
}

//export shutConnectionCallback
func shutConnectionCallback(c C.HQUIC) {
	now := time.Now()

	defer func() {
		time6.Add(time.Since(now).Milliseconds())
	}()
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC already closed connection 3")
		return // already closed
	}

	conn := rawConn.(MsQuicConn)
	conn.Close()
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
		//cAbortConnection(c)
		//cAbortStream(s)
		return // already closed
	}

	res, has := rawConn.(MsQuicConn).state.streams.Load(s)
	if !has {
		println("PANIC no stream for start stream")
		//cAbortStream(s)
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
		//cAbortConnection(c)
		return // already closed
	}

	select {
	case res.(MsQuicConn).startSignal <- struct{}{}:
	default:

	}
}

//export freeSendBuffer
func freeSendBuffer(idx uintptr) {
	releaseSendBuffer(idx)
}

const defaultReceiveBufferSize = 32 * 1024
const defaultSendBufferSize = 4 * 1024
const initReceiveBufs = 2

var receiveBufferSize = defaultReceiveBufferSize
var sendBufferSize = defaultSendBufferSize
var initBufs = initReceiveBufs

var recvBufferPool = sync.Pool{
	New: func() any {
		res := make([]byte, receiveBufferSize)
		return &res
	},
}

var sendBufferPool = sync.Pool{
	New: func() any {
		res := make([]byte, sendBufferSize)
		return &res
	},
}

func provideAppBuffer(s MsQuicStream) []byte {

	goSlice := recvBufferPool.Get().(*[]byte)
	sliceUnder := unsafe.Pointer(unsafe.SliceData(*goSlice))

	pinner := runtime.Pinner{}
	pinner.Pin(sliceUnder)

	receiveBuffers.Add(1)
	recvBuffersCount.Add(1)
	endAddr := sliceEnd(*goSlice)

	s.state.attachedRecvBuffers.access.Lock()
	s.state.attachedRecvBuffers.buffers = append(s.state.attachedRecvBuffers.buffers, sliceAddresses{
		start: uintptr(sliceUnder),
		end:   endAddr,
	})
	s.state.attachedRecvBuffers.access.Unlock()

	recvBuffers.Store(endAddr, escapingBuffer{
		goBuffer: goSlice,
		pinner:   pinner,
	})

	return *goSlice
}

//export newDatagramCallback
func newDatagramCallback(c C.HQUIC, recvBuffer *C.QUIC_BUFFER) {
	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for datagram")
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
		println("PANIC no conn for peer close")
		//cAbortConnection(c)
		return // already closed
	}

	res.(MsQuicConn).peerClose()
}

//export peerAddressChangedCallback
func peerAddressChangedCallback(c C.HQUIC) {
	res, has := connections.Load(c)
	if !has {
		println("PANIC no conn for peer change addr")
		return // already closed
	}
	res.(MsQuicConn).state.dirtyRemote.Store(true)
}
