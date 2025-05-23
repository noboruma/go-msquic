/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import (
	"unsafe"
)

// #include "msquic.h"
import "C"

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		return // already closed
	}
	res := newMsQuicConn(c, listener.(MsQuicListener).failOnOpenStream)

	select {
	case listener.(MsQuicListener).acceptQueue <- res:
		connections.Store(c, res)
	default:
		cAbortConnection(c)
	}
}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	res, has := connections.LoadAndDelete(c)
	if !has {
		return // already closed
	}
	res.(MsQuicConn).appClose()
}

//export closePeerConnectionCallback
func closePeerConnectionCallback(c C.HQUIC) {

	res, has := connections.Load(c)

	if !has {
		return // already closed
	}

	res.(MsQuicConn).peerClose()
}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, buffers *C.QUIC_BUFFER, bufferCount C.uint32_t) {

	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawConn.(MsQuicConn).openStream.RLock()
	defer rawConn.(MsQuicConn).openStream.RUnlock()
	rawStream, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		return // already closed
	}

	stream := rawStream.(MsQuicStream)
	state := stream.state

	state.readBufferAccess.Lock()
	for _, buffer := range unsafe.Slice(buffers, bufferCount) {
		state.readBuffer.Write(unsafe.Slice((*byte)(buffer.Buffer), buffer.Length))
	}
	state.readBufferAccess.Unlock()
	select {
	case stream.readSignal <- struct{}{}:
	default:
	}
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	rawConn, has := connections.Load(c)
	if !has {
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

	res := newMsQuicStream(s, conn.ctx)
	select {
	case conn.acceptStreamQueue <- res:
		rawConn.(MsQuicConn).streams.Store(s, res)
	default:
		cAbortStream(s)
	}
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawConn.(MsQuicConn).openStream.Lock()
	defer rawConn.(MsQuicConn).openStream.Unlock()
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.appClose()

}

//export startStreamCallback
func startStreamCallback(c, s C.HQUIC) {

	rawConn, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start stream")
		return // already closed
	}
	rawConn.(MsQuicConn).openStream.RLock()
	defer rawConn.(MsQuicConn).openStream.RUnlock()
	res, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		println("PANIC no stream for start stream")
		return // already closed
	}

	select {
	case res.(MsQuicStream).state.startSignal <- struct{}{}:
	default:
	}
}

//export startConnectionCallback
func startConnectionCallback(c C.HQUIC) {

	res, has := connections.Load(c)
	if !has {
		println("PANIC no conn for start conn")
		return // already closed
	}

	select {
	case res.(MsQuicConn).startSignal <- struct{}{}:
	default:
	}
}
