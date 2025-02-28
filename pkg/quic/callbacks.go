/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import "unsafe"

// #include "msquic.h"
import "C"

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		return // already closed
	}
	totalOpenedConnections.Add(1)
	res := newMsQuicConn(c)
	connections.Store(c, res)
	listener.(MsQuicListener).acceptQueue <- res
}

//export closeConnectionCallback
func closeConnectionCallback(c C.HQUIC) {
	res, has := connections.LoadAndDelete(c)
	if !has {
		return // already closed
	}
	totalClosedConnections.Add(1)
	res.(MsQuicConn).remoteClose()
}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, buffer *C.uint8_t, length C.int64_t) {
	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawStream, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		return // already closed
	}

	stream := rawStream.(MsQuicStream)
	state := stream.state
	state.readBufferAccess.Lock()
	defer state.readBufferAccess.Unlock()

	if length > 0 {
		goBuffer := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), length)
		_, err := state.readBuffer.Write(goBuffer)
		if err != nil {
			println("not enough RAM to achieve: ", length)
			panic(err.Error())
		}
	}
	select {
	case stream.readSignal <- struct{}{}:
	default:
	}
}

//export completeWriteCallback
func completeWriteCallback(c, s C.HQUIC) {
	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	rawStream, has := rawConn.(MsQuicConn).streams.Load(s)
	if !has {
		return // already closed

	}
	stream := rawStream.(MsQuicStream)
	select {
	case stream.writeSignal <- struct{}{}:
	default:
	}
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	totalOpenedStreams.Add(1)
	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	conn := rawConn.(MsQuicConn)
	res := newMsQuicStream(s, conn.ctx)
	rawConn.(MsQuicConn).streams.Store(s, res)
	conn.acceptStreamQueue <- res
}

//export closeStreamCallback
func closeStreamCallback(c, s C.HQUIC) {
	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	res, has := rawConn.(MsQuicConn).streams.LoadAndDelete(s)
	if !has {
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.remoteClose()

	totalClosedStreams.Add(1)
}
