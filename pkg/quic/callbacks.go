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
func newReadCallback(s C.HQUIC, buffer *C.uint8_t, length C.int64_t) {
	rawStream, has := streams.Load(s)
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
func completeWriteCallback(s C.HQUIC) {
	rawStream, has := streams.Load(s)
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
func newStreamCallback(c C.HQUIC, s C.HQUIC) {
	totalOpenedStreams.Add(1)
	rawConn, has := connections.Load(c)
	if !has {
		return // already closed
	}
	conn := rawConn.(MsQuicConn)
	res := newMsQuicStream(s, conn.ctx)
	streams.Store(s, res)
	conn.acceptStreamQueue <- res
}

//export closeStreamCallback
func closeStreamCallback(s C.HQUIC) {
	res, has := streams.LoadAndDelete(s)
	if !has {
		return // already closed
	}

	stream := res.(MsQuicStream)
	stream.remoteClose()
	streamStatePool.Put(stream.state)

	totalClosedStreams.Add(1)
}
