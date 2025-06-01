/* export duplicates preambles. This is why callbacks are separated from msquic.c */
package quic

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "msquic.h"
import "C"

var time1, time2, time3, time4, time5, time6, time7, time8, time9, time10, time11, time12 atomic.Int64

func init() {
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
				"streamReadDataArrived", time12.Swap(0))
		}
	}()
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
	now := time.Now()
	defer func() {
		time2.Add(time.Since(now).Milliseconds())
	}()
	res, has := connections.LoadAndDelete(c)
	if !has {
		return // already closed
	}

	res.(MsQuicConn).appClose()
}

//export closePeerConnectionCallback
func closePeerConnectionCallback(c C.HQUIC) {

	now := time.Now()
	defer func() {
		time3.Add(time.Since(now).Milliseconds())
	}()
	res, has := connections.Load(c)

	if !has {
		return // already closed
	}

	res.(MsQuicConn).peerClose()
}

//export newReadCallback
func newReadCallback(c, s C.HQUIC, buffers *C.QUIC_BUFFER, bufferCount C.uint32_t) {

	now := time.Now()
	defer func() {
		time4.Add(time.Since(now).Milliseconds())
	}()
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

	//now := time.Now()
	total := 0
	var batch [][]byte
	for _, buffer := range unsafe.Slice(buffers, bufferCount) {
		batch = append(batch, unsafe.Slice((*byte)(buffer.Buffer), buffer.Length))
		total += int(buffer.Length)
	}
	state.readBuffers.Write(batch)
	//println("write took", time.Since(now).Milliseconds(), "ms", "size:", total)
	select {
	case stream.readSignal <- struct{}{}:
	default:
	}
}

//export newStreamCallback
func newStreamCallback(c, s C.HQUIC) {
	now := time.Now()
	defer func() {
		time5.Add(time.Since(now).Milliseconds())
	}()
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

	now := time.Now()
	defer func() {
		time6.Add(time.Since(now).Milliseconds())
	}()
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

	now := time.Now()
	defer func() {
		time7.Add(time.Since(now).Milliseconds())
	}()
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

	now := time.Now()
	defer func() {
		time8.Add(time.Since(now).Milliseconds())
	}()
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
