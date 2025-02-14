package quic

/*

#cgo pkg-config: msquic

#cgo noescape StreamWrite

#cgo nocallback ShutdownConnection
#cgo nocallback ShutdownStream
#cgo nocallback OpenStream
#cgo nocallback LoadListenConfiguration
#cgo nocallback Listen
#cgo nocallback CloseListener
#cgo nocallback DialConnection
#cgo nocallback MsQuicSetup

#cgo nocallback GetRemoteAddr
#cgo nocallback StreamWrite

#include "c/msquic.c"

*/
import "C"
import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var totalOpenedStreams atomic.Int64
var totalOpenedConnections atomic.Int64
var totalClosedStreams atomic.Int64
var totalClosedConnections atomic.Int64

var streams sync.Map     //map[C.HQUIC]MsQuicStream
var listeners sync.Map   //map[C.HQUIC]MsQuicListener
var connections sync.Map //map[C.HQUIC]MsQuicConn

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

// #cgo noescape newReadCallback

//export newReadCallback
func newReadCallback(s C.HQUIC, buffer *C.uint8_t, length C.int64_t) {
	rawStream, has := streams.Load(s)
	if !has {
		return // already closed
	}
	stream := rawStream.(MsQuicStream)
	stream.buffer.m.Lock()
	defer stream.buffer.m.Unlock()

	goBuffer := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), length)
	_, err := stream.buffer.buffer.Write(goBuffer)
	if err != nil {
		panic(err.Error()) // not enough RAM
	}
	select {
	case stream.buffer.signal <- struct{}{}:
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
	totalClosedStreams.Add(1)
	res.(MsQuicStream).remoteClose()
}

func init() {
	status := C.MsQuicSetup()
	if status != 0 {
		panic(fmt.Sprintf("failed to load quic: %d", status))
	}
	go func() {
		for {
			<-time.After(5 * time.Second)
			sCount := 0
			streams.Range(func(_, _ any) bool {
				sCount += 1
				return true
			})
			cCount := 0
			connections.Range(func(_, _ any) bool {
				cCount += 1
				return true
			})
			println("streams: ", sCount, "connections:", cCount)
			println("totalOpenedStreams: ", totalOpenedStreams.Load(), "totalClosedStreams:", totalClosedStreams.Load())
			println("totalOpenedConns: ", totalOpenedConnections.Load(), "totalClosedConns:", totalClosedConnections.Load())
		}
	}()
}

func ListenAddr(addr string, cfg Config) (MsQuicListener, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return MsQuicListener{}, err
	}
	portInt, _ := strconv.Atoi(port)

	if cfg.KeyFile == "" {
		return MsQuicListener{}, fmt.Errorf("No TLS key files provided")
	}

	if cfg.CertFile == "" {
		return MsQuicListener{}, fmt.Errorf("No TLS cert files provided")
	}

	cAddr := C.CString(host)
	defer C.free(unsafe.Pointer(cAddr))
	cKeyFile := C.CString(cfg.KeyFile)
	cCertFile := C.CString(cfg.CertFile)
	cAlpn := C.CString(cfg.Alpn)

	buffer := C.struct_QUIC_BUFFER{
		Length: C.uint32_t(len(cfg.Alpn)),
		Buffer: (*C.uint8_t)(unsafe.Pointer(cAlpn)),
	}

	config := C.LoadListenConfiguration(C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout),
		keyFile:                      cKeyFile,
		certFile:                     cCertFile,
		Alpn:                         buffer,
	})

	listener := C.Listen(cAddr, C.uint16_t(portInt), config, buffer)
	if listener == nil {
		return MsQuicListener{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicListener(listener, config, cKeyFile, cCertFile, cAlpn)
	listeners.Store(listener, res)
	return res, nil
}

func DialAddr(ctx context.Context, addr string, cfg Config) (MsQuicConn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return MsQuicConn{}, err
	}
	portInt, _ := strconv.Atoi(port)

	cAddr := C.CString(host)
	defer C.free(unsafe.Pointer(cAddr))

	keepAliveMs := cfg.KeepAlivePeriod
	if keepAliveMs > cfg.MaxIdleTimeout {
		keepAliveMs = cfg.MaxIdleTimeout / 2
	}

	cAlpn := C.CString(cfg.Alpn)
	defer C.free(unsafe.Pointer(cAlpn))

	buffer := C.struct_QUIC_BUFFER{
		Length: C.uint32_t(len(cfg.Alpn)),
		Buffer: (*C.uint8_t)(unsafe.Pointer(cAlpn)),
	}
	totalOpenedConnections.Add(1)

	conn := C.DialConnection(cAddr, C.uint16_t(portInt), C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout),
		KeepAliveMs:                  C.int(keepAliveMs),
		Alpn:                         buffer,
	})
	if conn == nil {
		return MsQuicConn{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicConn(conn)
	connections.Store(conn, res)
	return res, nil
}

func cCloseListener(listener, config C.HQUIC) {
	C.CloseListener(listener, config)
}
func cShutdownStream(s C.HQUIC) {
	C.ShutdownStream(s)
}
func cStreamWrite(s C.HQUIC, cArray *C.uint8_t, size C.int64_t) C.int64_t {
	return C.StreamWrite(s, cArray, size)
}

func cOpenStream(c C.HQUIC) C.HQUIC {
	return C.OpenStream(c)
}

func cShutdownConnection(c C.HQUIC) {
	C.ShutdownConnection(c)
}

func getRemoteAddr(c C.HQUIC) (net.IP, int) {
	var addr C.struct_sockaddr_storage
	var addrLen C.uint32_t = C.uint32_t(unsafe.Sizeof(addr))

	if C.GetRemoteAddr(c, &addr, &addrLen) != 0 {
		return nil, 0
	}

	var (
		ip   net.IP
		port int
	)
	switch addr.ss_family {
	case C.AF_INET:
		ip = net.IPv4(
			byte(addr.__ss_padding[2]),
			byte(addr.__ss_padding[3]),
			byte(addr.__ss_padding[4]),
			byte(addr.__ss_padding[5]),
		)
		port = (int(addr.__ss_padding[0]) << 8) | int(addr.__ss_padding[1])
	default:
	}

	return ip, port
}
