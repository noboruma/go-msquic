package quic

/*

#cgo pkg-config: msquic
#cgo noescape ShutdownConnection
#cgo noescape AbortStream
#cgo noescape CreateStream
#cgo noescape LoadListenConfiguration
#cgo noescape CreateListener
#cgo noescape StartListener
#cgo noescape CloseListener
#cgo noescape OpenConnection
#cgo noescape StartConnection
#cgo noescape MsQuicSetup
#cgo noescape GetRemoteAddr

#cgo nocallback ShutdownConnection
#cgo nocallback AbortStream
#cgo nocallback CreateStream
#cgo nocallback LoadListenConfiguration
#cgo nocallback CreateListener
#cgo nocallback StartListener
#cgo nocallback CloseListener
#cgo nocallback OpenConnection
#cgo nocallback StartConnection
#cgo nocallback MsQuicSetup
#cgo nocallback GetRemoteAddr
#cgo nocallback StreamWrite
#cgo nocallback AttachAppBuffer

#include "c/msquic.c"
*/
import "C"

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

var listeners sync.Map   //map[C.HQUIC]MsQuicListener
var connections sync.Map //map[C.HQUIC]MsQuicConn

var perfCounterNames = []string{
	"QUIC_PERF_COUNTER_CONN_CREATED",
	"QUIC_PERF_COUNTER_CONN_HANDSHAKE_FAIL",
	"QUIC_PERF_COUNTER_CONN_APP_REJECT",
	"QUIC_PERF_COUNTER_CONN_RESUMED",
	"QUIC_PERF_COUNTER_CONN_ACTIVE",
	"QUIC_PERF_COUNTER_CONN_CONNECTED",
	"QUIC_PERF_COUNTER_CONN_PROTOCOL_ERRORS",
	"QUIC_PERF_COUNTER_CONN_NO_ALPN",
	"QUIC_PERF_COUNTER_STRM_ACTIVE",
	"QUIC_PERF_COUNTER_PKTS_SUSPECTED_LOST",
	"QUIC_PERF_COUNTER_PKTS_DROPPED",
	"QUIC_PERF_COUNTER_PKTS_DECRYPTION_FAIL",
	"QUIC_PERF_COUNTER_UDP_RECV",
	"QUIC_PERF_COUNTER_UDP_SEND",
	"QUIC_PERF_COUNTER_UDP_RECV_BYTES",
	"QUIC_PERF_COUNTER_UDP_SEND_BYTES",
	"QUIC_PERF_COUNTER_UDP_RECV_EVENTS",
	"QUIC_PERF_COUNTER_UDP_SEND_CALLS",
	"QUIC_PERF_COUNTER_APP_SEND_BYTES",
	"QUIC_PERF_COUNTER_APP_RECV_BYTES",
	"QUIC_PERF_COUNTER_CONN_QUEUE_DEPTH",
	"QUIC_PERF_COUNTER_CONN_OPER_QUEUE_DEPTH",
	"QUIC_PERF_COUNTER_CONN_OPER_QUEUED",
	"QUIC_PERF_COUNTER_CONN_OPER_COMPLETED",
	"QUIC_PERF_COUNTER_WORK_OPER_QUEUE_DEPTH",
	"QUIC_PERF_COUNTER_WORK_OPER_QUEUED",
	"QUIC_PERF_COUNTER_WORK_OPER_COMPLETED",
	"QUIC_PERF_COUNTER_PATH_VALIDATED",
	"QUIC_PERF_COUNTER_PATH_FAILURE",
	"QUIC_PERF_COUNTER_SEND_STATELESS_RESET",
	"QUIC_PERF_COUNTER_SEND_STATELESS_RETRY",
	"QUIC_PERF_COUNTER_CONN_LOAD_REJECT",
}

func init() {
	status := C.MsQuicSetup()
	if status != 0 {
		panic(fmt.Sprintf("failed to load quic: %d", status))
	}
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

	keepAliveMs := cfg.KeepAlivePeriod.Milliseconds()
	if keepAliveMs > cfg.MaxIdleTimeout.Milliseconds() {
		keepAliveMs = cfg.MaxIdleTimeout.Milliseconds() / 2
	}
	enableDatagram := C.int(0)
	if cfg.EnableDatagramReceive {
		enableDatagram = C.int(1)

	}
	disableSendBuffering := C.int(0)
	if cfg.DisableSendBuffering {
		disableSendBuffering = C.int(1)
	}
	config := C.LoadListenConfiguration(C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout.Milliseconds()),
		keyFile:                      cKeyFile,

		certFile:                      cCertFile,
		KeepAliveMs:                   C.int(keepAliveMs),
		Alpn:                          buffer,
		MaxBindingStatelessOperations: C.int(cfg.MaxBindingStatelessOperations),
		MaxStatelessOperations:        C.int(cfg.MaxStatelessOperations),
		EnableDatagramReceive:         enableDatagram,
		DisableSendBuffering:          disableSendBuffering,
		MaxBytesPerKey:                C.int(cfg.MaxBytesPerKey),
	})

	if config == nil {

		return MsQuicListener{}, fmt.Errorf("failed to create config")

	}

	listener := C.CreateListener(config)
	if listener == nil {
		return MsQuicListener{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicListener(listener, config, cKeyFile, cCertFile, cAlpn, !cfg.DisableFailOnOpenStream, cfg.DisableSendBuffering, cfg.EnableAppBuffering)
	listeners.Store(listener, res)

	status := C.StartListener(listener, cAddr, C.uint16_t(portInt), buffer)
	if status != 0 {
		return MsQuicListener{}, fmt.Errorf("error creating listener")
	}

	if cfg.TracePerfCounts != nil {
		go func() {
			timeout := cfg.TracePerfCountReport
			if timeout.Milliseconds() == 0 {
				timeout = 30 * time.Second
			}
			for ; ; <-time.After(timeout) {
				counters := cGetPerfCounters()
				cfg.TracePerfCounts(perfCounterNames, counters)
			}
		}()
	}

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

	keepAliveMs := cfg.KeepAlivePeriod.Milliseconds()
	if keepAliveMs > cfg.MaxIdleTimeout.Milliseconds() {
		keepAliveMs = cfg.MaxIdleTimeout.Milliseconds() / 2
	}

	cAlpn := C.CString(cfg.Alpn)
	defer C.free(unsafe.Pointer(cAlpn))

	buffer := C.struct_QUIC_BUFFER{
		Length: C.uint32_t(len(cfg.Alpn)),
		Buffer: (*C.uint8_t)(unsafe.Pointer(cAlpn)),
	}

	enableDatagram := C.int(0)
	if cfg.EnableDatagramReceive {
		enableDatagram = C.int(1)
	}
	disableBuffering := C.int(0)
	if cfg.DisableSendBuffering {
		disableBuffering = C.int(1)
	}
	conn := C.OpenConnection()
	if conn == nil {
		return MsQuicConn{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicConn(conn, !cfg.DisableFailOnOpenStream, cfg.DisableSendBuffering, cfg.EnableAppBuffering)
	_, load := connections.LoadOrStore(conn, res)

	if load {
		println("PANIC already registered connection")
	}

	C.StartConnection(conn, cAddr, C.uint16_t(portInt), C.struct_QUICConfig{
		DisableCertificateValidation:  1,
		MaxBidiStreams:                C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                 C.int(cfg.MaxIdleTimeout.Milliseconds()),
		KeepAliveMs:                   C.int(keepAliveMs),
		MaxBindingStatelessOperations: 0,
		MaxStatelessOperations:        0,
		Alpn:                          buffer,
		EnableDatagramReceive:         enableDatagram,
		DisableSendBuffering:          disableBuffering,
	})

	if !res.waitStart(ctx) {
		return res, fmt.Errorf("failed to start: %v", ctx.Err())
	}
	return res, nil
}

func cCloseListener(listener, config C.HQUIC) {
	C.CloseListener(listener, config)
}

func cAbortStream(s C.HQUIC) {
	C.AbortStream(s)
}

func cStreamWrite(c, s C.HQUIC, cArray *C.uint8_t, size C.int64_t, noAlloc C.uint8_t) C.int64_t {
	return C.StreamWrite(c, s, cArray, size, noAlloc)
}

func cCreateStream(c C.HQUIC, useAppBuffers C.int8_t) C.HQUIC {
	return C.CreateStream(c, useAppBuffers)
}

func cStartStream(s C.HQUIC, fail, useAppBuffers C.int8_t) int64 {
	return int64(C.StartStream(s, fail, useAppBuffers))
}

func cShutdownConnection(c C.HQUIC) {
	C.ShutdownConnection(c)
}

func cAbortConnection(c C.HQUIC) {
	C.AbortConnection(c)
}

func cDatagramSendConnection(c C.HQUIC, msg []byte) C.int32_t {
	buffer := new(C.QUIC_BUFFER)
	*buffer = C.QUIC_BUFFER{
		Buffer: (*C.uint8_t)(unsafe.SliceData(msg)),
		Length: C.uint32_t(len(msg)),
	}
	pinner := runtime.Pinner{}
	pinner.Pin(buffer)
	pinner.Pin(unsafe.SliceData(msg))
	defer pinner.Unpin()
	return C.DatagramSendConnection(c, buffer)
}

func cAttachAppBuffer(s C.HQUIC, buffer *C.QUIC_BUFFER) C.int32_t {
	return C.AttachAppBuffer(s, buffer)
}

func cGetPerfCounters() []uint64 {
	counters := make([]uint64, C.QUIC_PERF_COUNTER_MAX)
	C.GetPerfCounters((*C.uint64_t)(unsafe.SliceData(counters)))
	return counters
}

func QUICAddrToIPPort(addr *C.QUIC_ADDR) (net.IP, int) {
	sa := (*C.struct_sockaddr)(unsafe.Pointer(addr))
	switch sa.sa_family {
	case C.AF_INET:
		ipv4 := (*C.struct_sockaddr_in)(unsafe.Pointer(addr))
		ip := net.IPv4(
			byte(ipv4.sin_addr.s_addr),
			byte(ipv4.sin_addr.s_addr>>8),
			byte(ipv4.sin_addr.s_addr>>16),
			byte(ipv4.sin_addr.s_addr>>24),
		)
		port := int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&ipv4.sin_port))[:]))
		return ip, port

	case C.AF_INET6:
		ipv6 := (*C.struct_sockaddr_in6)(unsafe.Pointer(addr))
		ip := net.IP((*[16]byte)(unsafe.Pointer(&ipv6.sin6_addr))[:])
		port := int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&ipv6.sin6_port))[:]))
		return ip, port

	default:
		return nil, 0
	}
}

// TODO Add windows support
func getRemoteAddr(c C.HQUIC) (net.IP, int) {
	var addr C.QUIC_ADDR
	var addrLen C.uint32_t = C.uint32_t(unsafe.Sizeof(addr))

	if C.GetRemoteAddr(c, &addr, &addrLen) != 0 {
		return nil, 0
	}

	return QUICAddrToIPPort(&addr)
}
