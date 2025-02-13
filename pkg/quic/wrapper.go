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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
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

type Connection interface {
	OpenStream() (MsQuicStream, error)
	AcceptStream(ctx context.Context) (MsQuicStream, error)
	Close() error
	RemoteAddr() net.Addr
	Context() context.Context
}

type Stream interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	Close() error
	SetDeadline(ttl time.Time) error
	SetReadDeadline(ttl time.Time) error
	SetWriteDeadline(ttl time.Time) error
	Context() context.Context
}

type Config struct {
	MaxIncomingStreams int64
	MaxIdleTimeout     int64
	KeepAlivePeriod    int64
	CertFile           string
	KeyFile            string
	Alpn               string
}

type ReadBuffer struct {
	buffer        bytes.Buffer
	m             sync.Mutex
	signal        chan struct{}
	readDeadline  time.Time
	writeDeadline time.Time
}

var streams sync.Map     //map[C.HQUIC]MsQuicStream
var listeners sync.Map   //map[C.HQUIC]MsQuicListener
var connections sync.Map //map[C.HQUIC]MsQuicConn

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		panic("Missing listener")
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
		panic("Missing connection")
	}
	totalClosedConnections.Add(1)
	res.(MsQuicConn).remoteClose()
}

// #cgo noescape newReadCallback

//export newReadCallback
func newReadCallback(s C.HQUIC, buffer *C.uint8_t, length C.int64_t) {
	rawStream, has := streams.Load(s)
	if !has {
		panic("stream not registered")
	}
	stream := rawStream.(MsQuicStream)
	stream.buffer.m.Lock()
	defer stream.buffer.m.Unlock()

	goBuffer := unsafe.Slice((*byte)(unsafe.Pointer(buffer)), length)
	_, err := stream.buffer.buffer.Write(goBuffer)
	if err != nil {
		panic(err.Error())
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
		panic("connection not registered")
	}
	if _, has := streams.Load(s); has {
		// stream opened locally, no need to accept
		println("Yes it happens")
		return
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
		panic("stream not registered")
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

type MsQuicConn struct {
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
	remoteAddr        net.UDPAddr
	shutdown          *atomic.Bool
}

func newMsQuicConn(c C.HQUIC) MsQuicConn {
	ctx, cancel := context.WithCancel(context.Background())

	var addr C.struct_sockaddr_storage
	var addrLen C.uint32_t = C.uint32_t(unsafe.Sizeof(addr))

	if C.GetRemoteAddr(c, &addr, &addrLen) != 0 {
		panic("Remote addr issue")
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
	return MsQuicConn{
		conn:              c,
		acceptStreamQueue: make(chan MsQuicStream, 1024),
		ctx:               ctx,
		cancel:            cancel,
		remoteAddr:        net.UDPAddr{IP: ip, Port: port},
		shutdown:          new(atomic.Bool),
	}
}

func (mqc MsQuicConn) Close() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
		C.ShutdownConnection(mqc.conn)
	}
	return nil
}

func (mqc MsQuicConn) remoteClose() error {
	if !mqc.shutdown.Swap(true) {
		mqc.cancel()
	}
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	if mqc.shutdown.Load() {
		return MsQuicStream{}, fmt.Errorf("closed connection")
	}
	stream := C.OpenStream(mqc.conn)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	totalOpenedStreams.Add(1)
	res := newMsQuicStream(stream, mqc.ctx)
	streams.Store(stream, res)
	return res, nil
}

func (mqc MsQuicConn) AcceptStream(ctx context.Context) (MsQuicStream, error) {
	select {
	case <-ctx.Done():
	case <-mqc.ctx.Done():
	case s, open := <-mqc.acceptStreamQueue:
		if !open {
			return MsQuicStream{}, fmt.Errorf("closed connection")
		}
		return s, nil
	}
	return MsQuicStream{}, fmt.Errorf("closed connection")
}

func (mqc MsQuicConn) Context() context.Context {
	return mqc.ctx
}

func (mqc MsQuicConn) RemoteAddr() net.Addr {
	return &mqc.remoteAddr
}

type MsQuicStream struct {
	stream   C.HQUIC
	buffer   *ReadBuffer
	ctx      context.Context
	cancel   context.CancelFunc
	shutdown *atomic.Bool
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		stream: s,
		buffer: &ReadBuffer{
			buffer: bytes.Buffer{},
			signal: make(chan struct{}, 1),
		},
		ctx:      ctx,
		cancel:   cancel,
		shutdown: new(atomic.Bool),
	}

	return res
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	if mqs.shutdown.Load() {
		return 0, io.EOF
	}

	mqs.buffer.m.Lock()
	for mqs.buffer.buffer.Len() == 0 {
		ctx := mqs.ctx
		if !mqs.buffer.readDeadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, mqs.buffer.readDeadline)
			defer cancel()
		}

		mqs.buffer.m.Unlock()

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.EOF
		case <-mqs.buffer.signal:
			mqs.buffer.m.Lock()
		}
	}

	defer mqs.buffer.m.Unlock()

	return mqs.buffer.buffer.Read(data)
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	if mqs.shutdown.Load() {
		return 0, io.EOF
	}
	offset := 0
	size := len(data)
	for offset != len(data) {
		cArray := (*C.uint8_t)(unsafe.Pointer(&data[offset]))
		n := C.StreamWrite(mqs.stream, cArray, C.int64_t(size))
		offset += int(n)
		size -= int(n)
		if n == -1 {
			return int(n), fmt.Errorf("write stream error")
		}
	}
	runtime.KeepAlive(data)
	return int(offset), nil
}

func (mqs MsQuicStream) SetDeadline(ttl time.Time) error {
	err := mqs.SetReadDeadline(ttl)
	err2 := mqs.SetWriteDeadline(ttl)
	return errors.Join(err, err2)
}

func (mqs MsQuicStream) SetReadDeadline(ttl time.Time) error {
	mqs.buffer.m.Lock()
	defer mqs.buffer.m.Unlock()
	mqs.buffer.readDeadline = ttl
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx
}

func (mqs MsQuicStream) Close() error {
	if !mqs.shutdown.Swap(true) {
		mqs.cancel()
		C.ShutdownStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) remoteClose() error {
	if !mqs.shutdown.Swap(true) {
		mqs.cancel()
	}
	return nil
}

type MsQuicListener struct {
	listener, config C.HQUIC
	acceptQueue      chan MsQuicConn
	// deallocate
	key, cert, alpn *C.char
	shutdown        *atomic.Bool
}

func newMsQuicListener(c C.HQUIC, config C.HQUIC, key, cert, alpn *C.char) MsQuicListener {
	return MsQuicListener{
		listener:    c,
		acceptQueue: make(chan MsQuicConn, 1024),
		key:         key,
		cert:        cert,
		alpn:        alpn,
		config:      config,
		shutdown:    new(atomic.Bool),
	}
}

func (mql MsQuicListener) Close() error {
	if !mql.shutdown.Swap(true) {
		C.CloseListener(mql.listener, mql.config)
		C.free(unsafe.Pointer(mql.key))
		C.free(unsafe.Pointer(mql.cert))
		C.free(unsafe.Pointer(mql.alpn))
	}
	return nil
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

func (mql MsQuicListener) Accept(ctx context.Context) (MsQuicConn, error) {
	select {
	case c, open := <-mql.acceptQueue:
		if !open {
			return MsQuicConn{}, fmt.Errorf("closed listener")
		}
		return c, nil
	case <-ctx.Done():
		return MsQuicConn{}, fmt.Errorf("closed context")
	}
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

	conn := C.DialConnection(cAddr, C.uint16_t(portInt), C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout),
		KeepAliveMs:                  C.int(keepAliveMs),
	})
	if conn == nil {
		return MsQuicConn{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicConn(conn)
	connections.Store(conn, res)
	return res, nil
}
