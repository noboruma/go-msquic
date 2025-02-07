package quic

/*
#cgo CFLAGS: -I./inc
#cgo LDFLAGS: -L../../../msquic/small_build/bin/Release/ -lmsquic
#include "msquic.c"
*/
import "C"
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

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
}

type ReadBuffer struct {
	buffer bytes.Buffer
	m      sync.Mutex
	notify *sync.Cond
}

var streams sync.Map     //map[C.HQUIC]MsQuicStream
var listeners sync.Map   //map[C.HQUIC]MsQuicListener
var connections sync.Map //map[C.HQUIC]MsQuicConn

//export newConnectionCallback
func newConnectionCallback(l C.HQUIC, c C.HQUIC) {
	listener, has := listeners.Load(l)
	if !has {
		panic("listener not registered")
	}
	res := newMsQuicConn(c)
	connections.Store(c, res)
	listener.(MsQuicListener).acceptQueue <- res
}

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
	// TODO: errors
	if err != nil {
		panic(err.Error())
	}
	stream.buffer.notify.Signal()
}

//export newStreamCallback
func newStreamCallback(c C.HQUIC, s C.HQUIC) {
	rawConn, has := connections.Load(c)
	if !has {
		panic("connection not registered")
	}
	conn := rawConn.(MsQuicConn)
	res := newMsQuicStream(s)
	streams.Store(s, res)
	conn.acceptStreamQueue <- res
}

func init() {
	status := C.MsQuicSetup()
	if status != 0 {
		panic(fmt.Sprintf("failed to load quic: %d", status))
	}
}

type MsQuicConn struct {
	conn              C.HQUIC
	config            C.HQUIC
	acceptStreamQueue chan MsQuicStream
	ctx               context.Context
	cancel            context.CancelFunc
}

func newMsQuicConn(c C.HQUIC) MsQuicConn {
	ctx, cancel := context.WithCancel(context.Background())
	return MsQuicConn{
		conn:              c,
		acceptStreamQueue: make(chan MsQuicStream),
		ctx:               ctx,
		cancel:            cancel,
	}
}

func (mqc MsQuicConn) Close() error {
	mqc.cancel()
	close(mqc.acceptStreamQueue)
	C.CloseConfiguration(mqc.config)
	return nil
}

func (mqc MsQuicConn) OpenStream() (MsQuicStream, error) {
	stream := C.OpenStream(mqc.conn)
	if stream == nil {
		return MsQuicStream{}, fmt.Errorf("stream open error")
	}
	res := newMsQuicStream(stream)
	streams.Store(stream, res)
	return res, nil
}

func (mqc MsQuicConn) AcceptStream(ctx context.Context) (MsQuicStream, error) {
	select {
	case <-ctx.Done():
	case s, open := <-mqc.acceptStreamQueue:
		if !open {
			return MsQuicStream{}, fmt.Errorf("closed connection")
		}
		return s, nil
	}
	return MsQuicStream{}, fmt.Errorf("closed context")
}

func (mqc MsQuicConn) Context() context.Context {
	return mqc.ctx
}

func (mqc MsQuicConn) RemoteAddr() net.Addr {
	var addr C.struct_sockaddr_storage
	var addrLen C.uint32_t = C.uint32_t(unsafe.Sizeof(addr))

	// Call the C function to get the address
	if C.GetRemoteAddr(mqc.conn, &addr, &addrLen) != 0 {
		return nil
	}

	// Convert C sockaddr to Go net.Addr
	switch addr.ss_family {
	case C.AF_INET:
		// IPv4
		ip := net.IPv4(
			byte(addr.__ss_padding[2]),
			byte(addr.__ss_padding[3]),
			byte(addr.__ss_padding[4]),
			byte(addr.__ss_padding[5]),
		)
		port := (int(addr.__ss_padding[0]) << 8) | int(addr.__ss_padding[1])
		return &net.UDPAddr{IP: ip, Port: port}

	default:
		return nil
	}
}

type MsQuicStream struct {
	stream C.HQUIC
	buffer *ReadBuffer
	ctx    context.Context
	cancel context.CancelFunc
}

func newMsQuicStream(s C.HQUIC) MsQuicStream {
	ctx, cancel := context.WithCancel(context.Background())
	res := MsQuicStream{
		stream: s,
		buffer: &ReadBuffer{
			buffer: bytes.Buffer{},
		},
		ctx:    ctx,
		cancel: cancel,
	}
	res.buffer.notify = sync.NewCond(&res.buffer.m)
	return res
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	mqs.buffer.m.Lock()
	defer mqs.buffer.m.Unlock()
	for mqs.buffer.buffer.Len() == 0 && mqs.ctx.Err() == nil {
		mqs.buffer.notify.Wait()
	}
	if mqs.ctx.Err() != nil {
		return 0, io.EOF
	}
	return mqs.buffer.buffer.Read(data)
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	cArray := (*C.uint8_t)(unsafe.Pointer(&data[0]))
	n := C.int64_t(len(data))
	status := C.StreamWrite(mqs.stream, cArray, &n)
	if status != 0 {
		return int(n), fmt.Errorf("read stream error: %d", status)
	}
	runtime.KeepAlive(data)
	return int(n), nil
}

func (mqs MsQuicStream) SetDeadline(ttl time.Time) error {
	err := mqs.SetReadDeadline(ttl)
	err2 := mqs.SetWriteDeadline(ttl)
	return errors.Join(err, err2)
}

func (mqs MsQuicStream) SetReadDeadline(ttl time.Time) error {
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx
}

func (mqs MsQuicStream) Close() error {
	mqs.cancel()
	mqs.buffer.notify.Broadcast()
	return nil
}

type MsQuicListener struct {
	listener    C.HQUIC
	acceptQueue chan MsQuicConn
	// deallocate
	key, cert *C.char
}

func newMsQuicListener(c C.HQUIC, key, cert *C.char) MsQuicListener {
	return MsQuicListener{
		listener:    c,
		acceptQueue: make(chan MsQuicConn),
		key:         key,
		cert:        cert,
	}
}

func (mql MsQuicListener) Close() error {
	close(mql.acceptQueue)
	C.CloseListener(mql.listener)
	C.free(unsafe.Pointer(mql.key))
	C.free(unsafe.Pointer(mql.cert))
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

	listener := C.Listen(cAddr, C.uint16_t(portInt), C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout),
		keyFile:                      cKeyFile,
		certFile:                     cCertFile,
	})
	if listener == nil {
		return MsQuicListener{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicListener(listener, cKeyFile, cCertFile)
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

	conn := C.DialConnection(cAddr, C.uint16_t(portInt), C.struct_QUICConfig{
		DisableCertificateValidation: 1,
		MaxBidiStreams:               C.int(cfg.MaxIncomingStreams),
		IdleTimeoutMs:                C.int(cfg.MaxIdleTimeout),
		KeepAliveMs:                  C.int(cfg.KeepAlivePeriod) / 4,
	})
	if conn == nil {
		return MsQuicConn{}, fmt.Errorf("error creating listener")
	}
	res := newMsQuicConn(conn)
	connections.Store(conn, res)
	return res, nil
}
