package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "msquic.h"
import "C"

type Stream interface {
	Read(data []byte) (int, error)
	Write(data []byte) (int, error)
	Close() error
	SetDeadline(ttl time.Time) error
	SetReadDeadline(ttl time.Time) error
	SetWriteDeadline(ttl time.Time) error
	Context() context.Context
}

type streamState struct {
	shutdown         atomic.Bool
	readBufferAccess sync.Mutex
	readBuffer       bytes.Buffer
	readDeadline     time.Time
	writeDeadline    time.Time
	writeAccess      sync.Mutex
	startSignal      chan struct{}
}

func (ss *streamState) hasReadData() bool {
	ss.readBufferAccess.Lock()
	defer ss.readBufferAccess.Unlock()
	return ss.readBuffer.Len() != 0
}

type MsQuicStream struct {
	stream     C.HQUIC
	ctx        context.Context
	cancel     context.CancelFunc
	state      *streamState
	readSignal chan struct{}
	peerSignal chan struct{}
}

func newMsQuicStream(s C.HQUIC, connCtx context.Context) MsQuicStream {
	ctx, cancel := context.WithCancel(connCtx)
	res := MsQuicStream{
		stream: s,
		ctx:    ctx,
		cancel: cancel,
		state: &streamState{
			readBuffer:    bytes.Buffer{},
			readDeadline:  time.Time{},
			writeDeadline: time.Time{},
			shutdown:      atomic.Bool{},
			startSignal:   make(chan struct{}, 1),
		},
		readSignal: make(chan struct{}, 1),
		peerSignal: make(chan struct{}, 1),
	}
	return res
}

func (mqs MsQuicStream) waitStart() bool {
	select {
	case <-mqs.state.startSignal:
		return true
	case <-mqs.Context().Done():
	}
	return false
}

func (mqs MsQuicStream) Read(data []byte) (int, error) {
	state := mqs.state
	if mqs.ctx.Err() != nil {
		return 0, io.EOF
	}

	deadline := state.readDeadline
	if !state.hasReadData() {
		ctx := mqs.ctx
		if !deadline.IsZero() {
			if time.Now().After(deadline) {
				return 0, os.ErrDeadlineExceeded
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		if !mqs.waitRead(ctx) {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, os.ErrDeadlineExceeded
			}
			return 0, io.EOF
		}
	}

	state.readBufferAccess.Lock()
	defer state.readBufferAccess.Unlock()
	n, err := state.readBuffer.Read(data)
	if n == 0 { // ignore io.EOF
		return 0, nil
	}

	return n, err
}

func (mqs MsQuicStream) waitRead(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-mqs.readSignal:
		return true
	}
}

func (mqs MsQuicStream) Write(data []byte) (int, error) {
	state := mqs.state
	state.writeAccess.Lock()
	defer state.writeAccess.Unlock()
	ctx := mqs.ctx
	if ctx.Err() != nil {
		return 0, io.EOF
	}
	deadline := state.writeDeadline
	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	offset := 0
	size := len(data)
	for offset != len(data) && ctx.Err() == nil {
		n := cStreamWrite(mqs.stream, (*C.uint8_t)(unsafe.SliceData(data[offset:])), C.int64_t(size))
		if n == -1 {
			return int(offset), fmt.Errorf("write stream error %v/%v", offset, size)
		}
		offset += int(n)
		size -= int(n)
	}
	runtime.KeepAlive(data)
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return int(offset), os.ErrDeadlineExceeded
		}
		return int(offset), io.ErrUnexpectedEOF
	}
	return len(data), nil
}

func (mqs MsQuicStream) SetDeadline(ttl time.Time) error {
	err := mqs.SetReadDeadline(ttl)
	err2 := mqs.SetWriteDeadline(ttl)
	return errors.Join(err, err2)
}

func (mqs MsQuicStream) SetReadDeadline(ttl time.Time) error {
	mqs.state.readDeadline = ttl
	return nil
}

func (mqs MsQuicStream) SetWriteDeadline(ttl time.Time) error {
	mqs.state.writeDeadline = ttl
	return nil
}

func (mqs MsQuicStream) Context() context.Context {
	return mqs.ctx

}

// Close is a definitive operation
// The stream cannot be receive anything after that call
func (mqs MsQuicStream) Close() error {
	return mqs.shutdownClose()
}

func (mqs MsQuicStream) appClose() error {
	mqs.peerClose()
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	mqs.state.shutdown.Store(true)
	//stat := cGetStreamStats(mqs.stream)
	//oldSS := stats.Load()
	//oldSS.m.Lock()
	//defer oldSS.m.Unlock()
	//oldSS.arr = append(oldSS.arr, stat)
	return nil
}

func (mqs MsQuicStream) shutdownClose() error {
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	if !mqs.state.shutdown.Swap(true) {
		cShutdownStream(mqs.stream)
		select {
		case <-mqs.peerSignal:
		case <-time.After(3 * time.Second):
			cAbortStream(mqs.stream)
		}
	}
	return nil
}

func (mqs MsQuicStream) abortClose() error {
	mqs.peerClose()
	mqs.cancel()
	mqs.state.writeAccess.Lock()
	defer mqs.state.writeAccess.Unlock()
	if !mqs.state.shutdown.Swap(true) {
		cAbortStream(mqs.stream)
	}
	return nil
}

func (mqs MsQuicStream) peerClose() {
	select {
	case mqs.peerSignal <- struct{}{}:
	default:
	}
}

func (mqs MsQuicStream) WriteTo(w io.Writer) (int64, error) {
	state := mqs.state
	n := int64(0)
	var err error
	for mqs.ctx.Err() == nil {
		deadline := state.readDeadline
		if !state.hasReadData() {
			ctx := mqs.ctx
			if !deadline.IsZero() {
				if time.Now().After(deadline) {
					return n, os.ErrDeadlineExceeded
				}
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, deadline)
				defer cancel()
			}
			if !mqs.waitRead(ctx) {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return n, os.ErrDeadlineExceeded
				}
				return n, io.EOF
			}
		}

		nn := int64(0)
		state.readBufferAccess.Lock()
		nn, err = state.readBuffer.WriteTo(w)
		state.readBufferAccess.Unlock()
		n += nn
		if err != nil {
			break
		}
	}

	if mqs.ctx.Err() != nil {
		return n, io.EOF
	}

	return n, err
}

func (mqs MsQuicStream) ReadFrom(r io.Reader) (n int64, err error) {
	var buffer [32 * 1024]byte
	for mqs.ctx.Err() == nil {
		bn, err := r.Read(buffer[:])
		if bn != 0 {
			var nn int
			nn, err = mqs.Write(buffer[:bn])
			n += int64(nn)
		}
		if err != nil {
			return n, err
		}
	}
	return n, io.EOF
}

//var stats atomic.Pointer[statsStruct]
//func init() {
//
//	go func() {
//		for {
//			<-time.After(10 * time.Second)
//			listeners.Range(func(key, value any) bool {
//				stats := cGetListenerStats(key.(C.HQUIC))
//				println("TotalAcceptedConnections", stats.TotalAcceptedConnections)
//				println("TotalRejectedConnections", stats.TotalRejectedConnections)
//				println("BindingRecvDroppedPackets", stats.BindingRecvDroppedPackets)
//				return true
//			})
//		}
//	}()
//
//	ss := statsStruct{
//		arr: []C.QUIC_STREAM_STATISTICS{},
//		m:   sync.Mutex{},
//	}
//	stats.Store(&ss)
//	go func() {
//		for {
//			<-time.After(10 * time.Second)
//			ssn := statsStruct{
//				arr: []C.QUIC_STREAM_STATISTICS{},
//				m:   sync.Mutex{},
//			}
//			oldSS := stats.Swap(&ssn)
//			var ConnBlockedByAmplificationProtUs uint64 = 0
//			var ConnBlockedByCongestionControlUs uint64 = 0
//			var ConnBlockedByFlowControlUs uint64 = 0
//			var ConnBlockedByPacingUs uint64 = 0
//			var ConnBlockedBySchedulingUs uint64 = 0
//			var StreamBlockedByAppUs uint64 = 0
//			var StreamBlockedByFlowControlUs uint64 = 0
//			var StreamBlockedByIdFlowControlUs uint64 = 0
//			oldSS.m.Lock()
//			old := oldSS.arr
//			mmax := uint64(len(old))
//			if mmax == 0 {
//				continue
//			}
//			for i := range old {
//				ConnBlockedByAmplificationProtUs += uint64(old[i].ConnBlockedByAmplificationProtUs)
//				ConnBlockedByCongestionControlUs += uint64(old[i].ConnBlockedByCongestionControlUs)
//				ConnBlockedByFlowControlUs += uint64(old[i].ConnBlockedByFlowControlUs)
//				ConnBlockedByPacingUs += uint64(old[i].ConnBlockedByPacingUs)
//				ConnBlockedBySchedulingUs += uint64(old[i].ConnBlockedBySchedulingUs)
//				StreamBlockedByAppUs += uint64(old[i].StreamBlockedByAppUs)
//				StreamBlockedByFlowControlUs += uint64(old[i].StreamBlockedByFlowControlUs)
//				StreamBlockedByIdFlowControlUs += uint64(old[i].StreamBlockedByIdFlowControlUs)
//
//				total := uint64(old[i].ConnBlockedByAmplificationProtUs) +
//					uint64(old[i].ConnBlockedByCongestionControlUs) +
//					uint64(old[i].ConnBlockedByFlowControlUs) +
//					uint64(old[i].ConnBlockedByPacingUs) +
//					uint64(old[i].ConnBlockedBySchedulingUs) +
//					uint64(old[i].StreamBlockedByAppUs) +
//					uint64(old[i].StreamBlockedByFlowControlUs) +
//					uint64(old[i].StreamBlockedByIdFlowControlUs)
//				if total == 0 {
//					mmax -= 1
//				}
//			}
//			sort.Sort(statsArr(old))
//			println("entries:", mmax)
//			println("AVG")
//			println("ConnBlockedByAmplificationProtUs:", ConnBlockedByAmplificationProtUs/mmax)
//			println("ConnBlockedByCongestionControlUs:", ConnBlockedByCongestionControlUs/mmax)
//			println("ConnBlockedByFlowControlUs:", ConnBlockedByFlowControlUs/mmax)
//			println("ConnBlockedByPacingUs:", ConnBlockedByPacingUs/mmax)
//			println("ConnBlockedBySchedulingUs:", ConnBlockedBySchedulingUs/mmax)
//			println("StreamBlockedByAppUs:", StreamBlockedByAppUs/mmax)
//			println("StreamBlockedByFlowControlUs:", StreamBlockedByFlowControlUs/mmax)
//			println("StreamBlockedByIdFlowControlUs:", StreamBlockedByIdFlowControlUs/mmax)
//			println("p1")
//			i := 0
//			println("ConnBlockedByAmplificationProtUs:", old[i].ConnBlockedByAmplificationProtUs)
//			println("ConnBlockedByCongestionControlUs:", old[i].ConnBlockedByCongestionControlUs)
//			println("ConnBlockedByFlowControlUs:", old[i].ConnBlockedByFlowControlUs)
//			println("ConnBlockedByPacingUs:", old[i].ConnBlockedByPacingUs)
//			println("ConnBlockedBySchedulingUs:", old[i].ConnBlockedBySchedulingUs)
//			println("StreamBlockedByAppUs:", old[i].StreamBlockedByAppUs)
//			println("StreamBlockedByFlowControlUs:", old[i].StreamBlockedByFlowControlUs)
//			println("StreamBlockedByIdFlowControlUs:", old[i].StreamBlockedByIdFlowControlUs)
//			println("p50")
//			i = len(old) / 2
//			println("ConnBlockedByAmplificationProtUs:", old[i].ConnBlockedByAmplificationProtUs)
//			println("ConnBlockedByCongestionControlUs:", old[i].ConnBlockedByCongestionControlUs)
//			println("ConnBlockedByFlowControlUs:", old[i].ConnBlockedByFlowControlUs)
//			println("ConnBlockedByPacingUs:", old[i].ConnBlockedByPacingUs)
//			println("ConnBlockedBySchedulingUs:", old[i].ConnBlockedBySchedulingUs)
//			println("StreamBlockedByAppUs:", old[i].StreamBlockedByAppUs)
//			println("StreamBlockedByFlowControlUs:", old[i].StreamBlockedByFlowControlUs)
//			println("StreamBlockedByIdFlowControlUs:", old[i].StreamBlockedByIdFlowControlUs)
//			println("p99")
//			i = len(old) - 1
//			println("ConnBlockedByAmplificationProtUs:", old[i].ConnBlockedByAmplificationProtUs)
//			println("ConnBlockedByCongestionControlUs:", old[i].ConnBlockedByCongestionControlUs)
//			println("ConnBlockedByFlowControlUs:", old[i].ConnBlockedByFlowControlUs)
//			println("ConnBlockedByPacingUs:", old[i].ConnBlockedByPacingUs)
//			println("ConnBlockedBySchedulingUs:", old[i].ConnBlockedBySchedulingUs)
//			println("StreamBlockedByAppUs:", old[i].StreamBlockedByAppUs)
//			println("StreamBlockedByFlowControlUs:", old[i].StreamBlockedByFlowControlUs)
//			println("StreamBlockedByIdFlowControlUs:", old[i].StreamBlockedByIdFlowControlUs)
//			oldSS.m.Unlock()
//		}
//
//	}()
//
//}

type statsStruct struct {
	arr []C.QUIC_STREAM_STATISTICS
	m   sync.Mutex
}

type statsArr []C.QUIC_STREAM_STATISTICS

func (m statsArr) Len() int { return len(m) }
func (m statsArr) Less(i, j int) bool {
	iTotal := uint64(m[i].ConnBlockedByAmplificationProtUs) +
		uint64(m[i].ConnBlockedByCongestionControlUs) +
		uint64(m[i].ConnBlockedByFlowControlUs) +
		uint64(m[i].ConnBlockedByPacingUs) +
		uint64(m[i].ConnBlockedBySchedulingUs) +
		uint64(m[i].StreamBlockedByAppUs) +
		uint64(m[i].StreamBlockedByFlowControlUs) +
		uint64(m[i].StreamBlockedByIdFlowControlUs)
	jTotal := uint64(m[j].ConnBlockedByAmplificationProtUs) +
		uint64(m[j].ConnBlockedByCongestionControlUs) +
		uint64(m[j].ConnBlockedByFlowControlUs) +
		uint64(m[j].ConnBlockedByPacingUs) +
		uint64(m[j].ConnBlockedBySchedulingUs) +
		uint64(m[j].StreamBlockedByAppUs) +
		uint64(m[j].StreamBlockedByFlowControlUs) +
		uint64(m[j].StreamBlockedByIdFlowControlUs)
	return iTotal < jTotal
}
func (m statsArr) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
