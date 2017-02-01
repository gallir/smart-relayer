package cluster

import (
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"container/heap"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
)

const (
	requestBufferSize = 64
)

type connHandler struct {
	sync.Mutex
	seq        uint64
	last       uint64
	pQueue     seqRespHeap // Priority queue for the responses
	srv        *Server
	conn       net.Conn
	reqCh      chan *reqData
	respCh     chan *redis.Resp
	senders    int32
	maxSenders int
}

func Handle(srv *Server, netCon net.Conn) {
	h := &connHandler{
		srv:  srv,
		conn: netCon,
	}
	defer h.close()

	h.init()

	reader := redis.NewRespReader(h.conn)
	for {
		err := netCon.SetReadDeadline(time.Now().Add(listenTimeout * time.Second))
		if err != nil {
			log.Printf("error setting read deadline: %s", err)
			return
		}

		req := reader.Read()
		if redis.IsTimeout(req) {
			continue
		} else if req.IsType(redis.IOErr) {
			return
		}

		resp := h.process(req)
		if h.srv.config.Compress || h.srv.config.Uncompress {
			resp = compress.UResp(resp)
		}
		resp.WriteTo(h.conn)
	}
}

func (h *connHandler) init() {
	h.reqCh = make(chan *reqData, requestBufferSize)
	h.respCh = make(chan *redis.Resp, 1)
	if h.srv.config.Parallel {
		h.maxSenders = h.srv.config.MaxIdleConnections/2 + 1
		h.pQueue = make(seqRespHeap, 0, h.maxSenders)
		heap.Init(&h.pQueue)
	} else {
		h.maxSenders = 1
	}
	h.newSender()
}

func (h *connHandler) newSender() {
	if h.senders < int32(h.maxSenders) {
		go h.sender()
	}
}

func (h *connHandler) close() {
	if h.reqCh != nil {
		close(h.reqCh)
	}
	if h.respCh != nil {
		close(h.respCh)
	}
	h.conn.Close()

}

func (h *connHandler) process(m *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		atomic.AddUint64(&h.srv.statErrors, 1)
		return respBadCommand
	}

	cmd, err := ms[0].Str()
	if err != nil || strings.ToUpper(cmd) == selectCommand {
		atomic.AddUint64(&h.srv.statErrors, 1)
		return respBadCommand
	}

	h.seq++ //atomic.AddUint64(&h.seq, 1),
	data := &reqData{
		seq:      h.seq,
		cmd:      cmd,
		args:     ms[1:],
		compress: h.srv.config.Compress,
	}

	doAsync := false
	var fastResponse *redis.Resp
	if h.srv.mode == lib.ModeSmart {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		h.reqCh <- data
		atomic.AddUint64(&h.srv.statAsync, 1)
		return fastResponse
	}

	data.answerCh = h.respCh
	h.reqCh <- data
	atomic.AddUint64(&h.srv.statSync, 1)
	return <-h.respCh
}

func (h *connHandler) sender() {
	atomic.AddInt32(&h.senders, 1)
	atomic.AddInt32(&h.srv.statClients, 1)
	defer atomic.AddInt32(&h.senders, -1)
	defer atomic.AddInt32(&h.srv.statClients, -1)

	for m := range h.reqCh {
		// Add senders if there are pending requests
		if h.srv.config.Parallel && len(h.reqCh) > 0 {
			h.newSender()
		}
		args := make([]interface{}, len(m.args))
		for i, arg := range m.args {
			args[i] = arg
			if m.compress {
				b, e := arg.Bytes()
				if e == nil && len(b) > compress.MinCompressSize {
					args[i] = compress.Bytes(b)
				}
			}
		}

		if h.srv.config.Parallel {
			h.parallel(m, args)
			continue
		}

		m.resp = h.srv.pool.Cmd(m.cmd, args...)
		if m.answerCh != nil {
			m.answerCh <- m.resp
		}
	}
}

// This function ensures the responses are returned ordered
// using a priority queue when several goroutines are used for
// sending requests in parallel to the redis server. It DOES NOT
// ensure that the request were executed in order, a GET still
// can return (nil) before a previous SET is processed.
// But anyway, the computation cost is no high and may serve
// to improve behaviour in heavy clients that send a lot of data
// to the server.
// WARN: you've wanrned, think twice and use the "parallel" option
// only if you are sure.
func (h *connHandler) parallel(m *reqData, args []interface{}) {
	h.Lock()
	heap.Push(&h.pQueue, m)
	h.Unlock()

	m.resp = h.srv.pool.Cmd(m.cmd, args...)

	h.Lock()
	for h.pQueue.Len() > 0 {
		item := heap.Pop(&h.pQueue).(*reqData)
		if item.resp == nil {
			heap.Push(&h.pQueue, item)
			break
		}
		if item.answerCh != nil {
			item.answerCh <- item.resp
		}
	}
	h.Unlock()
}
