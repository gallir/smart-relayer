package cluster

import (
	"net"
	"strings"
	"sync/atomic"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

const (
	requestBufferSize = 64
)

type connHandler struct {
	initialized bool
	seq         uint64
	last        uint64
	srv         *Server
	conn        net.Conn
	reqCh       chan reqData
	respCh      chan *redis.Resp
	pending     int32
}

func Handle(srv *Server, netCon net.Conn) {
	h := &connHandler{
		srv:  srv,
		conn: netCon,
	}
	defer h.close()

	reader := redis.NewRespReader(h.conn)
	for {
		req := reader.Read()
		if req.IsType(redis.IOErr) {
			if redis.IsTimeout(req) {
				continue
			}
			return
		}

		resp := h.process(req)
		if h.srv.config.Compress || h.srv.config.Uncompress {
			resp.Uncompress(lib.MagicSnappy)
		}
		resp.WriteTo(h.conn)
		resp.ReleaseBuffers()
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

func (h *connHandler) process(req *redis.Resp) *redis.Resp {
	cmd, err := req.First()
	if err != nil || strings.ToUpper(cmd) == selectCommand {
		return respBadCommand
	}

	doAsync := false
	var fastResponse *redis.Resp
	if h.srv.mode == lib.ModeSmart {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		atomic.AddInt32(&h.pending, 1)
		if !h.initialized {
			h.initialized = true
			h.reqCh = make(chan reqData, requestBufferSize)
			h.respCh = make(chan *redis.Resp, 1)
			go h.sendWorker()
		}
		h.reqCh <- reqData{
			req:      req,
			compress: h.srv.config.Compress,
		}
		return fastResponse
	}

	p := atomic.LoadInt32(&h.pending)
	if p != 0 {
		// There are operations in queue, send by the same channel
		h.reqCh <- reqData{
			req:      req,
			compress: h.srv.config.Compress,
			answerCh: h.respCh,
		}
		return <-h.respCh
	}

	// No ongoing operations, we can send directly
	resp := h.sender(req, h.srv.config.Compress)
	return resp

}

func (h *connHandler) sendWorker() {
	for m := range h.reqCh {
		resp := h.sender(m.req, m.compress)
		if m.answerCh != nil {
			atomic.AddInt32(&h.pending, -1)
			m.answerCh <- resp
		}
	}
}

func (h *connHandler) sender(req *redis.Resp, compress bool) *redis.Resp {
	if compress {
		req.Compress(lib.MinCompressSize, lib.MagicSnappy)
	}
	a, err := req.Array()
	if err != nil {
		return respBadCommand
	}
	cmd, _ := a[0].Str()
	args := make([]interface{}, 0, len(a)-1)
	for _, v := range a {
		b, _ := v.Bytes()
		args = append(args, b)
	}

	resp := h.srv.pool.Cmd(cmd, args[1:])
	req.ReleaseBuffers()
	return resp
}
