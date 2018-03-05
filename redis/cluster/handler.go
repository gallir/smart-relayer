package cluster

import (
	"net"
	"strings"
	"sync/atomic"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

const (
	requestBufferSize = 1024
)

type connHandler struct {
	initialized bool
	srv         *Server
	conn        net.Conn
	reqCh       chan reqData
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

		h.process(req)
	}
}

func (h *connHandler) close() {
	if h.reqCh != nil {
		close(h.reqCh)
	}
	h.conn.Close()
}

func (h *connHandler) process(req *redis.Resp) {
	cmd, err := req.First()
	if err != nil || strings.ToUpper(cmd) == selectCommand {
		respBadCommand.WriteTo(h.conn)
	}

	doAsync := false
	var fastResponse *redis.Resp
	if h.srv.mode == lib.ModeSmart {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		fastResponse.WriteTo(h.conn)
		if !h.initialized {
			h.initialized = true
			h.reqCh = make(chan reqData, requestBufferSize)
			go h.sendWorker()
		}
		atomic.AddInt32(&h.pending, 1)
		h.reqCh <- reqData{
			req:        req,
			compress:   h.srv.config.Compress,
			mustAnswer: false,
		}
		return
	}

	comp := h.srv.config.Compress
	if cmd == evalCommand {
		comp = false
	}

	p := atomic.LoadInt32(&h.pending)
	if p != 0 {
		// There are operations in queue, send by the same channel
		atomic.AddInt32(&h.pending, 1)
		h.reqCh <- reqData{
			req:        req,
			compress:   comp,
			mustAnswer: true,
		}
		return
	}

	// No ongoing operations, we can send directly
	h.sender(true, req, comp, false)
}

func (h *connHandler) sendWorker() {
	for m := range h.reqCh {
		h.sender(m.mustAnswer, m.req, m.compress, true)
	}
}

func (h *connHandler) sender(mustAnswer bool, req *redis.Resp, compress, async bool) {
	if compress {
		req.Compress(lib.MinCompressSize, lib.MagicSnappy)
	}
	a, err := req.Array()
	if err != nil {
		if mustAnswer {
			respBadCommand.WriteTo(h.conn)
		}
		return
	}

	cmd, _ := a[0].Str()
	args := make([]interface{}, 0, len(a)-1)
	for _, v := range a {
		b, _ := v.Bytes()
		args = append(args, b)
	}

	resp := h.srv.pool.Cmd(cmd, args[1:])

	if h.srv.config.Compress || h.srv.config.Uncompress {
		resp.Uncompress(lib.MagicSnappy)
	}
	if mustAnswer {
		resp.WriteTo(h.conn)
	}
	if async {
		atomic.AddInt32(&h.pending, -1)
	}
	req.ReleaseBuffers()
	resp.ReleaseBuffers()
}
