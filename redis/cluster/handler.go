package cluster

import (
	"net"
	"strings"
	"sync/atomic"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
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
	if err != nil {
		respBadCommand.WriteTo(h.conn)
		return
	}

	cmd = strings.ToUpper(cmd)
	if cmd == selectCommand {
		respBadCommand.WriteTo(h.conn)
		return
	}

	doAsync := false
	var fastResponse *redis.Resp
	if h.srv.mode == lib.ModeSmart {
		if async, ok := h.srv.asynCommands.Load().(map[string]*redis.Resp); ok {
			fastResponse, doAsync = async[cmd]
		}
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
			compress:   (h.srv.config.Compress || h.srv.config.Gzip != 0) && cmd != evalCommand,
			mustAnswer: false,
		}
		return
	}

	p := atomic.LoadInt32(&h.pending)
	if p != 0 {
		// There are operations in queue, send by the same channel
		atomic.AddInt32(&h.pending, 1)
		h.reqCh <- reqData{
			req:        req,
			compress:   (h.srv.config.Compress || h.srv.config.Gzip != 0) && cmd != evalCommand,
			mustAnswer: true,
		}
		return
	}

	// No ongoing operations, we can send directly
	h.sender(true, req, h.srv.config.Compress && cmd != evalCommand, false)
}

func (h *connHandler) sendWorker() {
	for m := range h.reqCh {
		h.sender(m.mustAnswer, m.req, m.compress, true)
	}
}

func (h *connHandler) sender(mustAnswer bool, req *redis.Resp, compress bool, async bool) {
	if compress {
		switch {
		case h.srv.config.Gzip != 0:
			req.CompressGz(lib.MinCompressSize, h.srv.config.Gzip)
		case h.srv.config.Compress:
			req.CompressSnappy(lib.MinCompressSize, redis.MarkerSnappy)
		}
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
	if h.srv.config.Gunzip || h.srv.config.Gzip != 0 || h.srv.config.Compress || h.srv.config.Uncompress {
		resp.Uncompress()
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
