package cluster

import (
	"net"
	"strings"
	"sync"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

const (
	requestBufferSize = 64
)

type connHandler struct {
	sync.Mutex
	seq    uint64
	last   uint64
	srv    *Server
	conn   net.Conn
	reqCh  chan reqData
	respCh chan *redis.Resp
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

func (h *connHandler) init() {
	h.reqCh = make(chan reqData, requestBufferSize)
	h.respCh = make(chan *redis.Resp, 1)
	go h.sender()
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
	cmd, err := m.First()
	if err != nil || strings.ToUpper(cmd) == selectCommand {
		return respBadCommand
	}

	doAsync := false
	var fastResponse *redis.Resp
	if h.srv.mode == lib.ModeSmart {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		h.reqCh <- reqData{
			req:      m,
			compress: h.srv.config.Compress,
		}
		return fastResponse
	}

	h.reqCh <- reqData{
		req:      m,
		compress: h.srv.config.Compress,
		answerCh: h.respCh,
	}
	return <-h.respCh
}

func (h *connHandler) sender() {
	for m := range h.reqCh {
		if m.compress {
			m.req.Compress(lib.MinCompressSize, lib.MagicSnappy)
		}
		a, err := m.req.Array()
		if err != nil {
			if m.answerCh != nil {
				m.answerCh <- respBadCommand
			}
		}
		cmd, _ := a[0].Str()
		args := make([]interface{}, 0, len(a)-1)
		for _, v := range a {
			b, _ := v.Bytes()
			args = append(args, b)
		}

		m.resp = h.srv.pool.Cmd(cmd, args[1:])
		if m.answerCh != nil {
			m.answerCh <- m.resp
		}
		m.req.ReleaseBuffers()
	}
}
