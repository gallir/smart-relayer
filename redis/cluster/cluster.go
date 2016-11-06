package cluster

import (
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	mode     int
	done     chan bool
	listener net.Listener
	//	cluster  *cluster.Cluster
	pool util.Cmder
}

type reqData struct {
	cmd      string
	args     [][]byte
	compress bool
	answerCh chan *redis.Resp
}

const (
	requestBufferSize = 64
	modeSync          = 0
	modeSmart         = 1
	listenTimeout     = 15
	selectCommand     = "SELECT"
)

// errors
var (
	errBadCmd = errors.New("ERR bad command")
	commands  map[string]*redis.Resp

	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
)

func init() {
	// These are the commands that can be sent in "background" when in smart mode
	// The values are the immediate responses to the clients
	commands = map[string]*redis.Resp{
		"SET":       respOK,
		"SETEX":     respOK,
		"PSETEX":    respOK,
		"MSET":      respOK,
		"HMSET":     respOK,
		"SELECT":    respOK,
		"HSET":      respTrue,
		"EXPIRE":    respTrue,
		"EXPIREAT":  respTrue,
		"PEXPIRE":   respTrue,
		"PEXPIREAT": respTrue,
	}
}

// New creates a new Redis cluster or pool client
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}

	err := srv.Reload(&c)
	if err != nil {
		log.Println("no available redis cluster nodes", srv.config.URL)
		return nil, err
	}

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) error {
	srv.Lock()
	defer srv.Unlock()

	reset := false
	if srv.config.URL != c.URL {
		reset = true
	}
	srv.config = *c // Save a copy
	if c.Mode == "smart" {
		srv.mode = modeSmart
	} else {
		srv.mode = modeSync
	}

	if reset {
		if srv.config.Protocol == "redis-cluster" {
			return srv.resetCluster()
		}
		return srv.resetPool()
	}

	return nil
}

// Start listening in the specified local port
func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.pool == nil {
		return
	}

	srv.listener, e = lib.Listener(&srv.config)
	if e != nil {
		return e
	}

	// Serve clients
	go func() {
		for {
			netConn, e := srv.listener.Accept()
			if e != nil {
				log.Println("Exiting", srv.config.ListenHost())
				return
			}
			go srv.handleConnection(netConn)
		}
	}()

	return nil
}

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reqCh := make(chan *reqData, requestBufferSize)
	defer close(reqCh)

	go sender(srv.pool, reqCh)

	respCh := make(chan *redis.Resp)
	reader := redis.NewRespReader(conn)
	for {
		err := conn.SetReadDeadline(time.Now().Add(listenTimeout * time.Second))
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

		resp := srv.process(req, reqCh, respCh)
		if srv.config.Compress || srv.config.Uncompress {
			resp = compress.Uncompress(resp)
		}
		resp.WriteTo(conn)
	}
}

func (srv *Server) process(m *redis.Resp, reqCh chan *reqData, respCh chan *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return respBadCommand
	}

	cmd, err := ms[0].Str()
	if err != nil || strings.ToUpper(cmd) == selectCommand {
		return respBadCommand
	}

	args := make([][]byte, 0, len(ms[1:]))
	for _, argm := range ms[1:] {
		arg, err := argm.Bytes()
		if err != nil {
			return respBadCommand
		}
		args = append(args, arg)
	}

	data := reqData{
		cmd:      cmd,
		args:     args,
		compress: srv.config.Compress,
	}

	doAsync := false
	var fastResponse *redis.Resp
	if srv.mode == modeSmart {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		reqCh <- &data
		return fastResponse
	}

	data.answerCh = respCh
	reqCh <- &data
	return <-respCh
}

func (srv *Server) resetCluster() error {
	log.Printf("Reload and reset redis cluster server at port %s for target %s", srv.config.Listen, srv.config.Host())

	// Allows a list of URLs separated by spaces
	for _, url := range strings.Split(srv.config.URL, " ") {
		addr, err := lib.Host(url)
		if err != nil {
			continue
		}

		// Choose the highest value from config
		size := 0
		if srv.config.MaxIdleConnections > srv.config.MaxConnections {
			size = srv.config.MaxIdleConnections
		} else {
			size = srv.config.MaxConnections
		}
		if srv.pool, err = cluster.NewWithOpts(cluster.Opts{Addr: addr, PoolSize: size}); err != nil {
			log.Printf("Error in cluster %s: %s", addr, err)
			srv.pool = nil
			continue
		}
		lib.Debugf("Cluster linked to %s", addr)
		return nil
	}
	srv.pool = nil
	return errors.New("no available redis cluster nodes")
}

// The pool is only for testing, it doesn't ensure the use of the select'ed database
func (srv *Server) resetPool() error {
	log.Printf("Reload and reset redis server at port %s for target %s", srv.config.Listen, srv.config.Host())

	var err error
	srv.pool, err = pool.New("tcp", srv.config.Host(), srv.config.MaxIdleConnections)
	if err != nil {
		srv.pool = nil
		return errors.New("connection error")
	}

	lib.Debugf("Pool linked to %s", srv.config.Host())
	return nil
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	if srv.listener != nil {
		srv.listener.Close()
	}
	srv.done <- true
}

func sender(cl util.Cmder, reqCh chan *reqData) {
	for m := range reqCh {
		b := make([]interface{}, len(m.args))
		for i := range m.args {
			if m.compress && len(m.args[i]) > compress.MinCompressSize {
				b[i] = compress.CompressBytes(m.args[i])
			} else {
				b[i] = m.args[i]
			}
		}
		resp := cl.Cmd(m.cmd, b...)
		if m.answerCh != nil {
			m.answerCh <- resp
		}
	}
}
