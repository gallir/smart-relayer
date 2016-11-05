package rcluster

import (
	"bytes"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/golang/snappy"
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
	minCompressSize   = 256
	listenTimeout     = 15
)

// errors
var (
	magicSnappy = []byte("$sy$")

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

	connType := srv.config.ListenScheme()
	addr := srv.config.ListenHost()

	// Check that the socket does not exist
	if connType == "unix" {
		if s, err := os.Stat(addr); err == nil {
			if (s.Mode() & os.ModeSocket) > 0 {
				// Remove existing socket
				os.Remove(addr)
			} else {
				log.Println("socket", addr, s.Mode(), os.ModeSocket)
				log.Fatalf("Socket %s exists and it's not a Unix socket", addr)
			}
		}
	}

	srv.listener, e = net.Listen(connType, addr)
	if e != nil {
		log.Println("Error listening to", addr, e)
		return e
	}

	if connType == "unix" {
		// Make sure is accesible for everyone
		os.Chmod(addr, 0777)
	}

	log.Printf("Starting redis server at %s for target %s", addr, srv.config.Host())
	// Serve a client
	go func() {
		for {
			netConn, e := srv.listener.Accept()
			if e != nil {
				log.Println("Exiting", addr)
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
			resp = uncompress(resp)
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
	if err != nil || strings.ToUpper(cmd) == "SELECT" {
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
		if srv.pool, err = cluster.NewWithOpts(cluster.Opts{Addr: addr, PoolSize: srv.config.MaxIdleConnections}); err != nil {
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
			if m.compress && len(b) > minCompressSize {
				b[i] = append(magicSnappy, snappy.Encode(nil, m.args[i])...)
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

func uncompress(m *redis.Resp) *redis.Resp {
	if m.IsType(redis.Str) {
		b := uncompressItem(m)
		if b == nil {
			return m
		}
		return redis.NewResp(b)
	}

	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return m
	}

	changed := false
	items := make([]interface{}, 0, len(ms))
	for _, item := range ms {
		b := uncompressItem(item)
		if b != nil {
			changed = true
			items = append(items, b)
			continue
		}

		b, e := item.Bytes()
		if e != nil {
			// Fatal error, return the same resp
			return m
		}
		items = append(items, b)
	}

	if changed {
		return redis.NewResp(items)
	}
	return m

}

func uncompressItem(item *redis.Resp) []byte {
	if !item.IsType(redis.Str) {
		return nil
	}

	b, e := item.Bytes()
	if e != nil {
		return nil
	}

	if bytes.HasPrefix(b, magicSnappy) {
		uncompressed, e := snappy.Decode(nil, b[len(magicSnappy):])
		if e == nil {
			return uncompressed
		}
	}
	return nil
}
