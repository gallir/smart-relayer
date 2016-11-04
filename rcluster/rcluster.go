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
	cluster  *cluster.Cluster
	pool     *pool.Pool
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	requestBufferSize = 1024
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

	respOK   = redis.NewResp([]byte("OK"))
	respTrue = redis.NewResp(true)
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

// NewCluster creates a new Redis cluster client
func NewCluster(c lib.RelayerConfig, done chan bool) (*Server, error) {
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

func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.cluster == nil && srv.pool == nil {
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
				log.Println("Exiting", addr, e)
				return
			}
			go srv.handleConnection(netConn)
		}
	}()

	return nil
}

func (srv *Server) resetCluster() error {
	log.Printf("Reload and reset redis cluster server at port %s for target %s", srv.config.Listen, srv.config.Host())

	// Allows a list of URLs separated by spaces
	for _, url := range strings.Split(srv.config.URL, " ") {
		addr, err := lib.Host(url)
		if err != nil {
			continue
		}
		if srv.cluster, err = cluster.New(addr); err != nil {
			log.Printf("Error in cluster %s: %s", addr, err)
			srv.cluster = nil
			continue
		}
		lib.Debugf("Cluster linked to %s", addr)
		return nil
	}
	srv.cluster = nil
	return errors.New("no available redis cluster nodes")
}

func (srv *Server) resetPool() error {
	log.Printf("Reload and reset redis server at port %s for target %s", srv.config.Listen, srv.config.Host())

	var err error
	srv.pool, err = pool.New("tcp", srv.config.Host(), srv.config.MaxConnections)
	if err != nil {
		srv.pool = nil
		return errors.New("connection error")
	}

	lib.Debugf("Pool linked to %s", srv.config.Host())
	return nil
}

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	cl, e := srv.get()
	if e != nil {
		log.Println("error getting client", e)
		return
	}
	defer srv.put(cl)

	var asyncCh chan *redis.Resp

	if srv.mode == modeSmart {
		asyncCh = make(chan *redis.Resp, 64)
		defer close(asyncCh)
		go func() {
			cl, e := srv.get()
			if e != nil {
				log.Println("error getting client for async", e)
				return
			}
			defer srv.put(cl)

			for m := range asyncCh {
				cmd(cl, m, nil)
			}
		}()

	}

	rr := redis.NewRespReader(conn)
	for {
		err := conn.SetReadDeadline(time.Now().Add(listenTimeout * time.Second))
		if err != nil {
			log.Printf("error setting read deadline: %s", err)
			return
		}

		r := rr.Read()
		if redis.IsTimeout(r) {
			continue
		} else if r.IsType(redis.IOErr) {
			lib.Debugf("IOErr")
			return
		}

		if srv.config.Compress {
			r = compress(r)
		}

		resp := cmd(cl, r, asyncCh)
		if srv.config.Compress || srv.config.Uncompress {
			resp = uncompress(resp)
		}
		resp.WriteTo(conn)
	}

}

func (srv *Server) Exit() {
	if srv.listener != nil {
		srv.listener.Close()
	}
	srv.done <- true
}

func (srv *Server) get() (util.Cmder, error) {
	if srv.cluster != nil {
		return srv.cluster, nil
	}
	return srv.pool.Get()

}

func (srv *Server) put(c util.Cmder) {
	if srv.pool != nil {
		srv.pool.Put(c.(*redis.Client))
	}
}

func cmd(cl util.Cmder, m *redis.Resp, asyncCh chan *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return redis.NewResp(errBadCmd)
	}

	cmd, err := ms[0].Str()
	if err != nil {
		return redis.NewResp(errBadCmd)
	}

	doAsync := false
	var fastResponse *redis.Resp
	if asyncCh != nil {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		asyncCh <- m
		return fastResponse
	}

	args := make([]interface{}, 0, len(ms[1:]))
	for _, argm := range ms[1:] {
		arg, err := argm.Str()
		if err != nil {
			return redis.NewResp(errBadCmd)
		}
		args = append(args, arg)
	}

	return cl.Cmd(cmd, args...)
}

func compress(m *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return m
	}

	changed := false
	items := make([]interface{}, 0, len(ms))
	for _, item := range ms {
		if item.IsType(redis.Str) {
			b, e := item.Bytes()
			if e != nil {
				return m
			}
			if len(b) > minCompressSize {
				b = append(magicSnappy, snappy.Encode(nil, b)...)
				changed = true
			}
			items = append(items, b)
		}
	}

	if changed {
		return redis.NewResp(items)
	}
	return m

}

func uncompress(m *redis.Resp) *redis.Resp {
	if m.IsType(redis.Str) {
		uncompressed, e := uncompressItem(m)
		if e != nil {
			log.Println(e)
			return m
		}
		return redis.NewResp(uncompressed)
	}

	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return m
	}

	changed := false
	items := make([]interface{}, 0, len(ms))
	for _, item := range ms {
		b, e := uncompressItem(item)
		if b == nil { // Fatal error
			log.Println(e)
			return m
		}

		if e != nil {
			changed = true
		}
		items = append(items, b)
	}

	if changed {
		return redis.NewResp(items)
	}
	return m

}

func uncompressItem(item *redis.Resp) ([]byte, error) {
	if !item.IsType(redis.Str) {
		return nil, errors.New("not a str")
	}

	b, e := item.Bytes()
	if e != nil {
		return nil, errors.New("couldn't read bytes")
	}

	if bytes.HasPrefix(b, magicSnappy) {
		uncompressed, e := snappy.Decode(nil, b[len(magicSnappy):])
		if e == nil {
			b = uncompressed
		} else {
			return b, errors.New("error uncompressing")
		}
	}
	return b, nil
}
