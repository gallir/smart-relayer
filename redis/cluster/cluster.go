package cluster

import (
	"errors"
	"log"
	"net"
	"strings"
	"sync"

	"time"

	"github.com/gallir/radix.improved/cluster"
	"github.com/gallir/radix.improved/pool"
	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/radix.improved/util"
	"github.com/gallir/smart-relayer/lib"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	mode     int
	done     chan bool
	exiting  bool
	listener net.Listener
	pool     util.Cmder
}

type reqData struct {
	req      *redis.Resp
	compress bool
	answerCh chan *redis.Resp
	resp     *redis.Resp
}

const (
	selectCommand = "SELECT"
)

// errors
var (
	errBadCmd = errors.New("ERR bad command")
	commands  map[string]*redis.Resp

	respOK         = redis.NewRespSimple("OK")
	respPong       = redis.NewRespSimple("PONG")
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
		"SADD":      respTrue,
		"ZADD":      respTrue,
		"EXPIRE":    respTrue,
		"EXPIREAT":  respTrue,
		"PEXPIRE":   respTrue,
		"PEXPIREAT": respTrue,
		"PING":      respPong,
	}
}

// New creates a new Redis cluster or pool client
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}

	err := srv.Reload(&c)
	if err != nil {
		log.Fatalln("no available redis cluster nodes", srv.config.URL)
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
	srv.mode = c.Type()

	if srv.config.Protocol == "redis-cluster" {
		return srv.reloadCluster(reset)
	}
	return srv.reloadPool(reset)
}

// Start listening in the specified local port
func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.pool == nil {
		return
	}

	srv.listener, e = lib.NewListener(srv.config)
	if e != nil {
		return e
	}

	// Serve clients
	go func() {
		for {
			netConn, e := srv.listener.Accept()
			if e != nil {
				if netErr, ok := e.(net.Error); ok && netErr.Timeout() {
					// Paranoid, ignore timeout errors
					log.Println("Timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if srv.exiting {
					log.Println("Exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("Emergency error in local listener", srv.config.ListenHost(), e)
				return
			}
			go Handle(srv, netConn)
		}
	}()

	return nil
}

func (srv *Server) reloadCluster(reset bool) error {
	if srv.pool != nil {
		p, ok := srv.pool.(*cluster.Cluster)
		if !ok {
			return errors.New("Relod cluster failed, bad type")
		}

		if !reset {
			log.Printf("Reload redis cluster server at port %s for target %s", srv.config.Listen, srv.config.Host())
			e := p.Reset()
			return e
		}
		log.Printf("Reset redis cluster server at port %s for target %s", srv.config.Listen, srv.config.Host())
		p.Close()
	}

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
		if srv.pool, err = cluster.NewWithOpts(cluster.Opts{
			Addr:     addr,
			PoolSize: size,
			Timeout:  time.Duration(srv.config.Timeout) * time.Second,
		}); err != nil {
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
func (srv *Server) reloadPool(reset bool) error {
	if srv.pool != nil {
		p, ok := srv.pool.(*pool.Pool)
		if !ok {
			return errors.New("Reload pool failed, bad type")
		}
		if !reset {
			log.Printf("Reload redis pool server at port %s for target %s", srv.config.Listen, srv.config.Host())
			return nil
		}
		log.Printf("Reset redis pool server at port %s for target %s", srv.config.Listen, srv.config.Host())
		p.Empty()
	}

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
	srv.exiting = true
	if srv.listener != nil {
		srv.listener.Close()
	}
	srv.done <- true
}
