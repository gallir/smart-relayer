package rcluster

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config     lib.RelayerConfig
	mode       int
	done       chan bool
	ListenAddr string
	RedisAddrs []string
	listener   net.Listener
	cluster    *cluster.Cluster
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	requestBufferSize = 1024
	modeSync          = 0
	modeSmart         = 1
)

// errors
var (
	errBadCmd = errors.New("ERR bad command")
)

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}
	srv.ListenAddr = c.Listen
	srv.RedisAddrs = append(srv.RedisAddrs, c.Host())

	srv.Reload(&c)
	if srv.cluster == nil {
		log.Println("no available redis cluster nodes for ", srv.RedisAddrs)
		return nil, errors.New("no available redis cluster nodes")
	}

	return srv, nil
}

func (srv *Server) Reload(c *lib.RelayerConfig) {
	srv.Lock()
	defer srv.Unlock()

	reset := false
	if srv.config.Url != c.Url {
		reset = true
	}
	srv.config = *c // Save a copy
	if c.Mode == "smart" {
		srv.mode = modeSmart
	} else {
		srv.mode = modeSync
	}
	if reset {
		log.Printf("Reload and reset redis cluster server at port %s for target %s", srv.config.Listen, srv.config.Host())
		var err error
		for _, addr := range srv.RedisAddrs {
			if srv.cluster, err = cluster.New(addr); err != nil {
				log.Printf("Error in cluster %s: %s", addr, err)
				srv.cluster = nil
				continue
			}
			return
		}
		log.Println("no available redis cluster nodes for ", srv.RedisAddrs)
	} else {
		log.Printf("Reload redis cluster config at port %s for target %s", srv.config.Listen, srv.config.Host())
	}
}

func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	if srv.cluster == nil {
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

	log.Printf("Starting redis cluster server at %s for target %s", addr, srv.config.Host())
	// Serve a client
	go func() {
		defer func() {
			srv.listener.Close()
			srv.done <- true
		}()

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

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	rr := redis.NewRespReader(conn)
	for {
		err := conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			log.Printf("error setting read deadline: %s", err)
			return
		}

		r := rr.Read()
		if redis.IsTimeout(r) {
			continue
		} else if r.IsType(redis.IOErr) {
			return
		}

		srv.Cmd(r).WriteTo(conn)
	}

}

func (srv *Server) Cmd(m *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return redis.NewResp(errBadCmd)
	}

	cmd, err := ms[0].Str()
	if err != nil {
		return redis.NewResp(errBadCmd)
	}

	args := make([]interface{}, 0, len(ms[1:]))
	for _, argm := range ms[1:] {
		arg, err := argm.Str()
		if err != nil {
			return redis.NewResp(errBadCmd)
		}
		args = append(args, arg)
	}

	return srv.cluster.Cmd(cmd, args...)
}

func (srv *Server) Exit() {
	if srv.listener != nil {
		srv.listener.Close()
	}
}
