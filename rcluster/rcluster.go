package rcluster

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"strings"

	"bytes"

	"github.com/gallir/smart-relayer/lib"
	"github.com/golang/snappy"
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
	magicSnappy = []byte("$sy$")

	errBadCmd = errors.New("ERR bad command")
	commands  map[string][]byte

	protoOK   = []byte("+OK\r\n")
	protoTrue = []byte(":1\r\n")
	protoKO   = []byte("-Error\r\n")
)

func init() {
	// These are the commands that can be sent in "background" when in smart mode
	// The values are the immediate responses to the clients
	commands = map[string][]byte{
		"SET":       protoOK,
		"SETEX":     protoOK,
		"PSETEX":    protoOK,
		"MSET":      protoOK,
		"HMSET":     protoOK,
		"SELECT":    protoOK,
		"HSET":      protoTrue,
		"EXPIRE":    protoTrue,
		"EXPIREAT":  protoTrue,
		"PEXPIRE":   protoTrue,
		"PEXPIREAT": protoTrue,
	}
}

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
			lib.Debugf("Cluster linked to %s", addr)
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

	var asyncCh chan *redis.Resp

	if srv.mode == modeSmart {
		asyncCh = make(chan *redis.Resp, 64)
		defer close(asyncCh)
		go func() {
			for m := range asyncCh {
				srv.Cmd(m, nil)
			}
		}()

	}

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

		if srv.config.Compress {
			r = compress(r)
		}

		resp := srv.Cmd(r, asyncCh)
		if srv.config.Compress || srv.config.Uncompress {
			resp = uncompress(resp)
		}
		resp.WriteTo(conn)
	}

}

func (srv *Server) Cmd(m *redis.Resp, asyncCh chan *redis.Resp) *redis.Resp {
	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return redis.NewResp(errBadCmd)
	}

	cmd, err := ms[0].Str()
	if err != nil {
		return redis.NewResp(errBadCmd)
	}

	doAsync := false
	var fastResponse []byte
	if asyncCh != nil {
		fastResponse, doAsync = commands[strings.ToUpper(cmd)]
	}

	if doAsync {
		asyncCh <- m
		return redis.NewResp(fastResponse)
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
			if len(b) > 512 {
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
