package redis2

import (
	"bufio"
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/mediocregopher/radix.v2/redis"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	Config   lib.RelayerConfig
	pool     *pool
	mode     int
	done     chan bool
	listener net.Listener
}

const (
	listenTimeout     = 15
	connectionRetries = 3
	requestBufferSize = 1024
	modeSync          = 0
	modeSmart         = 1
	connectTimeout    = 5 * time.Second
	serverReadTimeout = 5 * time.Second
	writeTimeout      = 5 * time.Second
	maxIdle           = 10 * time.Second
	responseTimeout   = 20 * time.Second
	localReadTimeout  = 600 * time.Second
	selectCommand     = "SELECT"
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	commands       map[string]*redis.Resp
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

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}
	srv.pool = newPool(srv, &c)
	srv.Reload(&c)
	return srv, nil
}

// Start accepts incoming connections on the Listener l
func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	connType := srv.Config.ListenScheme()
	addr := srv.Config.ListenHost()

	// Check that the socket does not exist
	if connType == "unix" {
		if s, err := os.Stat(addr); err == nil {
			if (s.Mode() & os.ModeSocket) > 0 {
				// Remove existing socket
				// log.Println("Warning, removing existing socket", addr)
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

	log.Printf("Starting redis server at %s for target %s", addr, srv.Config.Host())
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

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) error {
	srv.Lock()
	defer srv.Unlock()

	reset := false
	if srv.Config.URL != "" && srv.Config.URL != c.URL {
		reset = true
	}
	srv.Config = *c // Save a copy
	if c.Mode == "smart" {
		srv.mode = modeSmart
	} else {
		srv.mode = modeSync
	}
	if reset {
		log.Printf("Reload and reset redis server at port %s for target %s", srv.Config.Listen, srv.Config.Host())
		srv.pool.reset(c)
		srv.pool = newPool(srv, c)
	} else {
		log.Printf("Reload redis config at port %s for target %s", srv.Config.Listen, srv.Config.Host())
		srv.pool.readConfig(c)
	}

	return nil
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	if srv.listener != nil {
		srv.listener.Close()
	}
	srv.done <- true
}

func (srv *Server) handleConnection(netCon net.Conn) {
	defer netCon.Close()

	conn := bufio.NewReadWriter(bufio.NewReader(netCon), bufio.NewWriter(netCon))
	defer conn.Flush()

	reader := redis.NewRespReader(conn)
	pooled, ok := srv.pool.get()
	if !ok {
		log.Println("Redis server, no clients available from pool")
		return
	}
	defer srv.pool.close(pooled)
	client := pooled.client
	responseCh := make(chan *redis.Resp, 1)
	defer close(responseCh)

	currentDB := 0

	for {
		conn.Flush()
		err := netCon.SetReadDeadline(time.Now().Add(listenTimeout * time.Second))
		if err != nil {
			log.Printf("error setting read deadline: %s", err)
			return
		}

		r := reader.Read()
		if redis.IsTimeout(r) {
			continue
		} else if r.IsType(redis.IOErr) {
			return
		}

		req := newRequest(r)
		if req == nil {
			respBadCommand.WriteTo(conn)
			continue
		}

		if req.database != unknownDB && req.database != currentDB {
			currentDB = req.database
		}
		req.database = currentDB

		// Smart mode, answer immediately and forget
		if srv.mode == modeSmart {
			fastResponse, ok := commands[string(req.command)]
			if ok {
				fastResponse.WriteTo(conn)
				ok = sendRequest(client.requestChan, req)
				if !ok {
					log.Printf("Error sending request to redis client, exiting")
					return
				}
				continue
			}
		}

		// Synchronized mode
		req.responseChannel = responseCh

		ok := sendRequest(client.requestChan, req)
		if !ok {
			log.Printf("Error sending request to redis client, exiting")
			respKO.WriteTo(conn)
			return
		}

		select {
		case response, more := <-responseCh:
			if !more {
				// The client has closed the channel
				lib.Debugf("Redis client has closed channel, exiting")
				return
			}
			response.WriteTo(conn)
		case <-time.After(responseTimeout):
			// Something very wrong happened in the client
			log.Println("Timeout waiting a response, closing client connection")
			respKO.WriteTo(conn)
			return
		}
	}
}

func sendRequest(c chan *Request, r *Request) (ok bool) {
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("sendRequest: Recovered from error %s, channel length %d", e, len(c))
			ok = false
		}
	}()

	if c == nil {
		lib.Debugf("Nil channel in send request")
		return false
	}

	c <- r
	return true
}

func sendAsyncRequest(c chan *Request, r *Request) (ok bool) {
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("sendAsyncRequest: Recovered from error %s, channel length %d", e, len(c))
			ok = false
		}
	}()

	if c == nil {
		return
	}

	select {
	case c <- r:
		ok = true
	default:
		lib.Debugf("Error sending request, channel length %d", len(c))
		ok = false
	}
	return
}

func sendAsyncResponse(c chan *redis.Resp, r *redis.Resp) (ok bool) {
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("AsyncResponseyncRequest: Recovered from error %s, channel length %d", e, len(c))
			ok = false
		}
	}()

	if c == nil {
		return
	}

	select {
	case c <- r:
		ok = true
	default:
		lib.Debugf("Error sending response to channel length %d", len(c))
		ok = false
	}
	return
}
