package redis2

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	pool     *pool.Pool
	mode     int
	done     chan bool
	exiting  bool
	listener net.Listener
}

const (
	connectionRetries = 3
	requestBufferSize = 1024
	listenTimeout     = 0 * time.Second // Don't timeout on local clients
	connectTimeout    = 5 * time.Second
	maxIdle           = 15 * time.Second
	responseTimeout   = 30 * time.Second
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
	srv.Reload(&c)
	return srv, nil
}

// Start accepts incoming connections on the Listener
func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	srv.listener, e = lib.Listener(&srv.config)
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
				} else {
					log.Println("Error in local listener, exiting", srv.config.ListenHost(), e)
				}
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
	if srv.config.URL != c.URL {
		reset = true
	}
	srv.config = *c
	srv.mode = c.Type()

	if reset {
		if srv.pool != nil {
			log.Printf("Reset redis server at port %s for target %s", srv.config.Listen, srv.config.Host())
			srv.pool.Reset()
		}
		srv.pool = pool.New(c, NewClient)
	} else {
		log.Printf("Reload redis config at port %s for target %s", srv.config.Listen, srv.config.Host())
		srv.pool.ReadConfig(c)
	}

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

func (srv *Server) handleConnection(netCon net.Conn) {
	defer netCon.Close()

	conn := lib.NewNetReadWriter(netCon, listenTimeout, 0)
	defer conn.Flush()

	reader := redis.NewRespReader(conn)
	pooled, ok := srv.pool.Get()
	if !ok {
		log.Println("Redis server, no clients available from pool")
		return
	}
	defer srv.pool.Close(pooled)
	client := pooled.Client
	responseCh := make(chan *redis.Resp, 1)
	defer close(responseCh)

	currentDB := 0

	for {
		conn.Flush()
		r := reader.Read()
		if r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				// Paranoid, don't close it just log it
				log.Printf("Local client listen timeout at", srv.config.Listen)
				continue
			}
			// Connection was closed
			return
		}

		req := newRequest(r, &srv.config)
		if req == nil {
			respBadCommand.WriteTo(conn)
			continue
		}

		if req.database != unknownDB && req.database != currentDB {
			currentDB = req.database
		}
		req.database = currentDB

		// Smart mode, answer immediately and forget
		if srv.mode == lib.ModeSmart {
			fastResponse, ok := commands[string(req.command)]
			if ok {
				fastResponse.WriteTo(conn)
				ok = client.Send(req)
				if !ok {
					log.Printf("Error sending request to redis client, exiting")
					return
				}
				continue
			}
		}

		// Synchronized mode
		req.responseChannel = responseCh

		ok = client.Send(req)
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
