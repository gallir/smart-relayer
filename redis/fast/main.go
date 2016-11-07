package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/pool"
)

// Request stores the data for each client request
type Request struct {
	command         []byte
	buffer          *bytes.Buffer
	responseChannel chan []byte // Channel to send the response to the original client
	database        int         // The current database at the time the request was issued
	mode            int
}

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	pool     *pool.Pool
	mode     int
	done     chan bool
	listener net.Listener
}

const (
	connectionRetries = 3
	requestBufferSize = 128
	connectTimeout    = 5 * time.Second
	serverReadTimeout = 5 * time.Second
	writeTimeout      = 5 * time.Second
	maxIdle           = 10 * time.Second
	responseTimeout   = 20 * time.Second
	localReadTimeout  = 600 * time.Second
)

var (
	selectCommand          = []byte("SELECT")
	quitCommand            = []byte("QUIT")
	closeConnectionCommand = []byte("CLOSE")
	reloadCommand          = []byte("RELOAD")
	exitCommand            = []byte("EXIT")

	protoOK                    = []byte("+OK\r\n")
	protoTrue                  = []byte(":1\r\n")
	protoPing                  = []byte("PING\r\n")
	protoPong                  = []byte("+PONG\r\n")
	protoKO                    = []byte("-Error\r\n")
	protoClientCloseConnection = Request{command: closeConnectionCommand}
)

var commands map[string][]byte

func getSelect(n int) []byte {
	str := fmt.Sprintf("%d", n)
	return []byte(fmt.Sprintf("*2\r\n$6\r\n%s\r\n$%d\r\n%s\r\n", selectCommand, len(str), str))
}

func init() {
	// These are the commands that can be sent in "background" when in smart mode
	// The values are the immediate responses to the clients
	commands = map[string][]byte{
		"PING":      protoPong,
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
		// "DEL":       protoTrue,  // Disabled until we add a throttling down option
		// "HDEL":      protoTrue,  // because DEL by patern can be very expensive and kill the server
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

// Start accepts incoming connections on the Listener l
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
				log.Println("Exiting", srv.config.ListenHost())
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
	srv.config = *c // Save a copy
	srv.mode = c.Type()
	if reset {
		if srv.pool != nil {
			log.Printf("Reset redis server at port %s for target %s", srv.config.Listen, srv.config.Host())
			srv.pool.Reset()
		}
		srv.pool = pool.New(c, NewClient)
	} else {
		log.Printf("Reload redis server at port %s for target %s", srv.config.Listen, srv.config.Host())
		srv.pool.ReadConfig(c)
	}
	return nil
}

// Config returns the server configuration
func (srv *Server) Config() *lib.RelayerConfig {
	return &srv.config
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	if srv.listener != nil {
		srv.listener.Close()
	}
	srv.done <- true
}

func (srv *Server) handleConnection(netConn net.Conn) {
	conn := lib.NewNetbuf(netConn, localReadTimeout, writeTimeout)
	defer netConn.Close()

	parser := newParser(conn)

	pooled, ok := srv.pool.Get()
	if !ok {
		log.Println("Redis server, no clients available from pool")
		return
	}
	defer srv.pool.Close(pooled)
	client := pooled.Client
	responseCh := make(chan []byte, 1)
	defer close(responseCh)

	for {
		req := &Request{}
		_, err := parser.read(req, true)
		if err != nil {
			return
		}

		// QUIT received from client
		if bytes.Compare(req.command, quitCommand) == 0 {
			conn.Write(protoOK)
			return
		}

		req.database = parser.database

		// Smart mode, answer immediately and forget
		if srv.mode == lib.ModeSmart {
			fastResponse, ok := commands[string(req.command)]
			if ok {
				ok = client.Send(req)
				if !ok {
					log.Printf("Error sending request to redis client, exiting")
					return
				}
				conn.Write(fastResponse)
				continue
			}
		}

		// Synchronized mode
		req.responseChannel = responseCh

		ok := client.Send(req)
		if !ok {
			log.Printf("Error sending request to redis client, exiting")
			conn.Write(protoKO)
			return
		}

		select {
		case response, more := <-responseCh:
			if !more {
				// The client has closed the channel
				lib.Debugf("Redis client has closed channel, exiting")
				return
			}
			conn.Write(response)
		case <-time.After(responseTimeout):
			// Something very wrong happened in the client
			log.Println("Timeout waiting a response, closing client connection")
			conn.Write(protoKO)
			return
		}
	}
}

func sendAsyncResponse(c chan []byte, b []byte) (ok bool) {
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("sendAssendAsyncResponseyncRequest: Recovered from error %s, channel length %d", e, len(c))
			ok = false
		}
	}()

	if c == nil {
		return
	}

	select {
	case c <- b:
		ok = true
	default:
		lib.Debugf("Error sending response %s channel length %d", string(b), len(c))
		ok = false
	}
	return
}
