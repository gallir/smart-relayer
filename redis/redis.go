package redis

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// It stores the data for each client request
type Request struct {
	Conn     *Conn
	Command  string
	Bytes    []byte
	Channel  chan []byte // Channel to send the response to the original client
	Database int         // The current database at the time the request was issued
}

// Server is the thread that listen for clients' connections
type Server struct {
	config lib.RelayerConfig
	client *Client
	Mode   int
	done   chan bool
}

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	server       *Server
	conn         *Conn
	channel      chan *Request // The server sends the requests via this channel
	database     int           // The current selected database
	sentRequests chan *Request // Requests sent to the server
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	requestBufferSize = 8192
	connectionIdleMax = 5 * time.Second
	selectCommand     = "SELECT"
	quitCommand       = "QUIT"
	modeSync          = 0
	modeSmart         = 1
	connectTimeout    = 5 * time.Second
	localReadTimeout  = 600 * time.Second
	serverReadTimeout = 5 * time.Second
	writeTimeout      = 5 * time.Second
)

var (
	protoOK                    = []byte("+OK\r\n")
	protoTrue                  = []byte(":1\r\n")
	protoPing                  = []byte("PING\r\n")
	protoPong                  = []byte("+PONG\r\n")
	protoKO                    = []byte("-Error\r\n")
	protoClientCloseConnection = Request{Bytes: []byte("CLOSE")}
	protoClientReload          = Request{Bytes: []byte("RELOAD")}
	protoClientExit            = Request{Bytes: []byte("EXIT")}
)

var commands map[string][]byte

func getSelect(n int) []byte {
	str := fmt.Sprintf("%d", n)
	return []byte(fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(str), str))
}

func init() {
	// These are the commands that can be sent in "background" when in smart mode
	// The values are the immediate responses to the clients
	commands = map[string][]byte{
		"PING":   protoPong,
		"SET":    protoOK,
		"SETEX":  protoOK,
		"PSETEX": protoOK,
		"MSET":   protoOK,
		"HMSET":  protoOK,

		"SELECT": protoOK,

		"DEL":       protoTrue,
		"HSET":      protoTrue,
		"HDEL":      protoTrue,
		"EXPIRE":    protoTrue,
		"EXPIREAT":  protoTrue,
		"PEXPIRE":   protoTrue,
		"PEXPIREAT": protoTrue,
	}
}

// New creates a new Redis server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}
	srv.Reload(&c)
	return srv, nil
}

func (srv *Server) Protocol() string {
	return "redis"
}

func (srv *Server) Port() int {
	return srv.config.Listen
}

// Serve accepts incoming connections on the Listener l
func (srv *Server) Start() error {
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", srv.config.Listen))
	if e != nil {
		log.Println("Error listening to port", fmt.Sprintf(":%d", srv.config.Listen), e)
		return e
	}

	log.Printf("Starting redis server at port %d for target %s", srv.config.Listen, srv.config.Host())
	go func() {
		defer func() {
			l.Close()
			srv.client.Exit()
			srv.done <- true
		}()

		srv.client, _ = NewClient(srv)
		go srv.client.Listen()

		for {
			netConn, err := l.Accept()
			if err != nil {
				return
			}
			conn := NewConn(netConn, localReadTimeout)
			go srv.serveClient(conn)
		}
	}()

	return nil
}

func (srv *Server) Reload(c *lib.RelayerConfig) {
	reload := false
	if srv.config.Url != "" {
		reload = true
	}
	srv.config = *c // Save a copy
	if c.Mode == "smart" {
		srv.Mode = modeSmart
	} else {
		srv.Mode = modeSync
	}
	if reload {
		log.Printf("Reloading redis server at port %d for target %s", srv.config.Listen, srv.config.Host())
		srv.client.channel <- &protoClientReload

	}
}

func (srv *Server) serveClient(conn *Conn) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.close()
	}()

	lib.Debugf("New connection from %s", conn.remoteAddr())
	responseCh := make(chan []byte, 1)
	started := time.Now()

	for {
		req := Request{Conn: conn}
		_, err = conn.parse(&req, true)
		if err != nil {
			break
		}

		// QUIT received from client
		if req.Command == quitCommand {
			conn.Write(protoOK)
			break
		}

		req.Database = conn.Database

		// Smart mode, answer immediately and forget
		if srv.Mode == modeSmart {
			fastResponse, ok := commands[req.Command]
			if ok {
				conn.Write(fastResponse)
				srv.client.channel <- &req
				continue
			}
		}

		// Synchronized mode
		req.Channel = responseCh
		srv.client.channel <- &req
		response := <-responseCh
		conn.Write(response)
	}
	lib.Debugf("Finished session %s", time.Since(started))
	return err
}
