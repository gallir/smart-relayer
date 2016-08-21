package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// Request stores the data for each client request
type Request struct {
	Conn    *Parser
	Command []byte
	//Bytes    []byte
	Buffer   *bytes.Buffer
	Channel  chan []byte // Channel to send the response to the original client
	Database int         // The current database at the time the request was issued
}

// Server is the thread that listen for clients' connections
type Server struct {
	config lib.RelayerConfig
	pool   *pool
	mode   int
	done   chan bool
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	requestBufferSize = 8192
	connectionIdleMax = 5 * time.Second
	modeSync          = 0
	modeSmart         = 1
	connectTimeout    = 5 * time.Second
	localReadTimeout  = 600 * time.Second
	serverReadTimeout = 5 * time.Second
	writeTimeout      = 5 * time.Second
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
	protoClientCloseConnection = Request{Command: closeConnectionCommand}
	protoClientReload          = Request{Command: reloadCommand}
	protoClientExit            = Request{Command: exitCommand}
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

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}
	srv.pool = newPool(srv, c.MaxConnections, c.MaxIdleConnections)
	srv.Reload(&c)
	return srv, nil
}

func (srv *Server) Protocol() string {
	return "redis"
}

func (srv *Server) Listen() string {
	return srv.config.Listen
}

// Start accepts incoming connections on the Listener l
func (srv *Server) Start() error {
	connType := srv.config.ListenScheme()
	addr := srv.config.ListenHost()

	// Check that the socket does not exist
	if connType == "unix" {
		if s, err := os.Stat(addr); err == nil {
			if (s.Mode() & os.ModeSocket) > 0 {
				// Remove existing socket
				log.Println("Warning, removing existing socket", addr)
				os.Remove(addr)
			} else {
				log.Println("socket", addr, s.Mode(), os.ModeSocket)
				log.Fatalf("Socket %s exists and it's not a Unix socket", addr)
			}
		}
	}

	l, e := net.Listen(connType, addr)
	if e != nil {
		log.Println("Error listening to", addr, e)
		return e
	}

	log.Printf("Starting redis server at %s for target %s", addr, srv.config.Host())
	// Serve a client
	go func() {
		defer func() {
			l.Close()
			srv.done <- true
		}()

		//client := NewClient(srv)
		// TODO:
		//defer client.Exit()
		// go client.Listen()

		for {
			netConn, err := l.Accept()
			if err != nil {
				return
			}
			go srv.serveClient(netConn)
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
		srv.mode = modeSmart
	} else {
		srv.mode = modeSync
	}
	if reload {
		log.Printf("Reloading redis server at port %s for target %s", srv.config.Listen, srv.config.Host())
		// TODO: pool reload
		// srv.client.requestsChan <- &protoClientReload

	}
}

func (srv *Server) Mode() int {
	return srv.mode
}

func (srv *Server) Config() *lib.RelayerConfig {
	return &srv.config
}

func (srv *Server) serveClient(netConn net.Conn) (err error) {
	defer netConn.Close()

	parser := newParser(netConn, localReadTimeout, writeTimeout)
	defer func() {
		if err != nil {
			fmt.Fprintf(parser, "-%s\n", err)
		}
		parser.Close()
	}()

	pooled := srv.pool.get()
	defer srv.pool.close(pooled)
	client := pooled.client

	// lib.Debugf("New connection from %s", netConn.RemoteAddr())
	// started := time.Now()
	responseCh := make(chan []byte, 1)

	for {
		req := Request{Conn: parser}
		_, err = parser.Read(&req, true)
		if err != nil {
			break
		}

		// QUIT received from client
		if bytes.Compare(req.Command, quitCommand) == 0 {
			parser.NetBuf.Write(protoOK)
			break
		}

		req.Database = parser.Database

		// Smart mode, answer immediately and forget
		if srv.mode == modeSmart {
			fastResponse, ok := commands[string(req.Command)]
			if ok {
				parser.NetBuf.Write(fastResponse)
				client.requestsChan <- &req
				continue
			}
		}

		// Synchronized mode
		req.Channel = responseCh
		client.requestsChan <- &req
		response := <-responseCh
		parser.NetBuf.Write(response)
	}
	// lib.Debugf("Finished session %s", time.Since(started))
	return err
}
