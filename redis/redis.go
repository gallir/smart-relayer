package redis

import (
	"container/list"
	"fmt"
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
	server        *Server
	conn          *Conn
	channel       chan *Request // The server sends the requests via this channel
	database      int           // The current selected database
	queued        *list.List    // The list of unanswered request
	listenerReady chan bool     // To signal the listener thread to read answers
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	requestBufferSize = 8192
	connectionIdleMax = 3 * time.Second
	selectCommand     = "SELECT"
	quitCommand       = "QUIT"
	modeSync          = 0
	modeSmart         = 1
)

var (
	protoOK                    = []byte("+OK\r\n")
	protoTrue                  = []byte(":1\r\n")
	protoPing                  = []byte("PING\r\n")
	protoPong                  = []byte("+PONG\r\n")
	protoKO                    = []byte("-Error\r\n")
	protoClientCloseConnection = Request{Bytes: []byte("CLOSE")}
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
		"QUIT":   protoOK,

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
		config: c,
		done:   done,
	}
	if c.Mode == "smart" {
		srv.Mode = modeSmart
	} else {
		srv.Mode = modeSync
	}
	return srv, nil
}
