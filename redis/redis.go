package redis

import (
	"container/list"
	"fmt"
	"time"

	"github.com/gallir/smart-relayer/tools"
)

type Request struct {
	Conn     *Conn
	Command  string
	Bytes    []byte
	Channel  chan []byte // Channel to send the response to the original client
	Database int
}

type Server struct {
	config tools.RelayerConfig
	client *Client
	done   chan bool
}

type Client struct {
	server    *Server
	conn      *Conn
	channel   chan *Request
	database  int
	pipelined int
	queued    *list.List
	serial    int
}

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	connectionIdleMax = 3 * time.Second
	selectCommand     = "SELECT"
)

var (
	protoOK                    = []byte("+OK\r\n")
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
	commands = map[string][]byte{
		"PING":   []byte("+PONG\r\n"),
		"SET":    []byte("+OK\r\n"),
		"SETEX":  []byte("+OK\r\n"),
		"PSETEX": []byte("+OK\r\n"),
		"MSET":   []byte("+OK\r\n"),
		"HMSET":  []byte("+OK\r\n"),

		"SELECT": []byte("+OK\r\n"),

		"DEL":       []byte(":1\r\n"),
		"HSET":      []byte(":1\r\n"),
		"HDEL":      []byte(":1\r\n"),
		"EXPIRE":    []byte(":1\r\n"),
		"EXPIREAT":  []byte(":1\r\n"),
		"PEXPIRE":   []byte(":1\r\n"),
		"PEXPIREAT": []byte(":1\r\n"),
	}
}
