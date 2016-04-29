package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/go-bulk-relayer"
)

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	connectionIdleMax = 3 * time.Second
)

type Client struct {
	Proto     string
	Addr      string // TCP address to listen on, ":6389" if empty
	Conn      *relayer.Conn
	Channel   chan []byte
	pipelined int
}

func NewClient() (*Client, error) {
	proto := "tcp"
	//addr := "127.0.0.1"
	addr := "192.168.111.2"
	port := 6379

	clt := &Client{
		Proto: "tcp",
	}

	if proto == "unix" {
		clt.Addr = addr
	} else {
		clt.Addr = fmt.Sprintf("%s:%d", addr, port)
	}

	clt.Channel = make(chan []byte, 4096)
	defer clt.Close()

	return clt, nil
}

func (clt *Client) checkIdle() {
	for {
		if clt.Channel == nil {
			break
		}
		if clt.Conn != nil && time.Since(clt.Conn.UsedAt) > connectionIdleMax {
			clt.Channel <- protoClientCloseConnection
		}
		time.Sleep(connectionIdleMax)
	}
}

func (clt *Client) Connect() bool {
	conn, err := net.Dial(clt.Proto, clt.Addr)
	if err != nil {
		log.Println("Failed to connect to", clt.Addr, err)
		clt.Conn = nil
		return false
	}
	relayer.Debugf("Connected to %s\n", conn.RemoteAddr())
	clt.Conn = relayer.NewConn(conn, parser)
	clt.Conn.ReadTimeout = time.Second * 10
	clt.Conn.WriteTimeout = time.Second * 10
	clt.pipelined = 0
	return true
}

// Listen for server messages in the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	go clt.checkIdle()

	for {
		request := <-clt.Channel
		if bytes.Compare(request, protoClientExit) == 0 {
			break
		}
		if bytes.Compare(request, protoClientCloseConnection) == 0 {
			relayer.Debugf("Closing by idle %s\n", clt.Conn.RemoteAddr())
			clt.Close()
			continue
		}

		_, err := clt.Write(request)
		if err != nil {
			log.Println("Error writing", err)
			clt.Close()
			continue
		}
		if clt.pipeline() {
			clt.readAll()
		} else {
			clt.Close()
		}
	}
	fmt.Println("Finished Redis client", time.Since(started))
}

func (clt *Client) pipeline() bool {
	for i := 0; i < pipelineCommands; i++ {
		select {
		case request := <-clt.Channel:
			_, err := clt.Write(request)
			if err != nil {
				log.Println("Error writing in pipeline", clt.pipelined, err)
				return false
			}
		default:
			break
		}
	}
	return clt.readAll()
}

func (clt *Client) readAll() bool {
	for clt.pipelined > 0 {
		_, err := clt.Conn.Receive()
		if err != nil {
			log.Println("Error receiving in readAll", err)
			return false
		}
		clt.pipelined--
	}
	return true
}

func (clt *Client) Write(b []byte) (int, error) {
	for i := 0; i < connectionRetries; i++ {
		if clt.Conn == nil {
			if i > 0 {
				time.Sleep(time.Duration(i*2) * time.Second)
			}
			if !clt.Connect() {
				continue
			}
		}
		c, err := clt.Conn.Write(b)
		if err != nil {
			clt.Close()
			if neterr, ok := err.(net.Error); !ok || !neterr.Timeout() {
				log.Println("Failed in write:", err)
			}
		} else {
			clt.pipelined++
			return c, err
		}
	}
	return 0, fmt.Errorf("Too many failed connections")
}

func (clt *Client) Close() {
	if clt.Conn != nil {
		clt.Conn.Close()
		clt.Conn = nil
		clt.pipelined = 0
	}
}

func (clt *Client) Exit() {
	clt.Close()
	clt.Channel <- protoClientExit
}
