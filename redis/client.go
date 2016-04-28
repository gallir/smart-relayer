package redis

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/go-bulk-relayer"
)

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	pingPeriod        = 3 * time.Second
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

	clt.Connect()
	go clt.pinger()

	return clt, nil
}

func (clt *Client) pinger() {
	for {
		if clt.Conn != nil {
			log.Println(time.Since(clt.Conn.UsedAt), clt.Conn.UsedAt)
		} else {
			log.Println("conn is nil")
		}
		if clt.Channel == nil {
			break
		}
		if clt.Conn != nil && time.Since(clt.Conn.UsedAt) > pingPeriod {
			log.Println("ping", time.Since(clt.Conn.UsedAt), clt.Conn.UsedAt)
			clt.Channel <- protoPing
		}
		time.Sleep(pingPeriod)
	}
	log.Println("Redis pinger exited")

}

func (clt *Client) Connect() bool {
	conn, err := net.Dial(clt.Proto, clt.Addr)
	if err != nil {
		log.Println("Failed to connect to", clt.Addr, err)
		clt.Conn = nil
		return false
	}
	fmt.Println("Connected to", conn.RemoteAddr())
	clt.Conn = relayer.NewConn(conn, parser)
	clt.Conn.ReadTimeout = time.Second * 15
	clt.Conn.WriteTimeout = time.Second * 15
	clt.pipelined = 0
	return true
}

// Listen for server messages in the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	for {
		request := <-clt.Channel
		if request == nil {
			break
		}
		//fmt.Println("To send", len(request))
		//time.Sleep(1 * time.Millisecond)
		_, err := clt.Write(request)
		if err != nil {
			log.Println("Error writing", err)
			clt.Close()
			continue
		}
		// clt.readAll()

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
		b, err := clt.Conn.Receive()
		log.Println("received", string(b))
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
			if !clt.Connect() {
				continue
			}
		}
		log.Println("Write bytes", len(b))
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
	clt.Channel <- nil
}
