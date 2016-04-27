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
	pipelineCommands  = 100
)

type Client struct {
	Proto   string
	Addr    string // TCP address to listen on, ":6389" if empty
	Conn    *relayer.Conn
	Channel chan []byte
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
	clt.Connect()
	defer clt.Close()

	return clt, nil
}

func (clt *Client) Connect() bool {
	clt.Conn = nil
	conn, err := net.Dial(clt.Proto, clt.Addr)
	if err != nil {
		log.Println("Failed to connect to", clt.Addr, err)
		return false
	}
	fmt.Println("Connected to", conn.RemoteAddr())
	clt.Conn = relayer.NewConn(conn, parser)
	clt.Conn.ReadTimeout = time.Second * 15
	clt.Conn.WriteTimeout = time.Second * 15
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

		if clt.pipeline() {
			clt.Conn.Receive()
		}
	}
	fmt.Println("Finished session", time.Since(started))
}

func (clt *Client) pipeline() bool {
	pipelined := 0
	for i := 0; i < pipelineCommands; i++ {
		select {
		case request := <-clt.Channel:
			_, err := clt.Write(request)
			if err != nil {
				log.Println("Error writing in pipeline", pipelined, err)
				return false
			}
			pipelined++
		default:
			break
		}
	}
	/*
		if pipelined > 0 {
			fmt.Println("Pipelined:", pipelined)
		}
	*/
	for i := 0; i < pipelined; i++ {
		_, err := clt.Conn.Receive()
		if err != nil {
			log.Println("Error receiving in pipeline", err)
			return false
		}
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
		c, err := clt.Conn.Write(b)
		if err != nil {
			clt.Close()
			log.Println("Failed in write:", err)
		} else {
			return c, err
		}
	}
	return 0, fmt.Errorf("Too many failed connections")
}

func (clt *Client) Close() {
	if clt.Conn != nil {
		clt.Conn.Close()
		clt.Conn = nil
	}
}

func (clt *Client) Exit() {
	clt.Close()
	clt.Channel <- nil
}
