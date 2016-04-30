package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/go-bulk-relayer/tools"
)

const (
	connectionRetries = 3
	pipelineCommands  = 1000
	connectionIdleMax = 3 * time.Second
)

type Client struct {
	server    *Server
	conn      *Conn
	channel   chan *Request
	database  int
	pipelined int
}

func NewClient(s *Server) (*Client, error) {
	clt := &Client{
		server: s,
	}

	clt.channel = make(chan *Request, 4096)
	defer clt.Close()

	return clt, nil
}

func (clt *Client) checkIdle() {
	for {
		if clt.channel == nil {
			break
		}
		if clt.conn != nil && time.Since(clt.conn.UsedAt) > connectionIdleMax {
			clt.channel <- &protoClientCloseConnection
		}
		time.Sleep(connectionIdleMax)
	}
}

func (clt *Client) Connect() bool {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", clt.server.config.Host, clt.server.config.Port))
	if err != nil {
		log.Println("Failed to connect to", clt.server.config.Host, clt.server.config.Port)
		clt.conn = nil
		return false
	}
	tools.Debugf("Connected to %s\n", conn.RemoteAddr())
	clt.conn = NewConn(conn)
	clt.conn.ReadTimeout = time.Second * 10
	clt.conn.WriteTimeout = time.Second * 10
	clt.pipelined = 0
	return true
}

// Listen for server messages in the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	go clt.checkIdle()

	for {
		request := <-clt.channel
		if bytes.Compare(request.Bytes, protoClientExit.Bytes) == 0 {
			break
		}
		if bytes.Compare(request.Bytes, protoClientCloseConnection.Bytes) == 0 {
			tools.Debugf("Closing by idle %s\n", clt.conn.RemoteAddr())
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
		case request := <-clt.channel:
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
	var req Request
	for clt.pipelined > 0 {
		resp, err := clt.conn.Parse(&req)
		fmt.Println("response", string(resp))
		if err != nil {
			log.Println("Error receiving in readAll", err)
			return false
		}
		clt.pipelined--
	}
	return true
}

func (clt *Client) Write(r *Request) (int, error) {
	for i := 0; i < connectionRetries; i++ {
		if clt.conn == nil {
			if i > 0 {
				time.Sleep(time.Duration(i*2) * time.Second)
			}
			if !clt.Connect() {
				continue
			}
		}

		log.Println("Databse", r.Conn.Database, clt.database)
		if clt.database != r.Conn.Database {
			fmt.Println("changed database to", r.Conn.Database)
			_, err := clt.conn.Write(getSelect(r.Conn.Database))
			if err != nil {
				log.Println("Error changing database", err)
				clt.Close()
				return 0, fmt.Errorf("Error in select")
			}
			clt.database = r.Conn.Database
		}
		c, err := clt.conn.Write(r.Bytes)

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
	if clt.conn != nil {
		clt.conn.Close()
		clt.conn = nil
		clt.pipelined = 0
		clt.database = 0
	}
}

func (clt *Client) Exit() {
	clt.Close()
	clt.channel <- &protoClientExit
}
