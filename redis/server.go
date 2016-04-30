package redis

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/go-bulk-relayer/tools"
)

type Request struct {
	Conn    *Conn
	Command string
	Bytes   []byte
	Channel chan *Request
}

type Server struct {
	config *tools.RelayerConfig
	client *Client
	done   chan bool
}

var (
	protoOK                    = []byte("+OK\r\n")
	protoPing                  = []byte("PING\r\n")
	protoPong                  = []byte("+PONG\r\n")
	protoKO                    = []byte("-Error\r\n")
	protoClientCloseConnection = Request{Bytes: []byte("CLOSE")}
	protoClientExit            = Request{Bytes: []byte("EXIT")}
)

// Serve accepts incoming connections on the Listener l
func (srv *Server) Serve() error {
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", srv.config.Listen))
	if e != nil {
		log.Println("Error listening to port", fmt.Sprintf(":%d", srv.config.Listen), e)
		return e
	}

	log.Println("Starting redis server at port", srv.config.Listen)
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
			conn := NewConn(netConn)
			if err != nil {
				return
			}
			go srv.serveClient(conn)
		}
	}()

	return nil
}

func (srv *Server) serveClient(conn *Conn) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}

	}()

	tools.Debugf("New connection from %s\n", conn.RemoteAddr())
	responseCh := make(chan *Request, 1)
	started := time.Now()

	for {
		req := Request{Conn: conn}
		_, err := conn.Parse(&req)
		if err != nil {
			tools.Debugf("Finished session %s\n", time.Since(started))
			return err
		}
		response, ok := commands[req.Command]
		if ok {
			conn.Write(response)
			srv.client.channel <- &req
		} else {
			req.Channel = responseCh
			fmt.Println("Waiting for response")
			/*
				srv.client.channel <- &req
				response := <-responseCh
				conn.Write(response.Bytes)
			*/
			conn.Write(protoOK)
			srv.client.channel <- &req
		}
	}
}

func New(c *tools.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		config: c,
		done:   done,
	}
	return srv, nil
}
