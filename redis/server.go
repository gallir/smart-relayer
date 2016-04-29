package redis

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gallir/go-bulk-relayer"
)

type Request struct {
	Name  string
	Bytes []byte
	Host  string
	Body  io.ReadCloser
}

type Server struct {
	Proto  string
	Addr   string // TCP address to listen on, ":6389" if empty
	client *Client
}

var (
	protoOK                    = []byte("+OK\r\n")
	protoPing                  = []byte("PING\r\n")
	protoPong                  = []byte("+PONG\r\n")
	protoKO                    = []byte("-Error\r\n")
	protoClientCloseConnection = []byte("CLOSE")
	protoClientExit            = []byte("EXIT")
)

func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if srv.Proto == "" {
		srv.Proto = "tcp"
	}
	if srv.Proto == "unix" && addr == "" {
		addr = "/tmp/redis.sock"
	} else if addr == "" {
		addr = ":6389"
	}
	l, e := net.Listen(srv.Proto, addr)
	if e != nil {
		return e
	}
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	defer srv.client.Exit()

	srv.client, _ = NewClient()
	go srv.client.Listen()

	for {
		netConn, err := l.Accept()
		conn := relayer.NewConn(netConn, parser)
		if err != nil {
			return err
		}
		go srv.ServeClient(conn)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
func (srv *Server) ServeClient(conn *relayer.Conn) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
	}()

	relayer.Debugf("New connection from %s\n", conn.RemoteAddr())
	started := time.Now()

	for {
		req, err := conn.Receive()
		if err != nil {
			relayer.Debugf("Finished session %s\n", time.Since(started))
			return err
		}
		conn.Write(protoOK)
		srv.client.Channel <- req
		/*
			"set",
			"hset",
			"del",
			"decr",
			"decrby",
			"expire",
			"expireat",
			"flushall",
			"flushdb",
			"geoadd",
			"hdel",
			"setbit",
			"setex",
			"setnx",
			"smove":

		*/
	}
}

func NewServer(c *Config) (*Server, error) {
	srv := &Server{
		Proto: c.proto,
	}

	if srv.Proto == "unix" {
		srv.Addr = c.host
	} else {
		srv.Addr = fmt.Sprintf("%s:%d", c.host, c.port)
	}

	return srv, nil
}
