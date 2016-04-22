// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

package relayer

import (
	"fmt"
	"io"
	"log"
	"net"
)

type Request struct {
	Name  string
	Bytes []byte
	Host  string
	Body  io.ReadCloser
}

type Server struct {
	Proto string
	Addr  string // TCP address to listen on, ":6389" if empty
}

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

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	clt, _ := NewClient()
	//clt.Connect()
	go clt.Listen()
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(rw, clt)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func (srv *Server) ServeClient(conn net.Conn, clt *Client) (err error) {
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
		conn.Close()
	}()

	var clientAddr string

	switch co := conn.(type) {
	case *net.UnixConn:
		f, err := conn.(*net.UnixConn).File()
		if err != nil {
			return err
		}
		clientAddr = f.Name()
	default:
		clientAddr = co.RemoteAddr().String()
	}
	fmt.Println("New connection from", clientAddr)

	for {
		request, err := parseRequest(conn)
		if err != nil {
			return err
		}
		var reply string
		relay := false
		switch request.Name {
		case
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
			reply = "+OK\r\n"
			relay = true
		case "ping":
			reply = "+PONG\r\n"
		default:
			reply = "-Error: Command not accepted\r\n"
			log.Printf("Error: command not accepted %q\n", request.Name)

		}

		request.Host = clientAddr

		if _, err = conn.Write([]byte(reply)); err != nil {
			return err
		}
		if relay {
			clt.Channel <- request
		}
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
