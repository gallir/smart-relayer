package redis

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/smart-relayer/tools"
)

// Serve accepts incoming connections on the Listener l
func (srv *Server) Serve() error {
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", srv.config.Listen))
	if e != nil {
		log.Println("Error listening to port", fmt.Sprintf(":%d", srv.config.Listen), e)
		return e
	}

	log.Printf("Starting redis server at port %d for target %s:%d", srv.config.Listen, srv.config.Host, srv.config.Port)
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

	tools.Debugf("New connection from %s", conn.RemoteAddr())
	responseCh := make(chan []byte, 1)
	started := time.Now()

	for {
		req := Request{Conn: conn}
		_, err = conn.Parse(&req, true)
		if err != nil {
			break
		}

		// QUIT received form client
		if req.Command == quitCommand {
			conn.Write(protoOK)
			break
		}

		req.Database = conn.Database

		// Smart mode, answer immediately and forget
		if srv.Mode == modeSmart {
			fastResponse, ok := commands[req.Command]
			if ok {
				conn.Write(fastResponse)
				srv.client.channel <- &req
				continue
			}
		}

		// Synchronized mode
		req.Channel = responseCh
		srv.client.channel <- &req
		response := <-responseCh
		conn.Write(response)
	}
	tools.Debugf("Finished session %s", time.Since(started))
	return err
}
