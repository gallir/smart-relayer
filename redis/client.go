package redis

import (
	"bytes"
	"container/list"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/smart-relayer/tools"
)

func NewClient(s *Server) (*Client, error) {
	clt := &Client{
		server: s,
	}

	clt.channel = make(chan *Request, 4096)
	clt.queued = list.New()
	defer clt.Close()

	log.Printf("Client for target %s:%d ready", clt.server.config.Host, clt.server.config.Port)

	return clt, nil
}

func (clt *Client) checkIdle() {
	for {
		if clt.channel == nil {
			break
		}
		if clt.conn != nil && clt.conn.IsStale(connectionIdleMax) {
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
	tools.Debugf("Connected to %s", conn.RemoteAddr())
	clt.Lock()
	clt.conn = NewConn(conn)
	clt.conn.ReadTimeout = time.Second * 10
	clt.conn.WriteTimeout = time.Second * 10
	clt.listenerReady = make(chan bool, pipelineCommands)
	clt.Unlock()
	go clt.netListener()
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
			tools.Debugf("Closing by idle %s:%d", clt.server.config.Host, clt.server.config.Port)
			clt.Close()
			continue
		}

		_, err := clt.Write(request)
		if err != nil {
			log.Println("Error writing", err)
			clt.Close()
			continue
		}
	}
	log.Println("Finished Redis client", time.Since(started))
}

// This gotoutine listen for incoming answers from the Redis server
func (clt *Client) netListener() {
	ready := clt.listenerReady
	if clt.conn == nil || ready == nil {
		log.Println("Net listener not starting, nil values")
		return
	}
	for {
		if clt.conn == nil ||
			clt.listenerReady == nil || clt.listenerReady != ready {
			log.Println("Net listener exiting, connection has changed")
			return
		}
		doWork := <-ready
		if !doWork {
			tools.Debugf("Net listener exiting")
			return
		}
		conn := clt.conn // Safeguard, copy to avoid nil in receive()
		if conn == nil {
			log.Println("Net listener exiting, conn is nil")
			return
		}
		_, e := clt.receive(conn)
		if e != nil {
			log.Println("Error in listener", e)
		}
	}
}

func (clt *Client) receive(conn *Conn) (bool, error) {
	req := Request{}
	resp, err := conn.Parse(&req, false)
	if err != nil {
		return false, err
	}

	var q *list.Element
	clt.Lock()
	q = clt.queued.Front()
	if q != nil {
		clt.queued.Remove(q)
	}
	clt.Unlock()
	if q != nil {
		r := q.Value
		switch r := r.(type) {
		default:
			log.Printf("unexpected type %T\n", r) // %T prints whatever type t has
		case *Request:
			if r.Channel != nil {
				r.Channel <- resp
			}
		}
	}
	return true, nil
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

		if r.Command == selectCommand {
			if clt.server.Mode == modeSmart && clt.database == r.Database { // There is no need to select again
				return 0, nil
			}
			clt.database = r.Database
		} else {
			if clt.database != r.Database {
				databaseChanger := Request{
					Command:  selectCommand,
					Bytes:    getSelect(r.Database),
					Conn:     r.Conn,
					Database: r.Database,
				}
				_, err := clt.Write(&databaseChanger)
				if err != nil {
					log.Println("\tError changing database", err)
					clt.Close()
					return 0, fmt.Errorf("Error in select")
				}
			}
		}
		c, err := clt.conn.Write(r.Bytes)

		if err != nil {
			clt.Close()
			if neterr, ok := err.(net.Error); !ok || !neterr.Timeout() {
				log.Println("Failed in write:", err)
			}
		} else {
			clt.Lock()
			clt.queued.PushBack(r)
			clt.Unlock()
			clt.listenerReady <- true
			return c, err
		}
	}
	return 0, fmt.Errorf("Too many failed connections")
}

func (clt *Client) Close() {
	clt.Lock()
	if clt.listenerReady != nil {
		select {
		case clt.listenerReady <- false:
			tools.Debugf("Signalig listener to quit")
		default:
			tools.Debugf("Couldn't signal to listener")
		}
	}
	conn := clt.conn
	clt.conn = nil
	if conn != nil {
		conn.Close()
		clt.database = 0
		clt.queued.Init()
	}
	clt.Unlock()
}

func (clt *Client) Exit() {
	clt.Close()
	clt.channel <- &protoClientExit
}
