package redis

import (
	"bytes"
	"container/list"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

func NewClient(s *Server) (*Client, error) {
	clt := &Client{
		server: s,
	}

	clt.channel = make(chan *Request, requestBufferSize)
	clt.queued = list.New()

	lib.Debugf("Client %d for target %s ready", clt.server.config.Listen, clt.server.config.Host())

	return clt, nil
}

func (clt *Client) checkIdle() {
	for {
		if clt.channel == nil {
			break
		}
		if clt.conn != nil && clt.conn.isStale(connectionIdleMax) {
			clt.channel <- &protoClientCloseConnection
		}
		time.Sleep(connectionIdleMax)
	}
}

func (clt *Client) connect() bool {
	conn, err := net.DialTimeout("tcp", clt.server.config.Host(), connectTimeout)
	if err != nil {
		log.Println("Failed to connect to", clt.server.config.Host())
		clt.conn = nil
		return false
	}
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.Lock()
	clt.conn = NewConn(conn)
	clt.listenerReady = make(chan bool, requestBufferSize)
	clt.purgePending()
	clt.Unlock()
	go clt.netListener()
	return true
}

// Listen for server messages in the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	currentConfig := clt.server.config
	go clt.checkIdle()

	for {
		request := <-clt.channel
		if bytes.Compare(request.Bytes, protoClientExit.Bytes) == 0 {
			break
		}
		if bytes.Compare(request.Bytes, protoClientCloseConnection.Bytes) == 0 {
			lib.Debugf("Closing by idle %s", clt.server.config.Host())
			clt.close()
			continue
		}
		if bytes.Compare(request.Bytes, protoClientReload.Bytes) == 0 {
			if currentConfig.Host() != clt.server.config.Host() {
				lib.Debugf("Closing by reload %s -> %s", currentConfig.Host(), clt.server.config.Host())
				clt.close()
			}
			currentConfig = clt.server.config
			continue
		}

		_, err := clt.write(request)
		if err != nil {
			log.Println("Error writing", err)
			clt.close()
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
			lib.Debugf("Net listener exiting")
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
			clt.close()
		}
	}
}

func (clt *Client) receive(conn *Conn) (bool, error) {
	req := Request{}
	resp, err := conn.parse(&req, false)
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
				select {
				case r.Channel <- resp:
				default:
					log.Println("Error sending data back to client")
				}
			}
		}
	}
	return true, nil
}

func (clt *Client) write(r *Request) (int, error) {
	for i := 0; i < connectionRetries; i++ {
		if clt.conn == nil {
			if i > 0 {
				time.Sleep(time.Duration(i*2) * time.Second)
			}
			if !clt.connect() {
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
				_, err := clt.write(&databaseChanger)
				if err != nil {
					log.Println("Error changing database", err)
					clt.close()
					return 0, fmt.Errorf("Error in select")
				}
			}
		}
		c, err := clt.conn.Write(r.Bytes)

		if err != nil {
			clt.close()
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

func (clt *Client) close() {
	clt.Lock()
	defer clt.Unlock()

	if clt.listenerReady != nil {
		select {
		case clt.listenerReady <- false:
			lib.Debugf("Signalig listener to quit")
		default:
			lib.Debugf("Couldn't signal to listener")
		}
	}
	conn := clt.conn
	clt.conn = nil
	if conn != nil {
		lib.Debugf("Closing connection")
		conn.close()
		clt.database = 0
		clt.purgePending()
	}
}

// The connection is being closed or reopened, send error to clients waiting for response
func (clt *Client) purgePending() {
	freed := 0
	for q := clt.queued.Front(); q != nil; q = q.Next() {
		freed++
		r := q.Value
		switch r := r.(type) {
		case *Request:
			if r.Channel != nil {
				select {
				case r.Channel <- protoKO:
				default:
				}
			}
		default:
			log.Printf("unexpected type %T\n", r) // %T prints whatever type t has
		}
	}
	if freed > 0 {
		clt.queued.Init()
	}
}

func (clt *Client) Exit() {
	clt.close()
	clt.channel <- &protoClientExit
}
