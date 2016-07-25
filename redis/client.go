package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// NewClient creates a new client that connect to a Redis server
func NewClient(s *Server) (*Client, error) {
	clt := &Client{
		server: s,
	}

	clt.channel = make(chan *Request, requestBufferSize)
	lib.Debugf("Client %d for target %s ready", clt.server.config.Listen, clt.server.config.Host())

	return clt, nil
}

func (clt *Client) checkIdle() {
	lib.Debugf("checkIdle() started")
	for {
		if clt.channel == nil {
			break
		}
		if clt.conn != nil && clt.conn.isStale(connectionIdleMax) {
			clt.channel <- &protoClientCloseConnection
		}
		time.Sleep(connectionIdleMax)
	}
	lib.Debugf("checkIdle() exiting")
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	conn, err := net.DialTimeout("tcp", clt.server.config.Host(), connectTimeout)
	if err != nil {
		log.Println("Failed to connect to", clt.server.config.Host())
		clt.conn = nil
		return false
	}
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.conn = NewConn(conn, serverReadTimeout)
	clt.createRequestChannel()
	go clt.netListener()
	return true
}

func (clt *Client) createRequestChannel() {
	clt.sentRequests = make(chan *Request, requestBufferSize)
}

// Listen for clients' messages from the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	currentConfig := clt.server.config
	go clt.checkIdle()

	for {
		request := <-clt.channel
		if bytes.Compare(request.Command, exitCommand) == 0 {
			break
		}
		if bytes.Compare(request.Command, closeConnectionCommand) == 0 {
			lib.Debugf("Closing by idle %s", clt.server.config.Host())
			clt.close()
			continue
		}
		if bytes.Compare(request.Command, reloadCommand) == 0 {
			if currentConfig.Host() != clt.server.config.Host() {
				lib.Debugf("Closing by reload %s -> %s", currentConfig.Host(), clt.server.config.Host())
				clt.close()
			}
			currentConfig = clt.server.config
			continue
		}

		_, err := clt.write(request)
		if err != nil {
			log.Println("Error writing:", err)

			// Respond with KO to the client
			if request.Channel != nil {
				select {
				case request.Channel <- protoKO:
				default:
				}
			}
			clt.close()
			continue
		}
	}
	log.Println("Finished Redis client", time.Since(started))
}

// This goroutine listens for incoming answers from the Redis server
func (clt *Client) netListener() {
	sent := clt.sentRequests
	conn := clt.conn
	if conn == nil || sent == nil {
		log.Println("Net listener not starting, nil values")
		return
	}
	for {
		if conn != clt.conn || sent != clt.sentRequests {
			log.Println("Net listener exiting, connection has changed")
			return
		}
		req := <-sent
		if req == nil {
			lib.Debugf("Net listener exiting")
			return
		}

		resp := &Request{}
		_, err := conn.parse(resp, false)
		if err != nil {
			log.Println("Error in listener", err)
			clt.close()
			return
		}

		if req.Channel != nil {
			select {
			case req.Channel <- resp.Buffer.Bytes():
			default:
				log.Println("Error sending data back to client")
			}
		}
	}
}

func (clt *Client) write(r *Request) (int, error) {
	if clt.conn == nil && !clt.connect() {
		return 0, fmt.Errorf("Connection failed")
	}

	if bytes.Compare(r.Command, selectCommand) == 0 {
		if clt.server.Mode == modeSmart && clt.database == r.Database { // There is no need to select again
			return 0, nil
		}
		clt.database = r.Database
	} else {
		if clt.database != r.Database {
			databaseChanger := Request{
				Command:  selectCommand,
				Buffer:   bytes.NewBuffer(getSelect(r.Database)),
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
	bytes := r.Buffer.Bytes()
	c, err := clt.conn.Write(bytes)
	lib.Debugf("Command: %s, Wrote %d bytes [%s]", r.Command, len(bytes), bytes)

	if err != nil {
		clt.close()
		if neterr, ok := err.(net.Error); !ok || !neterr.Timeout() {
			log.Println("Failed in write:", err)
			return 0, err
		}
	}

	clt.sentRequests <- r
	return c, err
}

func (clt *Client) close() {
	clt.Lock()
	defer clt.Unlock()

	if clt.sentRequests != nil {
		select {
		case clt.sentRequests <- nil:
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
	defer func() {
		clt.sentRequests <- nil    // Force netListener to quitd
		clt.createRequestChannel() // Restart with a fresh channel

	}()

	for {
		select {
		case r := <-clt.sentRequests:
			if r != nil && r.Channel != nil {
				select {
				case r.Channel <- protoKO:
				default:
				}
			}
		default:
			return
		}
	}

}

func (clt *Client) Exit() {
	clt.close()
	clt.channel <- &protoClientExit
}
