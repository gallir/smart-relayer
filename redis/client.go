package redis

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	server        lib.Relayer
	conn          *IO
	requestsChan  chan *Request // The server sends the requests via this channel
	database      int           // The current selected database
	responsesChan chan *Request // Requests sent to the server
}

// NewClient creates a new client that connect to a Redis server
func NewClient(s lib.Relayer) (*Client, error) {
	clt := &Client{
		server: s,
	}

	clt.requestsChan = make(chan *Request, requestBufferSize)
	lib.Debugf("Client %s for target %s ready", clt.server.Config().Listen, clt.server.Config().Host())

	return clt, nil
}

func (clt *Client) checkIdle() {
	lib.Debugf("checkIdle() started")
	for {
		if clt.requestsChan == nil {
			break
		}
		if clt.conn != nil && clt.conn.IsStale(connectionIdleMax) {
			clt.requestsChan <- &protoClientCloseConnection
		}
		time.Sleep(connectionIdleMax)
	}
	lib.Debugf("checkIdle() exiting")
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	conn, err := net.DialTimeout(clt.server.Config().Scheme(), clt.server.Config().Host(), connectTimeout)
	if err != nil {
		log.Println("Failed to connect to", clt.server.Config().Host())
		clt.conn = nil
		return false
	}
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.conn = NewConn(conn, serverReadTimeout, writeTimeout)
	clt.createRequestChannel()
	go clt.netListener()
	return true
}

func (clt *Client) createRequestChannel() {
	clt.responsesChan = make(chan *Request, requestBufferSize)
}

// Listen for clients' messages from the internal channel
func (clt *Client) Listen() {
	started := time.Now()
	currentConfig := clt.server.Config()
	go clt.checkIdle()

	for {
		request := <-clt.requestsChan
		if bytes.Compare(request.Command, exitCommand) == 0 {
			break
		}
		if bytes.Compare(request.Command, closeConnectionCommand) == 0 {
			lib.Debugf("Closing by idle %s", clt.server.Config().Host())
			clt.close()
			continue
		}
		if bytes.Compare(request.Command, reloadCommand) == 0 {
			if currentConfig.Host() != clt.server.Config().Host() {
				lib.Debugf("Closing by reload %s -> %s", currentConfig.Host(), clt.server.Config().Host())
				clt.close()
			}
			currentConfig = clt.server.Config()
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
	sent := clt.responsesChan
	conn := clt.conn
	if conn == nil || sent == nil {
		log.Println("Net listener not starting, nil values")
		return
	}
	for {
		if conn != clt.conn || sent != clt.responsesChan {
			log.Println("Net listener exiting, connection has changed")
			return
		}
		req := <-sent
		if req == nil {
			lib.Debugf("Net listener exiting")
			return
		}

		resp := &Request{}
		_, err := conn.Read(resp, false)
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
		if clt.server.Mode() == modeSmart && clt.database == r.Database { // There is no need to select again
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

	if err != nil {
		clt.close()
		if neterr, ok := err.(net.Error); !ok || !neterr.Timeout() {
			log.Println("Failed in write:", err)
			return 0, err
		}
	}

	clt.responsesChan <- r
	return c, err
}

func (clt *Client) close() {
	clt.Lock()
	defer clt.Unlock()

	if clt.responsesChan != nil {
		select {
		case clt.responsesChan <- nil:
			lib.Debugf("Signalig listener to quit")
		default:
			lib.Debugf("Couldn't signal to listener")
		}
	}
	conn := clt.conn
	clt.conn = nil
	if conn != nil {
		lib.Debugf("Closing connection")
		conn.Close()
		clt.database = 0
		clt.purgePending()
	}
}

// The connection is being closed or reopened, send error to clients waiting for response
func (clt *Client) purgePending() {
	defer func() {
		clt.responsesChan <- nil   // Force netListener to quitd
		clt.createRequestChannel() // Restart with a fresh channel

	}()

	for {
		select {
		case r := <-clt.responsesChan:
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
	clt.requestsChan <- &protoClientExit
}
