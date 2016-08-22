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
	server             *Server
	parser             *Parser
	requestChan        chan *Request // The relayer sends the requests via this channel
	database           int           // The current selected database
	queueChan          chan *Request // Requests sent to the Redis server, some pending of responses
	lastConnectFailure time.Time
}

// NewClient creates a new client that connect to a Redis server
func newClient(s *Server) *Client {
	clt := &Client{
		server: s,
	}

	clt.requestChan = make(chan *Request, requestBufferSize)
	lib.Debugf("Client %s for target %s ready", clt.server.Config().Listen, clt.server.Config().Host())
	go clt.listen()

	return clt
}

func (clt *Client) checkIdle() {
	lib.Debugf("checkIdle started")
	defer lib.Debugf("checkIdle exiting")

	for {
		clt.Lock()
		parser := clt.parser
		channel := clt.requestChan
		clt.Unlock()

		if channel == nil {
			return
		}

		if parser != nil && parser.isStale(connectionIdleMax) {
			sendAsyncRequest(channel, &protoClientCloseConnection)
		}
		time.Sleep(connectionIdleMax)
	}
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	if clt.lastConnectFailure.After(time.Now().Add(-1 * time.Second)) {
		// It failed too recently
		return false
	}

	conn, err := net.DialTimeout(clt.server.Config().Scheme(), clt.server.Config().Host(), connectTimeout)
	if err != nil {
		log.Println("Failed to connect to", clt.server.Config().Host())
		clt.parser = nil
		clt.lastConnectFailure = time.Now()
		return false
	}
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.parser = newParser(conn, serverReadTimeout, writeTimeout)
	clt.createQueueChannel()
	go clt.netListener()
	return true
}

func (clt *Client) createQueueChannel() {
	clt.queueChan = make(chan *Request, requestBufferSize)
}

// Listen for clients' messages from the requestChan
func (clt *Client) listen() {
	defer log.Println("Finished Redis client")
	go clt.checkIdle()

	for {
		request, more := <-clt.requestChan
		if !more { // exit from the program because the channel was closed
			clt.purgeRequests()
			clt.requestChan = nil
			clt.close()
			return
		}
		if bytes.Compare(request.command, closeConnectionCommand) == 0 {
			lib.Debugf("Closing by idle %s", clt.server.Config().Host())
			clt.close()
			continue
		}

		_, err := clt.write(request)
		if err != nil {
			log.Println("Error writing:", err)
			sendAsyncResponse(request.responseChannel, protoKO)
			clt.close()
			continue
		}
	}

}

// This goroutine listens for incoming answers from the Redis server
func (clt *Client) netListener() {
	sent := clt.queueChan
	conn := clt.parser
	if conn == nil || sent == nil {
		log.Println("Net listener not starting, nil values")
		return
	}
	for {
		if conn != clt.parser || sent != clt.queueChan {
			log.Println("Net listener exiting, connection has changed")
			return
		}
		req, more := <-sent
		if !more || req == nil {
			clt.purgeRequests()
			lib.Debugf("Net listener exiting")
			return
		}

		resp := &Request{}
		_, err := conn.read(resp, false)
		if err != nil {
			log.Println("Error in listener", err)
			clt.purgeRequests()
			clt.close()
			return
		}
		sendAsyncResponse(req.responseChannel, resp.buffer.Bytes())
	}
}

func (clt *Client) purgeRequests() {
	clt.Lock()
	defer clt.Unlock()

	if clt.queueChan == nil {
		return
	}

	for {
		select {
		case req := <-clt.queueChan:
			sendAsyncResponse(req.responseChannel, protoKO)
		default:
			return
		}
	}
}

func (clt *Client) write(r *Request) (int, error) {
	if clt.parser == nil && !clt.connect() {
		return 0, fmt.Errorf("Connection failed")
	}

	if bytes.Compare(r.command, selectCommand) == 0 {
		if clt.server.mode == modeSmart && clt.database == r.database { // There is no need to select again
			return 0, nil
		}
		clt.database = r.database
	} else {
		if clt.database != r.database {
			databaseChanger := Request{
				command:  selectCommand,
				buffer:   bytes.NewBuffer(getSelect(r.database)),
				parser:   r.parser,
				database: r.database,
			}
			_, err := clt.write(&databaseChanger)
			if err != nil {
				log.Println("Error changing database", err)
				clt.close()
				return 0, fmt.Errorf("Error in select")
			}
		}
	}
	bytes := r.buffer.Bytes()
	c, err := clt.parser.Write(bytes)

	if err != nil {
		clt.close()
		if neterr, ok := err.(net.Error); !ok || !neterr.Timeout() {
			log.Println("Failed in write:", err)
			return 0, err
		}
	}

	clt.queueChan <- r
	return c, err
}

func (clt *Client) close() {
	clt.Lock()
	defer clt.Unlock()

	if clt.queueChan != nil {
		close(clt.queueChan)
		clt.queueChan = nil
	}
	conn := clt.parser
	clt.parser = nil
	if conn != nil {
		lib.Debugf("Closing connection")
		conn.close()
		clt.database = 0
	}
}

func (clt *Client) exit() {
	clt.Lock()
	defer clt.Unlock()

	if clt.requestChan != nil {
		close(clt.requestChan)
	}
}
