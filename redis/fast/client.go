package redis

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	mode   int
	config lib.RelayerConfig
	ready  bool
	conn   *lib.Netbuf
	//	parser             *Parser
	requestChan        chan *Request // The relayer sends the requests via this channel
	database           int           // The current selected database
	queueChan          chan *Request // Requests sent to the Redis server, some pending of responses
	lastConnectFailure time.Time
	connectedAt        time.Time
}

// NewClient creates a new client that connect to a Redis server
func NewClient(c *lib.RelayerConfig) lib.RelayerClient {
	clt := &Client{}
	clt.Reload(c)

	clt.requestChan = make(chan *Request, requestBufferSize)
	clt.ready = true
	go clt.listen(clt.requestChan)
	lib.Debugf("Client %s for target %s ready", clt.config.Listen, clt.config.Host())

	return clt
}

func (clt *Client) Reload(c *lib.RelayerConfig) {
	clt.Lock()
	defer clt.Unlock()

	clt.config = *c
	clt.mode = c.Type()
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	if clt.lastConnectFailure.After(time.Now().Add(100 * time.Millisecond)) {
		// It failed too recently
		return false
	}

	conn, err := net.DialTimeout(clt.config.Scheme(), clt.config.Host(), connectTimeout)
	if err != nil {
		log.Println("Failed to connect to", clt.config.Host())
		clt.conn = nil
		clt.lastConnectFailure = time.Now()
		return false
	}
	clt.connectedAt = time.Now()
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.conn = lib.NewNetbuf(conn, serverReadTimeout, writeTimeout)
	clt.queueChan = make(chan *Request, requestBufferSize)

	go clt.netListener(clt.conn, clt.queueChan)

	return true
}

// Listen for clients' messages from the requestChan
func (clt *Client) listen(ch chan *Request) {
	defer log.Println("Finished Redis client")
	defer func() {
		clt.disconnect()
		clt.ready = false
	}()

	for {
		select {
		case req, more := <-ch:
			if !more { // exit from the program because the channel was closed
				clt.disconnect()
				return
			}
			_, err := clt.write(req)
			if err != nil {
				log.Println("Error writing:", err)
				sendAsyncResponse(req.responseChannel, protoKO)
				clt.disconnect()
			}
		case <-time.After(maxIdle):
			if clt.conn != nil {
				lib.Debugf("Closing by idle %s", clt.config.Host())
				clt.disconnect()
			}
		}
	}

}

// This goroutine listens for incoming answers from the Redis server
func (clt *Client) netListener(conn *lib.Netbuf, queue chan *Request) {
	parser := newParser(conn)
	for {
		req, more := <-queue
		if !more || req == nil {
			lib.Debugf("Net listener exiting")
			return
		}

		resp := &Request{}
		_, err := parser.read(resp, false)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error with server %s connection: %s", clt.config.Host(), err)
			}
			clt.disconnect()
			return
		}
		sendAsyncResponse(req.responseChannel, resp.buffer.Bytes())
	}
}

func (clt *Client) purgeRequests() {
	for {
		select {
		case req := <-clt.queueChan:
			sendAsyncResponse(req.responseChannel, protoKO)
		default:
			return
		}
	}
}

func (clt *Client) write(r *Request) (int64, error) {
	if clt.conn == nil && !clt.connect() {
		return 0, fmt.Errorf("Connection failed")
	}

	if bytes.Compare(r.command, selectCommand) == 0 {
		if clt.mode == lib.ModeSmart && clt.database == r.database { // There is no need to select again
			return 0, nil
		}
		clt.database = r.database
	} else {
		if clt.database != r.database {
			databaseChanger := Request{
				command:  selectCommand,
				buffer:   bytes.NewBuffer(getSelect(r.database)),
				database: r.database,
			}
			_, err := clt.write(&databaseChanger)
			if err != nil {
				log.Println("Error changing database", err)
				clt.disconnect()
				return 0, fmt.Errorf("Error in select")
			}
		}
	}
	c, err := r.buffer.WriteTo(clt.conn)

	if err != nil {
		lib.Debugf("Failed in write: %s", err)
		clt.disconnect()
	}

	r.buffer = nil // We don't need it anymore, don't use memory
	clt.queueChan <- r
	return c, err
}

func (clt *Client) disconnect() {
	clt.Lock()
	defer clt.Unlock()

	if clt.queueChan != nil {
		clt.purgeRequests()
		close(clt.queueChan)
		clt.queueChan = nil
	}

	if clt.conn != nil {
		lib.Debugf("Closing connection")
		clt.conn.Close()
		clt.conn = nil
	}

	clt.database = 0
}

func (clt *Client) Exit() {
	clt.Lock()
	defer clt.Unlock()

	clt.ready = false
	if clt.requestChan != nil {
		close(clt.requestChan)
		clt.requestChan = nil
	}
}

func (clt *Client) IsValid() bool {
	clt.Lock()
	defer clt.Unlock()

	return clt.ready
}

func (clt *Client) Send(req interface{}) (ok bool) {
	r := req.(*Request)
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("sendRequest: Recovered from error %s, channel length %d", e, len(clt.requestChan))
			ok = false
		}
	}()

	if clt.requestChan == nil {
		lib.Debugf("Nil channel in send request")
		return false
	}

	clt.requestChan <- r
	return true
}

func sendAsyncRequest(c chan *Request, r *Request) (ok bool) {
	defer func() {
		e := recover() // To avoid panic due to closed channels
		if e != nil {
			log.Printf("sendAsyncRequest: Recovered from error %s, channel length %d", e, len(c))
			ok = false
		}
	}()

	if c == nil {
		return
	}

	select {
	case c <- r:
		ok = true
	default:
		lib.Debugf("Error sending request, channel length %d", len(c))
		ok = false
	}
	return
}
