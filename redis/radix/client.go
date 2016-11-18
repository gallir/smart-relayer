package redis2

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
	"github.com/mediocregopher/radix.v2/redis"
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	config             lib.RelayerConfig
	mode               int
	ready              bool
	conn               net.Conn
	buf                *lib.NetBuffedReadWriter
	requestChan        chan *Request // The relayer sends the requests via this channel
	database           int           // The current selected database
	queueChan          chan *Request // Requests sent to the Redis server, some pending of responses
	lastConnectFailure time.Time
	connectedAt        time.Time
	failures           int
	pipelined          int
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

	if clt.ready {
		clt.flush(true)
	}
	clt.config = *c
	clt.mode = clt.config.Type()
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	if clt.failures > 10 {
		clt.ready = false
		return false
	}
	if clt.lastConnectFailure.After(time.Now().Add(100 * time.Millisecond)) {
		// It failed too recently
		return false
	}

	conn, err := net.DialTimeout(clt.config.Scheme(), clt.config.Host(), connectTimeout)
	if err != nil {
		lib.Debugf("Failed to connect to %s", clt.config.Host())
		clt.conn = nil
		clt.lastConnectFailure = time.Now()
		clt.failures++
		return false
	}
	clt.failures = 0
	clt.connectedAt = time.Now()
	lib.Debugf("Connected to %s", conn.RemoteAddr())
	clt.conn = conn
	clt.queueChan = make(chan *Request, requestBufferSize)
	clt.buf = lib.NewNetReadWriter(conn, time.Duration(clt.config.Timeout)*time.Second, 0)

	go clt.netListener(clt.buf, clt.queueChan)

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
				log.Println("Error writing:", clt.config.Host(), err)
				sendAsyncResponse(req.responseChannel, respKO)
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
func (clt *Client) netListener(buf io.ReadWriter, queue chan *Request) {
	reader := redis.NewRespReader(buf)
	for req := range queue {
		r := reader.Read()
		if req == nil {
			continue
		}

		if r.Err != nil && r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				log.Printf("Timeout (%d secs) at %s", clt.config.Timeout, clt.config.Host())
			} else {
				log.Printf("Error with server %s connection: %s", clt.config.Host(), r.Err)
			}
			sendAsyncResponse(req.responseChannel, respKO) // Send back and error immediately
			clt.disconnect()
			return
		}

		if clt.config.Compress || clt.config.Uncompress {
			r = compress.UResp(r)
		}
		sendAsyncResponse(req.responseChannel, r)
	}
	lib.Debugf("Net listener exiting")
}

func (clt *Client) purgeRequests() {
	for {
		select {
		case req := <-clt.queueChan:
			sendAsyncResponse(req.responseChannel, respKO)
		default:
			return
		}
	}
}

func (clt *Client) write(r *Request) (int64, error) {
	if clt.conn == nil && !clt.connect() {
		return 0, fmt.Errorf("Connection failed")
	}

	if r.command == selectCommand {
		if clt.mode == lib.ModeSmart && clt.database == r.database { // There is no need to select again
			return 0, nil
		}
		clt.database = r.database
	} else if clt.database != r.database {
		changer := redis.NewResp([]interface{}{
			selectCommand,
			fmt.Sprintf("%d", r.database),
		})
		_, err := changer.WriteTo(clt.buf)
		if err != nil {
			log.Println("Error changing database", err)
			clt.disconnect()
			return 0, fmt.Errorf("Error in select")
		}
		err = clt.flush(false)
		if err != nil {
			return 0, err
		}
		clt.database = r.database
		clt.queueChan <- nil
	}

	resp := r.resp
	if clt.config.Compress {
		items, ok := compress.Items(r.items)
		if ok {
			resp = redis.NewResp(items)
		}
	}

	c, err := resp.WriteTo(clt.buf)
	if err != nil {
		lib.Debugf("Failed in write: %s", err)
		clt.disconnect()
		return 0, err
	}

	err = clt.flush(r.responseChannel != nil) // Force flush if it's an sync command
	if err != nil {
		return 0, err
	}

	clt.queueChan <- r
	return c, err
}
func (clt *Client) flush(force bool) error {
	if force || clt.config.Pipeline == 0 && clt.pipelined >= clt.config.Pipeline || len(clt.requestChan) == 0 {
		err := clt.buf.Flush()
		clt.pipelined = 0
		if err != nil {
			lib.Debugf("Failed in flush: %s", err)
			clt.disconnect()
			return err
		}
	} else {
		// Don't flush yet, for pipelining
		clt.pipelined++
	}
	return nil
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
		clt.buf.Flush()
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

func (clt *Client) Send(req interface{}) (e error) {
	r := req.(*Request)
	defer func() {
		r := recover() // To avoid panic due to closed channels
		if r != nil {
			log.Printf("sendRequest: Recovered from error %s, channel length %d", e, len(clt.requestChan))
			e = r.(error)
		}
	}()

	if clt.requestChan == nil {
		lib.Debugf("Nil channel in send request")
		return errKO
	}

	if len(clt.requestChan) == requestBufferSize {
		log.Println("Redis overloaded", clt.config.Host())
		return errOverloaded
	}

	clt.requestChan <- r
	return nil
}
