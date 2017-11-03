package redis2

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	config             lib.RelayerConfig
	mode               int
	ready              int32
	connected          int32
	conn               net.Conn
	buf                *lib.NetBuffedReadWriter
	requestChan        chan *lib.Request // The relayer sends the requests via this channel
	database           int               // The current selected database
	queueChan          chan *lib.Request // Requests sent to the Redis server, some pending of responses
	lastConnectFailure time.Time
	connectedAt        time.Time
	failures           int64
	pipelined          int
}

// NewClient creates a new client that connect to a Redis server
func NewClient(c *lib.RelayerConfig) *Client {
	clt := &Client{}
	clt.Reload(c)

	clt.requestChan = make(chan *lib.Request, requestBufferSize)
	clt.setReady(true)
	go clt.listen(clt.requestChan)
	lib.Debugf("Client %s for target %s ready", clt.config.Listen, clt.config.Host())

	return clt
}

func (clt *Client) Reload(c *lib.RelayerConfig) {
	clt.Lock()
	defer clt.Unlock()

	if clt.isReady() {
		clt.flush(true)
	}
	clt.config = *c
	clt.mode = clt.config.Type()
}

func (clt *Client) setReady(s bool) {
	if s {
		atomic.StoreInt32(&clt.ready, 1)
	} else {
		atomic.StoreInt32(&clt.ready, 0)
	}
}

func (clt *Client) isReady() bool {
	if atomic.LoadInt32(&clt.ready) == 1 {
		return true
	}
	return false
}

func (clt *Client) setConnected(s bool) {
	if s {
		atomic.StoreInt32(&clt.connected, 1)
	} else {
		atomic.StoreInt32(&clt.connected, 0)
	}
}

func (clt *Client) isConnected() bool {
	if atomic.LoadInt32(&clt.connected) == 1 {
		return true
	}
	return false
}

func (clt *Client) connect() bool {
	clt.Lock()
	defer clt.Unlock()

	if clt.failures > 10 {
		// The pool manager will see we are invalid and kill us
		clt.setReady(false)
		return false
	}
	if clt.lastConnectFailure.Add(200 * time.Millisecond).After(time.Now()) {
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
	clt.queueChan = make(chan *lib.Request, requestBufferSize)
	clt.buf = lib.NewNetReadWriter(conn, time.Duration(clt.config.Timeout)*time.Second, 0)

	go clt.netListener(clt.buf, clt.queueChan)
	clt.setConnected(true)

	return true
}

// Listen for clients' messages from the requestChan
func (clt *Client) listen(ch chan *lib.Request) {
	defer lib.Debugf("Finished Redis client")
	defer func() {
		clt.setReady(false)
	}()

	timer := time.NewTimer(maxIdle)
	defer func() {
		// Clean timer gracefully
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	clt.connect()
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
				sendAsyncResponse(req.ResponseChannel, respKO)
				clt.disconnect()
			}
			timer.Stop()
			timer.Reset(maxIdle)
		case <-timer.C:
			if clt.conn != nil {
				lib.Debugf("Closing by idle %s", clt.config.Host())
				clt.disconnect()
			}
		}
	}
}

// This goroutine listens for incoming answers from the Redis server
func (clt *Client) netListener(buf io.ReadWriter, queue chan *lib.Request) {
	lib.Debugf("Net listener started")
	defer clt.close() // Will force to close net connection

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
			sendAsyncResponse(req.ResponseChannel, respKO) // Send back and error immediately
			clt.disconnect()
			return
		}

		if clt.config.Compress || clt.config.Uncompress {
			r = compress.UResp(r)
		}
		sendAsyncResponse(req.ResponseChannel, r)
	}
	lib.Debugf("Net listener exiting")
}

func (clt *Client) purgeRequests() {
	for {
		select {
		case req := <-clt.queueChan:
			sendAsyncResponse(req.ResponseChannel, respKO)
		default:
			return
		}
	}
}

func (clt *Client) write(r *lib.Request) (int64, error) {
	if !clt.isConnected() && !clt.connect() {
		return 0, fmt.Errorf("Connection failed")
	}

	if r.Command == selectCommand {
		if clt.mode == lib.ModeSmart && clt.database == r.Database { // There is no need to select again
			return 0, nil
		}
		clt.database = r.Database
	} else if clt.database != r.Database {
		err := clt.changeDB(r.Database)
		if err != nil {
			clt.disconnect()
			return 0, err
		}
		clt.queueChan <- nil
	}

	resp := r.Resp
	if clt.config.Compress {
		items, ok := compress.Items(r.Items)
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

	err = clt.flush(r.ResponseChannel != nil) // Force flush if it's an sync command
	if err != nil {
		return 0, err
	}

	clt.queueChan <- r
	return c, err
}

func (clt *Client) changeDB(db int) (err error) {
	changer := redis.NewResp([]interface{}{
		selectCommand,
		fmt.Sprintf("%d", db),
	})
	_, err = changer.WriteTo(clt.buf)
	if err != nil {
		log.Println("Error changing database", err)
		return
	}
	err = clt.flush(false)
	if err != nil {
		return
	}
	clt.database = db
	return

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

// Start disconnection
func (clt *Client) disconnect() {
	clt.Lock()
	defer clt.Unlock()

	clt.setConnected(false)
	if clt.queueChan != nil {
		clt.purgeRequests()
		close(clt.queueChan)
		clt.queueChan = nil
	}
	clt.database = 0
}

// Close connection
func (clt *Client) close() {
	if clt.isConnected() {
		clt.disconnect() // Double check
	}
	clt.Lock()
	defer clt.Unlock()

	if clt.conn != nil {
		clt.buf.Flush()
		lib.Debugf("Closing connection")
		clt.conn.Close()
		clt.conn = nil
	}
}

func (clt *Client) Exit() {
	clt.Lock()
	defer clt.Unlock()

	clt.setReady(false)
	if clt.requestChan != nil {
		// Closing the request channel triggers all disconnections
		close(clt.requestChan)
		clt.requestChan = nil
	}
}

// IsValid is used for pool management, it ignores disconnect clients
func (clt *Client) IsValid() bool {
	return clt.isReady() && clt.isConnected()
}

func (clt *Client) Send(req interface{}) (e error) {
	r := req.(*lib.Request)
	defer func() {
		r := recover() // To avoid panic due to closed channels
		if r != nil {
			log.Printf("sendRequest: Recovered from error %s, channel length %d", e, len(clt.requestChan))
			e = r.(error)
		}
	}()

	if clt.requestChan == nil {
		lib.Debugf("Nil channel in send request %s", clt.config.Host())
		return errKO
	}

	if !clt.isReady() {
		lib.Debugf("Client not ready %s", clt.config.Host())
		return errKO
	}

	if f := clt.failures; f > 0 {
		if clt.lastConnectFailure.Add(connectTimeout).After(time.Now()) {
			lib.Debugf("Client is failing to connect %s", clt.config.Host())
			return errKO
		}
	}

	if len(clt.requestChan) == requestBufferSize {
		log.Println("Redis overloaded", clt.config.Host())
		return errOverloaded
	}

	clt.requestChan <- r
	return nil
}
