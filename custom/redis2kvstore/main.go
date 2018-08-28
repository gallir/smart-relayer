package redis2kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
	"github.com/golang/snappy"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	done     chan bool
	exiting  bool
	listener net.Listener

	client *http.Client

	lastConnection time.Time
	lastError      time.Time
	running        int64
}

const (
	strContentType      = "application/octet-stream"
	maxConnections      = 2
	requestBufferSize   = 1024 * 10
	maxConnectionsTries = 3
	connectionRetry     = 5 * time.Second
	errorsFrame         = 10 * time.Second
	maxErrors           = 10 // Limit of errors to restart the connection
	connectTimeout      = 15 * time.Second
	defaultExpire       = 8 * 60 * 60 // 8h
	retryTime           = 100 * time.Millisecond
	waitingForExit      = 2 * time.Second
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errOverloaded  = errors.New("http server overloaded")
	errNotFound    = errors.New("Not found")
	respOK         = redis.NewRespSimple("OK")
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	commands       map[string]*redis.Resp

	pool = bytebufferpool.Pool{}
)

func init() {
	commands = map[string]*redis.Resp{
		"PING":    respOK,
		"HMSET":   respOK,
		"EXPIRE":  respOK,
		"HGET":    respOK,
		"HGETALL": respOK,
	}
}

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DisableKeepAlives:  false,
				MaxIdleConns:       1024,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
					DualStack: true,
				}).DialContext,
			},
		},
	}
	srv.Reload(&c)

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c

	if srv.config.MaxConnections <= 0 {
		srv.config.MaxConnections = maxConnections
	}

	return nil
}

// Start accepts incoming connections on the Listener
func (srv *Server) Start() (e error) {
	srv.Lock()
	defer srv.Unlock()

	srv.listener, e = lib.NewListener(srv.config)
	if e != nil {
		return e
	}

	// Serve clients
	go func(l net.Listener) {
		defer srv.listener.Close()
		for {
			netConn, e := l.Accept()
			if e != nil {
				if netErr, ok := e.(net.Error); ok && netErr.Timeout() {
					// Paranoid, ignore timeout errors
					log.Println("redis2kvstore ERROR: timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if srv.exiting {
					log.Println("redis2kvstore: exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("redis2kvstore ERROR: emergency error in local listener", srv.config.ListenHost(), e)
				return
			}
			go srv.handleConnection(netConn)
		}
	}(srv.listener)

	return
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	srv.exiting = true

	if srv.listener != nil {
		srv.listener.Close()
	}

	retry := 0
	for retry < 10 {
		n := atomic.LoadInt64(&srv.running)
		if n == 0 {
			break
		}
		log.Printf("redis2kvstore Waiting that %d process are still running", n)
		time.Sleep(waitingForExit)
		retry++
	}

	if n := atomic.LoadInt64(&srv.running); n > 0 {
		log.Printf("redis2kvstore ERROR: %d messages lost", n)
	}

	// finishing the server
	srv.done <- true
}

func (srv *Server) handleConnection(netCon net.Conn) {
	defer netCon.Close()

	reader := redis.NewRespReader(netCon)

	pending := getPending()
	defer func() {
		if len(pending) > 0 {
			for key, p := range pending {
				if !p.Sent {
					srv.send(key, defaultExpire, p)
				}
				putPoolHMSet(p)
				delete(pending, key)
			}
		}
		putPending(pending)
	}()

	for {
		r := reader.Read()

		if r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				// Paranoid, don't close it just log it
				log.Println("redis2kvstore: Local client listen timeout at", srv.config.Listen)
				continue
			}
			// Connection was closed
			return
		}

		req := lib.NewRequest(r, &srv.config)
		if req == nil {
			respBadCommand.WriteTo(netCon)
			continue
		}

		validCommand, ok := commands[req.Command]
		if !ok {
			respBadCommand.WriteTo(netCon)
			continue
		}

		switch req.Command {
		case "HMSET":
			if len(req.Items) < 2 {
				respKO.WriteTo(netCon)
				continue
			}
			validCommand.WriteTo(netCon)

			key, _ := req.Items[1].Str()
			if _, ok := pending[key]; !ok {
				// Increase the number of running process before create a new hmset
				atomic.AddInt64(&srv.running, 1)
				pending[key] = getPoolHMSet()
			}
			pending[key].processItems(req.Items[2:])
		case "EXPIRE":
			if len(req.Items) != 3 {
				respKO.WriteTo(netCon)
				continue
			}

			key, _ := req.Items[1].Str()
			p, ok := pending[key]
			if !ok || key == "" {
				log.Printf("redis2kvstore ERROR: Invalid key %s", key)
				respBadCommand.WriteTo(netCon)
				continue
			}

			expire, _ := req.Items[2].Int()
			p.Sent = true
			go func(srv *Server, key string, expire int, p *Hmset) {
				if expire == 0 {
					expire = defaultExpire
				}
				srv.send(key, expire, p)
			}(srv, key, expire, p)
			validCommand.WriteTo(netCon)
		case "HGETALL":
			if len(req.Items) != 2 {
				respKO.WriteTo(netCon)
				continue
			}
			key, _ := req.Items[1].Str()

			// Return information that is in memory
			if m, ok := pending[key]; ok {
				if r, err := m.getAllAsRedis(); err == nil {
					r.WriteTo(netCon)
					continue
				}
			}

			// If is not in memory we go to the cluster
			items, err := srv.getHGetAll(key)
			if err != nil {
				redis.NewResp(err).WriteTo(netCon)
				continue
			}
			items.WriteTo(netCon)
			items.ReleaseBuffers()
		case "HGET":
			if len(req.Items) != 3 {
				respKO.WriteTo(netCon)
				continue
			}
			key, _ := req.Items[1].Str()
			item, _ := req.Items[2].Str()

			// Return information that is in memory
			if m, ok := pending[key]; ok {
				if r, err := m.getOneAsRedis(item); err == nil {
					r.WriteTo(netCon)
					continue
				}
			}

			// If is not in memory we go to the cluster
			g, err := srv.getHGet(key, item)
			if err != nil {
				if err == errNotFound {
					redis.NewResp(nil).WriteTo(netCon)
					continue
				}
				redis.NewResp(err).WriteTo(netCon)
				continue
			}
			g.WriteTo(netCon)
			g.ReleaseBuffers()
		}

		for _, i := range req.Items {
			i.ReleaseBuffers()
		}

	}
}

func (srv *Server) send(key string, expire int, p *Hmset) {
	w := pool.Get()
	defer pool.Put(w)
	// Reduce the number of running sets
	defer atomic.AddInt64(&srv.running, -1)

	url := fmt.Sprintf("%s/%s/%ds", srv.config.URL, key, expire)
	b, _ := p.Marshal()

	if srv.config.Gzip > 0 {
		gzWriter := lib.GetGzipWriterLevel(w, srv.config.Gzip)
		gzWriter.Write(b)
		gzWriter.Close()
		lib.PutGzipWriter(gzWriter)
	} else if srv.config.Compress {
		w.Write(compress.Bytes(b))
	} else {
		w.Write(b)
	}
	if w.Len() <= 0 {
		log.Printf("redis2kvstore ERROR empty body: %s", url)
		return
	}

	for i := 0; i < maxConnectionsTries; i++ {
		resp, err := srv.client.Post(url, strContentType, bytes.NewReader(w.B))
		if err == nil && resp.StatusCode == 200 {
			// Success
			defer resp.Body.Close()
			return
		}

		if err != nil {
			log.Printf("redis2kvstore ERROR connect: %s %s", url, err)
		}

		if resp.StatusCode > 0 {
			log.Printf("redis2kvstore ERROR post: [%d] %s %s", resp.StatusCode, url, err)
		}

		time.Sleep(retryTime)
	}
}

// get will get via http the content of the key, this content will be
// an slice of bytes. The function will uncompress it based on the configuration
// IMPORTANT: this functions use a sync.Pool for the &Hmet{}. You should send
// the struct back to the pull after use it.
func (srv *Server) get(key string) (*Hmset, error) {
	buf := pool.Get()
	defer pool.Put(buf)

	url := fmt.Sprintf("%s/get/%s", srv.config.URL, key)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("redis2kvstore ERROR connect: %s %s", url, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Error: %s %s", url, resp.Status)
	}

	lib.Debugf("redis2kvstore: get %s", url)

	if srv.config.Gzip > 0 || srv.config.Gunzip {
		gzReader, err := lib.GetGzipReader(resp.Body)
		if err != nil {
			return nil, err
		}
		buf.ReadFrom(gzReader)
		gzReader.Close()
		lib.PutGzipReader(gzReader)
	} else if srv.config.Compress {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, nil
		}
		buf.B, err = snappy.Decode(buf.B, b[len(redis.MarkerSnappy):])
		if err != nil {
			return nil, nil
		}
	} else {
		var err error
		buf.B, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	if buf.Len() <= 0 {
		return nil, fmt.Errorf("Empty response: %s", url)
	}

	// Notice that you should return m to the pool
	m := getPoolHMSet()
	if err := m.Unmarshal(buf.B); err != nil {
		return nil, err
	}
	return m, nil
}

func (srv *Server) getHGetAll(key string) (*redis.Resp, error) {
	var m *Hmset
	var err error

	for i := 0; i < maxConnectionsTries; i++ {
		m, err = srv.get(key)
		if err == nil {
			defer putPoolHMSet(m)
			return m.getAllAsRedis()
		}
		time.Sleep(retryTime * 2)
	}

	return nil, err
}

func (srv *Server) getHGet(key, field string) (*redis.Resp, error) {
	var m *Hmset
	var err error

	for i := 0; i < maxConnectionsTries; i++ {
		m, err = srv.get(key)
		if err == nil {
			defer putPoolHMSet(m)
			return m.getOneAsRedis(field)
		}
		time.Sleep(retryTime * 2)
	}

	return nil, err
}
