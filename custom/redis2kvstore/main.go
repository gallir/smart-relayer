package redis2kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"net/http/httputil"

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
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errOverloaded  = errors.New("http server overloaded")
	errNotFound    = errors.New("Not found")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
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
				srv.send(key, defaultExpire, p)
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
			key, _ := req.Items[1].Str()
			if _, ok := pending[key]; !ok {
				pending[key] = getPoolHMSet()
			}
			pending[key].processItems(req.Items[2:])
			validCommand.WriteTo(netCon)
		case "EXPIRE":
			if len(req.Items) != 3 {
				respKO.WriteTo(netCon)
				continue
			}
			key, _ := req.Items[1].Str()
			expire, err := req.Items[2].Int()
			if _, ok := pending[key]; ok {
				go func(key string, expire int, p *Hmset) {
					defer putPoolHMSet(p)
					if expire == 0 || err != nil {
						expire = defaultExpire
					}
					srv.send(key, expire, p)
				}(key, expire, pending[key])

				delete(pending, key)
				validCommand.WriteTo(netCon)
			}
			respBadCommand.WriteTo(netCon)
		case "HGETALL":
			if len(req.Items) != 2 {
				respKO.WriteTo(netCon)
				continue
			}
			key, _ := req.Items[1].Str()
			items, err := srv.getHGetAll(key)
			if err != nil {
				redis.NewResp(err).WriteTo(netCon)
				continue
			}
			items.WriteTo(netCon)
		case "HGET":
			if len(req.Items) != 3 {
				respKO.WriteTo(netCon)
				continue
			}
			key, _ := req.Items[1].Str()
			item, _ := req.Items[2].Str()
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
		}

	}
}

func (srv *Server) send(key string, expire int, p *Hmset) {
	w := pool.Get()
	defer pool.Put(w)

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

	resp, err := srv.client.Post(url, strContentType, bytes.NewReader(w.B))
	if err != nil {
		log.Printf("redis2kvstore ERROR connect: %s %s", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("redis2kvstore ERROR post: [%d] %s %s", resp.StatusCode, url, err)
		return
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
		b, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return nil, nil
		}
		buf.B, err = snappy.Decode(buf.B, b[len(redis.MarkerSnappy):])
		if err != nil {
			return nil, nil
		}
	} else {
		var err error
		buf.B, err = httputil.DumpResponse(resp, true)
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
	m, err := srv.get(key)
	if err != nil {
		return nil, err
	}
	defer putPoolHMSet(m)

	t := make(map[string][]byte, 0)
	for _, f := range m.Fields {
		t[f.Name] = f.Value
	}

	r := redis.NewResp(t)
	if r == nil {
		return nil, errBadCmd
	}
	return r, nil
}

func (srv *Server) getHGet(key, field string) (*redis.Resp, error) {
	m, err := srv.get(key)
	if err != nil {
		return nil, err
	}
	defer putPoolHMSet(m)

	for _, f := range m.Fields {
		if f.Name == field {
			return redis.NewResp(f.Value), nil
		}
	}

	return nil, errNotFound
}
