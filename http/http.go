package http

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

const (
	defaultBytePoolSize = 128
)

// Pool is used, if enabled, as a pool for the reverse proxy
type Pool struct {
	pool sync.Pool
}

type HttpProxy struct {
	sync.Mutex
	target     *url.URL
	listen     string
	proxy      *httputil.ReverseProxy
	transport  *http.Transport
	server     *http.Server
	bufferPool Pool
	done       chan bool
}

// New returns a new http reverse proxy
func New(c lib.RelayerConfig, done chan bool) (*HttpProxy, error) {
	s := &HttpProxy{
		transport: &http.Transport{},
		done:      done,
	}

	s.proxy = &httputil.ReverseProxy{
		Director:  s.director,
		Transport: s.transport,
	}

	err := s.Reload(&c)
	if err != nil {
		log.Println("E: Error to", s.target, err)
		return nil, err
	}

	return s, nil
}

// Reload changes the configuration of the proxy
func (s *HttpProxy) Reload(c *lib.RelayerConfig) error {
	s.Lock()
	defer s.Unlock()

	s.listen = c.Listen

	if !c.Compress {
		s.transport.DisableCompression = true
	}

	if c.MaxIdleConnections > 0 {
		s.transport.MaxIdleConnsPerHost = c.MaxIdleConnections
		s.transport.MaxIdleConns = c.MaxIdleConnections
	} else {
		s.transport.DisableKeepAlives = true
	}

	if c.Timeout > 0 {
		s.transport.IdleConnTimeout = time.Duration(c.Timeout) * time.Second
	}

	if c.UseBufferPool {
		s.proxy.BufferPool = &s.bufferPool
	} else {
		s.proxy.BufferPool = nil
	}

	t, err := url.Parse(c.URL)
	if err != nil {
		return err
	}
	s.target = t

	if s.server != nil {
		s.transport.CloseIdleConnections()
	}

	return nil
}

// Start starts the http server in the local port/socket
func (s *HttpProxy) Start() error {
	if s.server == nil {
		s.server = &http.Server{
			Addr:           s.listen,
			Handler:        s.proxy,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
	}
	go s.serve()
	return nil
}

// Exit closes the listener and send done to main
func (s *HttpProxy) Exit() {
	s.Lock()
	defer s.Unlock()
	s.server.Close()
	s.transport.CloseIdleConnections()
	s.done <- true
}

func (s *HttpProxy) serve() {
	err := s.server.ListenAndServe()
	log.Println("E: Error in http server", s.listen, err)
	s.Lock()
	s.server = nil
	s.Unlock()
}

func (s *HttpProxy) director(req *http.Request) {
	req.URL.Scheme = s.target.Scheme
	req.URL.Host = s.target.Host
	if s.target.Path != "" {
		req.URL.Path = s.target.Path + req.URL.Path
	}
	if _, ok := req.Header["User-Agent"]; !ok {
		// explicitly disable User-Agent so it's not set to default value
		req.Header.Set("User-Agent", "")
	}
}

// Get returns a []byte from the Pool
func (p *Pool) Get() []byte {
	b := p.pool.Get()
	if b == nil {
		return make([]byte, 0, defaultBytePoolSize)
	}
	return b.([]byte)
}

// Put a []byte into the pool
func (p *Pool) Put(b []byte) {
	p.pool.Put(b)
}
