package fh

import (
	"errors"
	"log"
	"net"
	"sync"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	done     chan bool
	exiting  bool
	listener net.Listener

	clients   []*Client
	recordsCh chan record
}

const (
	selectCommand     = "SELECT"
	maxConnections    = 3
	requestBufferSize = 4000
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errOverloaded  = errors.New("Redis overloaded")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	commands       map[string]*redis.Resp
)

func init() {
	commands = map[string]*redis.Resp{
		"PING":   respOK,
		"MULTI":  respOK,
		"EXEC":   respOK,
		"SET":    respOK,
		"SADD":   respOK,
		"HMSET":  respOK,
		"RAWSET": respOK,
	}
}

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done:      done,
		recordsCh: make(chan record, requestBufferSize),
	}
	srv.Reload(&c)
	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) error {
	srv.Lock()
	defer srv.Unlock()

	for _, c := range srv.clients {
		c.Exit()
	}

	srv.clients = nil

	srv.config = *c

	log.Printf("Reload firehost config to the stream %s listen %s", srv.config.StreamName, srv.config.Listen)

	for i := 0; i < maxConnections; i++ {
		srv.clients = append(srv.clients, NewClient(c, srv.recordsCh))
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
					log.Println("Timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if srv.exiting {
					log.Println("Exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("Emergency error in local listener", srv.config.ListenHost(), e)
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

func (srv *Server) sendRecord(r record) {
	select {
	case srv.recordsCh <- r:
	default:
		log.Printf("ERROR: main channel is full. Queued messages %d", len(srv.recordsCh))
	}
}

func (srv *Server) sendBytes(b []byte) {
	r := newRecord()
	r.types = 1
	r.bytes = b

	srv.sendRecord(r)
}

func (srv *Server) handleConnection(netCon net.Conn) {

	// Transaction detected in this connection
	multi := false

	var record record
	defer func() {
		if multi {
			log.Printf("ERROR: MULTI start but didn't sent an EXEC before close the connection")
			putRecord(record)
		}
	}()

	defer netCon.Close()
	reader := redis.NewRespReader(netCon)

	for {

		r := reader.Read()
		if r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				// Paranoid, don't close it just log it
				log.Println("Local client listen timeout at", srv.config.Listen)
				continue
			}
			// Connection was closed
			return
		}

		req := newRequest(r, &srv.config)
		if req == nil {
			respBadCommand.WriteTo(netCon)
			continue
		}

		fastResponse, ok := commands[req.command]
		if !ok {
			respBadCommand.WriteTo(netCon)
			continue
		}

		switch req.command {
		case "PING":
		case "MULTI":
			multi = true
			record = newRecord()
		case "EXEC":
			multi = false
			srv.sendRecord(record)
		case "RAWSET":
			if multi || len(req.items) > 2 {
				respKO.WriteTo(netCon)
				continue
			}
			b, _ := req.items[1].Bytes()
			srv.sendBytes(b)
		case "SET":
			k, _ := req.items[1].Str()
			v, _ := req.items[2].Str()
			if multi {
				record.add(k, v)
			} else {
				record = newRecord()
				record.add(k, v)
				srv.sendRecord(record)
			}
		case "SADD":
			k, _ := req.items[1].Str()
			v, _ := req.items[2].Str()
			if multi {
				record.sadd(k, v)
			} else {
				record = newRecord()
				record.sadd(k, v)
				srv.sendRecord(record)
			}
		case "HMSET":
			var key string
			var k string
			var v string

			if !multi {
				record = newRecord()
			}

			for i, o := range req.items {
				if i == 0 {
					continue
				}

				if i == 1 {
					key, _ = o.Str()
					continue
				}

				if i%2 == 0 {
					k, _ = o.Str()
				} else {
					v, _ = o.Str()
					record.mhset(key, k, v)
				}
			}

			if !multi {
				srv.sendRecord(record)
			}

		}

		fastResponse.WriteTo(netCon)
		continue

	}
}
