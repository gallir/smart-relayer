package fh

import (
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	firehosePool "github.com/gabrielperezs/streamspooler/firehose"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	done     chan bool
	exiting  bool
	listener net.Listener

	fh             *firehosePool.Server
	lastConnection time.Time
	lastError      time.Time

	ignoreCommands atomic.Value
}

const (
	maxConnections      = 2
	requestBufferSize   = 1024 * 10
	maxConnectionsTries = 3
	connectionRetry     = 5 * time.Second
	errorsFrame         = 10 * time.Second
	maxErrors           = 10 // Limit of errors to restart the connection
	connectTimeout      = 15 * time.Second
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errOverloaded  = errors.New("Redis overloaded")
	respPong       = redis.NewRespSimple("PONG")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	commands       map[string]*redis.Resp
)

func init() {
	commands = map[string]*redis.Resp{
		"GET":    respOK,
		"MULTI":  respOK,
		"EXEC":   respOK,
		"SET":    respOK,
		"SADD":   respOK,
		"HMSET":  respOK,
		"CSET":   respOK,
		"CSADD":  respOK,
		"CHMSET": respOK,
		"RAWSET": respOK,
	}
}

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}

	srv.Reload(&c)

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {

	// Config struct for the firehose plugin
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c

	// Ignore commands store as an atomic value
	ignore := make(map[string]*redis.Resp)
	if srv.config.IgnoreCommands != "" {
		for _, s := range strings.Split(srv.config.IgnoreCommands, " ") {
			ignore[strings.ToUpper(s)] = respOK
		}
	}
	log.Printf("DEBUG IgnoreCommands: %s | %v", srv.config.IgnoreCommands, ignore)
	srv.ignoreCommands.Store(ignore)

	if srv.config.MaxConnections <= 0 {
		srv.config.MaxConnections = maxConnections
	}

	if srv.config.Buffer == 0 {
		srv.config.Buffer = requestBufferSize
	}

	fhConfig := firehosePool.Config{
		Profile:       srv.config.Profile,
		Region:        srv.config.Region,
		StreamName:    srv.config.StreamName,
		MaxWorkers:    srv.config.MaxConnections,
		MaxRecords:    srv.config.MaxRecords,
		Buffer:        srv.config.Buffer,
		ConcatRecords: srv.config.Concat,
		Critical:      srv.config.Critical,
	}

	if srv.fh == nil {
		srv.fh = firehosePool.New(fhConfig)
	} else {
		go srv.fh.Reload(&fhConfig)
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
					log.Println("Firehose ERROR: timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if srv.exiting {
					log.Println("Firehose: exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("Firehose ERROR: emergency error in local listener", srv.config.ListenHost(), e)
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

	go srv.fh.Exit()

	srv.fh.Waiting()

	// finishing the server
	srv.done <- true
}

func (srv *Server) sendRecord(r *lib.InterRecord) {
	if srv.exiting {
		return
	}

	// It blocks until the message can be delivered, for critical logs
	if srv.config.Critical || srv.config.Mode != "smart" {
		srv.fh.C <- r.Bytes()
		return
	}

	select {
	case srv.fh.C <- r.Bytes():
	default:
		log.Printf("Firehose: channel is full, discarded. Queued messages %d", len(srv.fh.C))
	}
}

func (srv *Server) sendBytes(b []byte) {
	r := lib.NewInterRecord()
	r.Types = 1
	r.Raw = b
	srv.sendRecord(r)
}

func (srv *Server) handleConnection(netCon net.Conn) {

	defer netCon.Close()

	reader := redis.NewRespReader(netCon)

	// Active transaction
	multi := false

	var row *lib.InterRecord
	defer func() {
		if multi {
			log.Println("Firehose ERROR: MULTI closed before ending with EXEC")
		}
	}()

	var ignoreCommands map[string]*redis.Resp
	ignoreCommands, _ = srv.ignoreCommands.Load().(map[string]*redis.Resp)

	for {

		r := reader.Read()

		if r.IsType(redis.Err) {
			if r.IsType(redis.IOErr) {
				if r.IsType(redis.IOErr) && redis.IsTimeout(r) {
					// Paranoid, don't close it just log it
					log.Println("Firehose: Local client listen timeout at", srv.config.Listen)
					continue
				}
				// Connection was closed
				return
			}
			// Other error
			lib.Debugf("Error with request %#v", r.String())
			return
		}

		// For debug
		lib.Debugf("DEBUG: %#v", r.String())

		req := lib.NewRequest(r, &srv.config)
		if req == nil {
			respBadCommand.WriteTo(netCon)
			continue
		}

		// Just return, without other effect
		if ignoreCommand, ok := ignoreCommands[req.Command]; ok {
			ignoreCommand.WriteTo(netCon)
			lib.Debugf("DEBUG ignore command: %#v", r.String())
			continue
		}

		// Warning, the following switch case block needs to have a "continue"
		// after writing to netConn
		switch req.Command {
		case "ECHO":
			if len(req.Items) == 2 {
				req.Items[1].WriteTo(netCon)
			} else {
				respBadCommand.WriteTo(netCon)
			}
			continue
		case "PING":
			if len(req.Items) == 2 {
				req.Items[1].WriteTo(netCon)
			} else {
				respPong.WriteTo(netCon)
			}
			continue
		case "EXISTS":
			respTrue.WriteTo(netCon)
			continue
		}

		fastResponse, ok := commands[req.Command]
		if !ok {
			respBadCommand.WriteTo(netCon)
			continue
		}
		fastResponse.WriteTo(netCon)

		lib.Debugf("DEBUG fastResponse command: %#v", req)

		switch req.Command {
		case "RAWSET":
			if len(req.Items) < 2 {
				lib.Debugf("Error, bad number of arguments %#v", r.String())
				continue
			}
			if multi || len(req.Items) > 2 {
				respKO.WriteTo(netCon)
				continue
			}
			src, _ := req.Items[1].Bytes()
			srv.sendBytes(src)
		case "MULTI":
			multi = true
			row = lib.NewInterRecord()
		case "EXEC":
			multi = false
			srv.sendRecord(row)
		case "SET", "CSET":
			if len(req.Items) < 3 {
				lib.Debugf("Error, bad number of arguments %#v", r.String())
				continue
			}
			k, _ := req.Items[1].Str()

			var v interface{}
			if req.Command == "CSET" {
				v, _ = req.Items[2].Bytes()
			} else {
				v, _ = req.Items[2].Str()
			}

			if multi {
				row.Add(k, v)
			} else {
				row = lib.NewInterRecord()
				row.Add(k, v)
				srv.sendRecord(row)
			}
		case "SADD", "CSADD":
			if len(req.Items) < 3 {
				lib.Debugf("Error, bad number of arguments %#v", r.String())
				continue
			}
			k, _ := req.Items[1].Str()

			var v interface{}
			if req.Command == "CSADD" {
				v, _ = req.Items[2].Bytes()
			} else {
				v, _ = req.Items[2].Str()
			}

			if multi {
				row.Sadd(k, v)
			} else {
				row = lib.NewInterRecord()
				row.Sadd(k, v)
				srv.sendRecord(row)
			}
		case "HMSET", "CHMSET":
			var key string
			var k string
			var v interface{}

			if len(req.Items) < 3 {
				lib.Debugf("Error, bad number of arguments %#v", r.String())
				continue
			}
			if !multi {
				row = lib.NewInterRecord()
				row.Types = 0
			}

			for i, o := range req.Items[1:] {
				if i == 0 {
					key, _ = o.Str()
					continue
				}

				// Now odd elements are the keys
				if i%2 != 0 {
					k, _ = o.Str()
				} else {
					if req.Command == "CHMSET" {
						v, _ = o.Bytes()
					} else {
						v, _ = o.Str()
					}
					row.Mhset(key, k, v)
				}
			}

			if !multi {
				srv.sendRecord(row)
			}
		}
	}
}
