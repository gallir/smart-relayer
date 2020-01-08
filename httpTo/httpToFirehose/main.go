package httpToFirehose

import (
	"encoding/json"
	firehosePool "github.com/gabrielperezs/streamspooler/firehose"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fasthttp/router"
	"github.com/gallir/smart-relayer/lib"
	"github.com/valyala/fasthttp"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config  lib.RelayerConfig
	done    chan bool
	exiting bool
	engine  *fasthttp.Server

	fh             *firehosePool.Server
	lastConnection time.Time
	lastError      time.Time
}

var (
	defaultTimeout          = 5 * time.Second
	defaultMaxResults int64 = 100
)

// New creates a new http local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done: done,
	}

	srv.Reload(&c)

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c

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

	if srv.engine != nil {
		return nil
	}

	r := router.New()
	r.GET("/ping", srv.ok)
	r.POST("/firehose-raw", srv.submitRaw)
	r.POST("/firehose", srv.submit)

	srv.engine = &fasthttp.Server{
		Handler: r.Handler,
	}

	go func() {
		switch {
		case strings.HasPrefix(srv.config.Listen, "tcp://"):
			e = srv.engine.ListenAndServe(srv.config.Listen[6:])
		case strings.HasPrefix(srv.config.Listen, "unix://"):
			e = srv.engine.ListenAndServeUNIX(srv.config.Listen[7:], os.FileMode(0666))
		}
	}()
	return e
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	srv.exiting = true

	if srv.engine != nil {
		if e := srv.engine.Shutdown(); e != nil {
			log.Println("httpToFirehose ERROR: shutting down failed", e)
		}
	}

	// finishing the server
	srv.done <- true
}

func (srv *Server) ok(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("{\"status\": \"success\"}"))
}

func (srv *Server) submitRaw(ctx *fasthttp.RequestCtx) {
	srv.sendBytes(ctx.Request.Body()) // Does not check if the data is a valid json.

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("{\"status\": \"success\"}"))
}

func (srv *Server) submit(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/json")

	var parsedJson map[string]interface{}
	err := json.Unmarshal(ctx.Request.Body(), &parsedJson)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte("{\"status\": \"invalid_json\"}"))
		return
	}

	var rowWithTimestamp = lib.NewInterRecord()
	rowWithTimestamp.SetData(parsedJson)
	srv.sendRecord(rowWithTimestamp)

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("{\"status\": \"success\"}"))
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
