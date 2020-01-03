package httpToFirehose

import (
	"encoding/json"
	firehosePool "github.com/gabrielperezs/streamspooler/firehose"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gin-gonic/gin"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config  lib.RelayerConfig
	done    chan bool
	exiting bool
	engine  *gin.Engine

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

	gin.SetMode(gin.ReleaseMode)
	srv.engine = gin.New()
	srv.engine.POST("/firehose_raw", srv.submitRaw)
	srv.engine.POST("/firehose", srv.submit)
	srv.engine.GET("/ping", srv.ok)
	go func() {
		switch {
		case strings.HasPrefix(srv.config.Listen, "tcp://"):
			e = srv.engine.Run(srv.config.Listen[6:])
		case strings.HasPrefix(srv.config.Listen, "unix://"):
			e = srv.engine.RunUnix(srv.config.Listen[7:])
		}
	}()
	return e
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	srv.exiting = true

	if srv.engine != nil {
		// finish gin
	}

	// finishing the server
	srv.done <- true
}

func (srv *Server) ok(ctx *gin.Context) {
	ctx.IndentedJSON(http.StatusOK, map[string]interface{}{
		"status": "success",
	})
}

func (srv *Server) submitRaw(ctx *gin.Context) {

	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, map[string]interface{}{
			"Error": err.Error(),
		})
		return
	}

	srv.sendBytes(b) // Does not check if the data is a valid json.

	ctx.IndentedJSON(http.StatusOK, map[string]interface{}{
		"status": "success",
	})
}

func (srv *Server) submit(ctx *gin.Context) {

	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, map[string]interface{}{
			"Error": err.Error(),
		})
		return
	}
	var parsedJson map[string]interface{}

	err = json.Unmarshal(b, &parsedJson)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, map[string]interface{}{
			"Error": err.Error(),
		})
		return
	}

	var rowWithTimestamp = lib.NewInterRecord()
	rowWithTimestamp.SetData(parsedJson)
	srv.sendRecord(rowWithTimestamp)

	ctx.IndentedJSON(http.StatusOK, map[string]interface{}{
		"status": "success",
	})
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
