package httpToAthena

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/httpTo/httpToAthena/ifaceAthena"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
	"github.com/gin-gonic/gin"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config  lib.RelayerConfig
	done    chan bool
	exiting bool
	engine  *gin.Engine

	iface          *ifaceAthena.Athena
	lastConnection time.Time
	lastError      time.Time
}

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errOverloaded  = errors.New("Redis overloaded")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	commands       map[string]*redis.Resp

	defaultTimeout = 5 * time.Second
)

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
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c

	at, err := ifaceAthena.New(ifaceAthena.Config{
		Region:         c.Region,
		Profile:        c.Profile,
		OutputLocation: c.S3Bucket,
	})
	if err != nil {
		return err
	}

	srv.iface = at

	return nil
}

// Start accepts incoming connections on the Listener
func (srv *Server) Start() (e error) {

	if srv.engine != nil {
		return nil
	}

	srv.engine = gin.Default()
	srv.engine.POST("/:database", srv.query)
	srv.engine.GET("/:jobId", srv.get)
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

func (srv *Server) query(ctx *gin.Context) {
	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}

	r, err := srv.iface.Query(ctx.Param("database"), string(b))
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, map[string]interface{}{
			"Error": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, map[string]interface{}{
		"jobId": r,
	})
}

func (srv *Server) get(ctx *gin.Context) {
	nextToken := ctx.GetHeader("X-NextToken")

	timeOut := defaultTimeout
	if ctx.GetHeader("X-Timeout") != "" {
		var err error
		timeOut, err = time.ParseDuration(ctx.GetHeader("X-Timeout"))
		if err != nil {
			timeOut = defaultTimeout
		}
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			r, nextToken, err := srv.iface.Read(ctx.Param("jobId"), nextToken)
			if err != nil {
				if err == ifaceAthena.ErrPending {
					continue
				}
				ctx.IndentedJSON(http.StatusBadRequest, map[string]interface{}{
					"Error": err.Error(),
				})
				return
			}
			ctx.IndentedJSON(http.StatusOK, map[string]interface{}{
				"nextToken": nextToken,
				"rows":      r,
			})
			return
		case <-ctxTimeout.Done():
			ctx.IndentedJSON(http.StatusCreated, map[string]interface{}{
				"Error": ctxTimeout.Err().Error(),
			})
			return
		}
	}
}
