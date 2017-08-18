package fh

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	done     chan bool
	exiting  bool
	reseting bool
	failing  bool
	listener net.Listener

	clients        []*Client
	recordsCh      chan record
	awsSvc         *firehose.Firehose
	lastConnection time.Time
	lastError      time.Time
	errors         int64
}

const (
	maxConnections      = 2
	requestBufferSize   = 1024 * 5
	maxConnectionsTries = 3
	connectionRetry     = 5 * time.Second
	errorsFrame         = 10 * time.Second
	maxErrors           = 10 // Limit of errors to restart the connection
	connectTimeout      = 5 * time.Second
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
		errors:    0,
		recordsCh: make(chan record, requestBufferSize),
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

	go srv.retry()

	return nil
}

func (srv *Server) retry() {
	srv.Lock()
	defer srv.Unlock()

	if srv.failing {
		return
	}

	srv.failing = true
	tries := 0

	for {
		if srv.clientsReset() == nil {
			return
		}

		tries++
		log.Printf("Firehose ERROR: %d attempts to connect to kinens", tries)

		if tries >= maxConnectionsTries {
			time.Sleep(connectionRetry * 2)
		} else {
			time.Sleep(connectionRetry)
		}
	}
}

func (srv *Server) clientsReset() (err error) {

	if srv.exiting {
		return nil
	}

	srv.reseting = true
	defer func() { srv.reseting = false }()

	lib.Debugf("Firehose Reload config to the stream %s listen %s", srv.config.StreamName, srv.config.Listen)

	var sess *session.Session

	if srv.config.Profile != "" {
		sess, err = session.NewSessionWithOptions(session.Options{Profile: srv.config.Profile})
	} else {
		sess, err = session.NewSession()
	}

	if err != nil {
		log.Printf("Firehose ERROR: session: %s", err)

		srv.errors++
		srv.lastError = time.Now()
		return err
	}

	srv.awsSvc = firehose.New(sess, &aws.Config{Region: aws.String(srv.config.Region)})
	stream := &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: &srv.config.StreamName,
	}

	var l *firehose.DescribeDeliveryStreamOutput
	l, err = srv.awsSvc.DescribeDeliveryStream(stream)
	if err != nil {
		log.Printf("Firehose ERROR: describe stream: %s", err)

		srv.errors++
		srv.lastError = time.Now()
		return err
	}

	lib.Debugf("Firehose Connected to %s (%s) status %s",
		*l.DeliveryStreamDescription.DeliveryStreamName,
		*l.DeliveryStreamDescription.DeliveryStreamARN,
		*l.DeliveryStreamDescription.DeliveryStreamStatus)

	srv.lastConnection = time.Now()
	srv.errors = 0
	srv.failing = false

	for i := len(srv.clients); i < srv.config.MaxConnections; i++ {
		srv.clients = append(srv.clients, NewClient(srv))
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
					log.Println("Firehose ERROR: exiting local listener", srv.config.ListenHost())
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

	for _, c := range srv.clients {
		c.Exit()
	}

	lib.Debugf("Firehose: messages lost %d", len(srv.recordsCh))

	// finishing the server
	srv.done <- true
}

func (srv *Server) failure() {
	srv.Lock()
	defer srv.Unlock()

	if srv.reseting || srv.exiting {
		return
	}

	if time.Now().Sub(srv.lastError) > errorsFrame {
		srv.errors = 0
	}

	srv.errors++
	srv.lastError = time.Now()
	log.Printf("Firehose: %d errors detected", srv.errors)

	if srv.errors > maxErrors {
		go srv.retry()
	}
}

func (srv *Server) canSend() bool {
	if srv.reseting || srv.exiting || srv.failing {
		return false
	}

	return true
}

func (srv *Server) sendRecord(r record) {
	if !srv.canSend() {
		putRecord(r)
		return
	}

	select {
	case srv.recordsCh <- r:
	default:
		lib.Debugf("Firehose: channel is full. Queued messages %d", len(srv.recordsCh))
		putRecord(r)
	}
}

func (srv *Server) sendBytes(b []byte) {
	r := newRecord()
	r.types = 1
	r.bytes = b

	srv.sendRecord(r)
}

func (srv *Server) handleConnection(netCon net.Conn) {

	defer netCon.Close()

	reader := redis.NewRespReader(netCon)

	// Active transaction
	multi := false

	var record record
	defer func() {
		if multi {
			log.Println("Firehose ERROR: MULTI closed before ending with EXEC")
			putRecord(record)
		}
	}()

	for {

		r := reader.Read()

		if r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				// Paranoid, don't close it just log it
				log.Println("Firehose: Local client listen timeout at", srv.config.Listen)
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

		fastResponse, ok := commands[req.Command]
		if !ok {
			respBadCommand.WriteTo(netCon)
			continue
		}

		switch req.Command {
		case "RAWSET":
			if multi || len(req.Items) > 2 {
				respKO.WriteTo(netCon)
				continue
			}
			src, _ := req.Items[1].Bytes()
			c := make([]byte, len(src))
			copy(c, src)
			srv.sendBytes(c)
		case "MULTI":
			multi = true
			record = newRecord()
		case "EXEC":
			multi = false
			srv.sendRecord(record)
		case "SET":
			k, _ := req.Items[1].Str()
			v, _ := req.Items[2].Str()
			if multi {
				record.add(k, v)
			} else {
				record = newRecord()
				record.add(k, v)
				srv.sendRecord(record)
			}
		case "SADD":
			k, _ := req.Items[1].Str()
			v, _ := req.Items[2].Str()
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

			for i, o := range req.Items {
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
