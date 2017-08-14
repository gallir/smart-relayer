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
	listener net.Listener

	clients        []*Client
	recordsCh      chan record
	awsSvc         *firehose.Firehose
	lastConnection time.Time
	lastError      time.Time
	errors         int64
}

const (
	maxConnections    = 2
	requestBufferSize = 5000
	connectionRetry   = 5 * time.Second
	errorWindow       = 10 * time.Second
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

	err := srv.Reload(&c)

	return srv, err
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c

	if srv.config.MaxConnections <= 0 {
		srv.config.MaxConnections = maxConnections
	}

	for {
		if srv.clientsReset() == nil {
			return nil
		}

		log.Printf("Waiting for the next connection try")
		time.Sleep(connectionRetry)
	}

}

func (srv *Server) clientsReset() (err error) {

	if srv.exiting {
		return nil
	}

	srv.reseting = true
	defer func() { srv.reseting = false }()

	lib.Debugf("clientReset")
	defer lib.Debugf("clientReset DONE")

	log.Printf("Reload firehost config to the stream %s listen %s", srv.config.StreamName, srv.config.Listen)

	var sess *session.Session

	if srv.config.Profile != "" {
		sess, err = session.NewSessionWithOptions(session.Options{Profile: srv.config.Profile})
	} else {
		sess, err = session.NewSession()
	}

	if err != nil {
		log.Printf("Error session: %s", err)

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
		log.Printf("ERROR connecting to kinesis/firehost: %s", err)

		srv.errors++
		srv.lastError = time.Now()
		return err
	}

	lib.Debugf("Connected to kinesis/firehost deliver to %s (%s) status %s",
		*l.DeliveryStreamDescription.DeliveryStreamName,
		*l.DeliveryStreamDescription.DeliveryStreamARN,
		*l.DeliveryStreamDescription.DeliveryStreamStatus)

	srv.lastConnection = time.Now()
	srv.errors = 0

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

	for _, c := range srv.clients {
		c.Exit()
	}

	lib.Debugf("Messages lost: %d", len(srv.recordsCh))

	// finishing the server
	srv.done <- true
}

func (srv *Server) failure() {
	if srv.reseting || srv.exiting {
		return
	}

	srv.Lock()
	defer srv.Unlock()

	if time.Now().Sub(srv.lastError) > errorWindow {
		srv.errors = 0
	}

	srv.errors++
	srv.lastError = time.Now()
	log.Println("Error detected", srv.errors)

	if srv.errors > 10 {
		for {
			if srv.clientsReset() == nil {
				return
			}

			log.Printf("Waiting for the next connection try")
			time.Sleep(connectionRetry)
		}
	}
}

func (srv *Server) canSend() bool {
	if srv.reseting || srv.exiting {
		return false
	}

	return true
}

func (srv *Server) sendRecord(r record) {
	if !srv.canSend() {
		return
	}

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

	// Active transaction
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
			b, _ := req.Items[1].Bytes()
			srv.sendBytes(b)
		case "PING":
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
