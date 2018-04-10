package fs

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

const (
	waitingForExit = 2 * time.Second
)

// Server is the thread that listen for clients' connections
type Server struct {
	sync.Mutex
	config   lib.RelayerConfig
	done     chan bool
	exiting  uint32
	listener net.Listener

	breakPoint  int64
	running     int64
	shardServer *ShardsServer
	shards      uint32

	lastError time.Time
	errors    int64

	s3sess *session.Session
}

var (
	sep     = []byte("\t")
	newLine = []byte("\n")
)

var (
	errBadCmd      = errors.New("ERR bad command")
	errKO          = errors.New("fatal error")
	errSet         = errors.New("ERR - syntax: SET project key [timestamp] value")
	errGet         = errors.New("ERR - syntax: GET project key [timestamp]")
	errChanFull    = errors.New("ERR - The file can't be created")
	errNotFound    = errors.New("KO - Key not found")
	errExiting     = errors.New("ERR - System exiting")
	errFailing     = errors.New("ERR - System failing")
	respOK         = redis.NewRespSimple("OK")
	respTrue       = redis.NewResp(1)
	respBadCommand = redis.NewResp(errBadCmd)
	respKO         = redis.NewResp(errKO)
	respBadSet     = redis.NewResp(errSet)
	respBadGet     = redis.NewResp(errGet)
	respChanFull   = redis.NewResp(errChanFull)
	respNotFound   = redis.NewResp(errNotFound)
	respExiting    = redis.NewResp(errExiting)
	respFailing    = redis.NewResp(errFailing)
	commands       map[string]*redis.Resp

	defaultWritersByShard        = 2
	defaultShards                = 32
	defaultInterval              = 500 * time.Millisecond
	defaultWriterCooldown        = 15 * time.Second
	defaultWriterThresholdWarmUp = 50.0 // percent
	defaultBuffer                = 20000
	defaultPath                  = "/tmp"
	defaultBreakMultiplier       = 5
)

func init() {
	commands = map[string]*redis.Resp{
		"PING": respOK,
		"SET":  respOK,
		"GET":  respOK,
	}
}

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done:   done,
		errors: 0,
	}

	srv.Reload(&c)

	return srv, nil
}

func (srv *Server) exitingOrFailing() error {
	if atomic.LoadUint32(&srv.exiting) > 0 {
		return errExiting
	}

	if atomic.LoadInt64(&srv.running) >= srv.breakPoint {
		return errFailing
	}

	return nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c
	if srv.config.Buffer == 0 {
		srv.config.Buffer = defaultBuffer
	}

	if srv.config.Path == "" {
		srv.config.Path = defaultPath
	}

	// If Shards is 0 we manage it as undefined so we apply the default value
	if srv.config.Shards == 0 {
		srv.config.Shards = defaultShards
	}
	// If the Shards are higher than 0 (could be by default or defined by the config file)
	// In case the shards are defined with a value less than 0 we will manage it as disabled
	// definding srv.shards as 0, look in function srv.path
	if srv.config.Shards > 0 {
		srv.shards = uint32(srv.config.Shards)
	}

	// Define the number of workers for EACH shard
	if srv.config.Writers == 0 {
		srv.config.Writers = defaultWritersByShard
	}

	// Break point to declare as unhealthy
	if srv.config.BreakMultiplier == 0 {
		srv.config.BreakMultiplier = defaultBreakMultiplier
	}
	srv.breakPoint = int64(srv.config.Shards * srv.config.Writers * srv.config.BreakMultiplier)

	// Create new shards servers or update the config
	if srv.shardServer == nil {
		srv.shardServer = NewShardsServer(srv)
	} else {
		srv.shardServer.reload()
	}

	awsOpt := session.Options{
		Profile: srv.config.Profile,
		Config: aws.Config{
			Region: &srv.config.Region,
		},
	}

	if sess, err := session.NewSessionWithOptions(awsOpt); err == nil {
		srv.s3sess = sess
	} else {
		log.Printf("FS ERROR: invalid S3 session: %s", err)
		srv.s3sess = nil
	}

	log.Printf("FS %s config Buffer %d Shards %d/%d (writers), total writers %d, running %d/%d",
		srv.config.Listen, srv.config.Buffer, srv.shards, srv.config.Writers,
		int(srv.shards)*srv.config.Writers, atomic.LoadInt64(&srv.running), srv.breakPoint)

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
					log.Println("FS ERROR: timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if atomic.LoadUint32(&srv.exiting) > 0 {
					log.Println("FS: exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("FS ERROR: emergency error in local listener", srv.config.ListenHost(), e)
				return
			}
			go srv.handleConnection(netConn)
		}
	}(srv.listener)

	return
}

// Exit closes the listener and send done to main
func (srv *Server) Exit() {
	atomic.StoreUint32(&srv.exiting, 1)

	if srv.listener != nil {
		srv.listener.Close()
	}

	retry := 0
	for retry < 10 {
		n := atomic.LoadInt64(&srv.running)
		if n == 0 {
			break
		}
		log.Printf("FS Waiting that %d process are still running", n)
		time.Sleep(waitingForExit)
		retry++
	}

	if n := atomic.LoadInt64(&srv.running); n > 0 {
		log.Printf("FS ERROR: %d messages lost", n)
	}

	srv.shardServer.Exit()

	// finishing the server
	srv.done <- true
}

func (srv *Server) handleConnection(netCon net.Conn) {

	defer netCon.Close()

	reader := redis.NewRespReader(netCon)

	for {

		r := reader.Read()

		if r.IsType(redis.IOErr) {
			if redis.IsTimeout(r) {
				// Paranoid, don't close it just log it
				log.Println("FS ERROR: Local client listen timeout at", srv.config.Listen)
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
		case "PING":
			if err := srv.exitingOrFailing(); err != nil {
				log.Printf("FS ERROR: %s", err)
				switch err {
				case errFailing:
					respFailing.WriteTo(netCon)
				case errExiting:
					respExiting.WriteTo(netCon)
				}
				continue
			}
			fastResponse.WriteTo(netCon)
		case "SET":
			// SET project key [timestamp] value
			if len(req.Items) <= 3 || len(req.Items) > 5 {
				respBadSet.WriteTo(netCon)
				continue
			}

			if err := srv.set(netCon, req.Items); err != nil {
				log.Printf("FS ERROR: %s", err)
				switch err {
				case errFailing:
					respFailing.WriteTo(netCon)
				case errExiting:
					respExiting.WriteTo(netCon)
				default:
					redis.NewResp(err).WriteTo(netCon)
				}
			}
		case "GET":
			// GET project key [timestamp]
			if len(req.Items) <= 2 || len(req.Items) > 4 {
				respBadGet.WriteTo(netCon)
				continue
			}

			if err := srv.get(netCon, req.Items); err != nil {
				switch err {
				case err.(*os.PathError):
					respNotFound.WriteTo(netCon)
				default:
					log.Printf("FS ERROR GET: %s", err)
					redis.NewResp(err).WriteTo(netCon)
				}
			}
		default:
			log.Panicf("FS ERROR: Invalid command: This never should happen, check the cases or the list of valid command")
		}

	}
}

func (srv *Server) set(netCon net.Conn, items []*redis.Resp) (err error) {

	if err := srv.exitingOrFailing(); err != nil {
		return err
	}

	// Get a message struct from the pool
	msg := getMsg(srv)

	// Find the project name
	msg.project, err = items[1].Str()
	if err != nil {
		return err
	}

	// Find the key name of the message
	msg.k, err = items[2].Str()
	if err != nil {
		return err
	}
	// Calculate the shard for this message
	msg.getShard()

	if len(items) == 4 {
		// If have 4 items means that the client sent the content without timestamp
		// in this case the message will have the current timestamp
		if b, err := items[3].Bytes(); err == nil {
			msg.b.Set(b)
		} else {
			return err
		}
		// Define the current timestamp
		msg.t = time.Now()
	} else {
		// If have more than 4 items we read timestamp
		if i, err := items[3].Int64(); err == nil {
			msg.t = time.Unix(i, 0)
		} else {
			return err
		}

		if b, err := items[4].Bytes(); err == nil {
			msg.b.Set(b)
		} else {
			return err
		}
	}

	r := msg.fullpath() + "/" + msg.filename()

	// If the server is configured in sync mode we will try to write
	// the file directly and respons to the client with the result
	if srv.config.Mode == "sync" {
		if err := msg.storeTmp(); err != nil {
			return err
		}
	}

	// Send response to the client
	redis.NewResp(r).WriteTo(netCon)

	if srv.config.Mode != "sync" {
		if err := msg.storeTmp(); err != nil {
			return err
		}
	}

	go msg.sentToShard()

	return nil
}

func (srv *Server) get(netCon net.Conn, items []*redis.Resp) (err error) {
	msg := getMsg(srv)
	defer putMsg(msg)

	// Find the project name
	msg.project, err = items[1].Str()
	if err != nil {
		return err
	}

	// Find the key name
	msg.k, err = items[2].Str()
	if err != nil {
		return err
	}
	msg.getShard()

	// Verify if the 3th item is a int64 value to be converted in time
	if len(items) > 3 {
		if i, err := items[3].Int64(); err == nil {
			msg.t = time.Unix(i, 0)
		} else {
			return err
		}
	}

	var b []byte
	b, err = msg.Bytes()
	if err != nil {
		return err
	}

	redis.NewResp(b).WriteTo(netCon)
	return nil
}
