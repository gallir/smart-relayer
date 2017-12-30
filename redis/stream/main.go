package stream

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

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
	C        chan *Msg

	writers chan *writer

	lastError time.Time
	errors    int64

	s3sess *session.Session
}

var (
	sep     = []byte("\t")
	newLine = []byte("\n")
)

var (
	errBadCmd              = errors.New("ERR bad command")
	errKO                  = errors.New("fatal error")
	errOverloaded          = errors.New("Redis overloaded")
	errStreamSet           = errors.New("STSET [timestamp] [key] [value]")
	errStreamGet           = errors.New("STGET [timestamp] [key]")
	errStreamToBackend     = errors.New("ST2BACKEND [path]")
	errChanFull            = errors.New("The file can't be created")
	respOK                 = redis.NewRespSimple("OK")
	respTrue               = redis.NewResp(1)
	respBadCommand         = redis.NewResp(errBadCmd)
	respKO                 = redis.NewResp(errKO)
	respBadStreamSet       = redis.NewResp(errStreamSet)
	respBadStreamGet       = redis.NewResp(errStreamGet)
	respBadStreamToBackend = redis.NewResp(errStreamToBackend)
	respChanFull           = redis.NewResp(errChanFull)
	commands               map[string]*redis.Resp

	defaultMaxWriters   = 100
	defaultBuffer       = 1024
	defaultPath         = "/tmp"
	defaultLimitRecords = 9999
	defaultFileSize     = 50 * 1024 * 1025
)

func init() {
	commands = map[string]*redis.Resp{
		"PING":       respOK,
		"STSET":      respOK,
		"STGET":      respOK,
		"ST2BACKEND": respOK,
	}
}

// New creates a new Redis local server
func New(c lib.RelayerConfig, done chan bool) (*Server, error) {
	srv := &Server{
		done:    done,
		errors:  0,
		writers: make(chan *writer, defaultMaxWriters),
	}

	srv.Reload(&c)

	return srv, nil
}

// Reload the configuration
func (srv *Server) Reload(c *lib.RelayerConfig) (err error) {
	srv.Lock()
	defer srv.Unlock()

	srv.config = *c
	if srv.config.Buffer == 0 {
		srv.config.Buffer = defaultBuffer
	}

	if srv.config.MaxConnections == 0 {
		srv.config.MaxConnections = defaultMaxWriters
	}

	if srv.config.Path == "" {
		srv.config.Path = defaultPath
	}

	if srv.C == nil {
		srv.C = make(chan *Msg, srv.config.Buffer)
	}

	if sess, err := session.NewSessionWithOptions(session.Options{
		Profile: srv.config.Profile,
		Config: aws.Config{
			Region: &srv.config.Region,
		},
	}); err == nil {
		srv.s3sess = sess
	} else {
		log.Printf("ERROR: invalid S3 session: %s", err)
		srv.s3sess = nil
	}

	lw := len(srv.writers)

	if lw == srv.config.MaxConnections {
		return nil
	}

	if lw > srv.config.MaxConnections {
		for i := srv.config.MaxConnections; i < lw; i++ {
			w := <-srv.writers
			w.exit()
		}
		return nil
	}

	if lw < srv.config.MaxConnections {
		for i := lw; i < srv.config.MaxConnections; i++ {
			srv.writers <- newWriter(srv)
		}
		return nil
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
					log.Println("File ERROR: timeout at local listener", srv.config.ListenHost(), e)
					continue
				}
				if srv.exiting {
					log.Println("File: exiting local listener", srv.config.ListenHost())
					return
				}
				log.Fatalln("File ERROR: emergency error in local listener", srv.config.ListenHost(), e)
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

	// Close the channel were we store the active writers
	close(srv.writers)
	for w := range srv.writers {
		go w.exit()
	}

	// Close the main channel, all writers will finish
	close(srv.C)

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
				log.Println("File: Local client listen timeout at", srv.config.Listen)
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
			fastResponse.WriteTo(netCon)
		case "STSET":
			// This command require 4 elements
			if len(req.Items) != 4 {
				respBadStreamSet.WriteTo(netCon)
				continue
			}

			// Get message from the pool. This message will be return after write
			// in a file in one of the writers routines (writers.go). In case of error
			// will be returned to the pull immediately
			msg := getMsg(srv)
			// Read the key string and store in the Msg
			if err := msg.parse(req.Items); err != nil {
				// Response error to the client
				respBadStreamSet.WriteTo(netCon)
				// Return message to the pool just in errors
				putMsg(msg)
				continue
			}

			// Read bytes from the client and store in the message buffer (Msg.b)
			b, _ := req.Items[3].Bytes()
			msg.b.Write(b)

			select {
			case srv.C <- msg:
				fastResponse.WriteTo(netCon)
			default:
				respChanFull.WriteTo(netCon)
			}

		case "STGET":
			// This command require 3 items
			if len(req.Items) != 3 {
				respBadStreamGet.WriteTo(netCon)
				continue
			}

			// Get message from the pool.
			msg := getMsg(srv)
			// Read the key string and store in the Msg
			if err := msg.parse(req.Items); err != nil {
				// Response error to the client
				respBadStreamGet.WriteTo(netCon)
				// Return message to the pool
				putMsg(msg)
				continue
			}

			if b, err := msg.Bytes(); err != nil {
				redis.NewResp(err).WriteTo(netCon)
			} else {
				redis.NewResp(b).WriteTo(netCon)
			}

			putMsg(msg)

		case "ST2BACKEND":
			if len(req.Items) > 3 {
				respBadStreamToBackend.WriteTo(netCon)
				continue
			}

			fromTime, _ := req.Items[1].Str()

			d, err := time.ParseDuration(fromTime)
			if err != nil {
				respBadStreamToBackend.WriteTo(netCon)
				continue
			}

			until := time.Duration(15 * time.Minute)
			if len(req.Items) > 2 {
				if untilTime, err := req.Items[2].Str(); err == nil {
					until, err = time.ParseDuration(untilTime)
					if err != nil {
						respBadStreamToBackend.WriteTo(netCon)
						continue
					}
				}
			}

			if until >= d {
				respBadStreamToBackend.WriteTo(netCon)
				continue
			}

			from := time.Now().Add(-d).Add(-1 * time.Second)
			recDone := 0

			for {

				// Don't process files newer than X minutes
				if from.After(time.Now().Add(-until)) {
					break
				}

				from = from.Add(1 * time.Second)
				path := srv.fullpath(from)

				files, err := ioutil.ReadDir(path)
				if err != nil {
					continue
				}

				// The idea of the next lines is read each log file and copy in one file,
				// trying to don't allocate the entire content in the memory.
				// The encoding to base64 use an stream and io.Copy to copy the content
				// from the log file to the base64 stream. In the same way the *WriteCompress will
				// compress all the content in a new file using an stream.
				wc, err := NewWriteCompress(srv.path(from), srv.s3Upload)
				if err != nil {
					redis.NewResp(err).WriteTo(netCon)
					continue
				}

				for _, file := range files {

					// Split the timestamp and the index name
					sf := strings.SplitN(file.Name(), "-", 2)

					f, err := os.Open(fmt.Sprintf("%s/%s", path, file.Name()))
					if err != nil {
						log.Printf("STREAM: Can't read the file %s: %s", file.Name(), err)
						continue
					}

					// Write the timestamp
					wc.Write([]byte(sf[0]))
					wc.Write(sep)
					// Write the ID ignoring the last bytes because is the file ext
					wc.Write([]byte(sf[1][:len(sf[1])-len(ext)-1]))
					wc.Write(sep)

					// Create the Base64 encoder with WriteCompress as a Reader
					encoder := base64.NewEncoder(base64.StdEncoding, wc)
					// Copy the content of the file (f) in the encoder
					if _, err := io.Copy(encoder, f); err != nil {
						log.Printf("ERROR Base64 encoder: %s", err)
						continue
					}
					// Close enconder
					encoder.Close()

					// Close file (f)
					f.Close()

					wc.Write(newLine)

					// Interal controller of WriteCompress to limit the size of the file
					if err := wc.Control(); err != nil {
						log.Printf("ERROR: Can't close the file: %s", err)
						break
					}
				}

				if err := wc.Close(); err != nil {
					redis.NewResp(err).WriteTo(netCon)
					continue
				}

				// Count total records
				recDone += wc.RecDone

				// for _, file := range files {
				// 	(os.Remove(fmt.Sprintf("%s/%s", path, file.Name()))
				// }
			}

			// Send OK response to the client
			redis.NewRespSimple(fmt.Sprintf("OK - %d records moved to S3", recDone)).WriteTo(netCon)

		default:
			log.Panicf("Invalid command: This never should happen, check the cases or the list of valid command")

		}

	}
}

func (srv *Server) s3Upload(name string, f *os.File) error {
	_, err := s3.New(srv.s3sess).PutObject(&s3.PutObjectInput{
		Bucket: &srv.config.S3Bucket,
		Key:    &name,
		Body:   f,
	})
	return err
}

func (srv *Server) fullpath(t time.Time) string {
	return fmt.Sprintf("%s/%d/%.2d/%.2d/%.2d/%.2d/%.2d", srv.config.Path, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func (srv *Server) path(t time.Time) string {
	return fmt.Sprintf("%d/%.2d/%.2d/%.2d/%.2d/%.2d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}
