package rsqs

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout  = 1 * time.Second // Maximum time after send a batch
	maxRecordSize   = 262144          // The maximum is 262,144 bytes (256 KB).
	maxBatchRecords = 10              // A single message batch request can include a maximum of 10 messages.
)

var (
	clientCount int64 = 0
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	buff        []byte
	count       int
	batch       []*sqs.SendMessageBatchRequestEntry
	batchSize   int
	status      int
	finish      chan bool
	done        chan bool
	ID          int
	tick        *time.Ticker
	lastFlushed time.Time
}

// NewClient creates a new client that connect to a Redis server
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:   make(chan bool),
		finish: make(chan bool),
		status: 0,
		srv:    srv,
		ID:     int(n),
		tick:   time.NewTicker(recordsTimeout),
	}

	if err := clt.Reload(); err != nil {
		lib.Debugf("SQS client %d ERROR: reload %s", clt.ID, err)
		return nil
	}

	lib.Debugf("SQS client %d ready", clt.ID)

	return clt
}

// Reload finish the listener and run it again
func (clt *Client) Reload() error {
	clt.Lock()
	defer clt.Unlock()

	if clt.status > 0 {
		clt.Exit()
	}

	go clt.listen()

	return nil
}

func (clt *Client) listen() {
	defer lib.Debugf("SQS client %d: Closed listener", clt.ID)

	clt.status = 1

	for {

		select {
		case r := <-clt.srv.recordsCh:
			// ignore empty messages
			if r.Len() <= 0 {
				continue
			}

			// Limits control
			if len(clt.batch) >= clt.srv.config.MaxRecords {
				// Force flush
				clt.flush()
			}

			// The maximum is 262,144 bytes (256 KB)
			if clt.batchSize+1 >= maxRecordSize {
				// Save in new record
				fmt.Println(r.Bytes())
				continue
			}

			h := murmur3.New128()
			h.Write(r.Bytes())
			h1, h2 := h.Sum128()
			hId := fmt.Sprintf("%v-%v", h1, h2)

			m := &sqs.SendMessageBatchRequestEntry{}
			m.SetMessageBody(string(r.Bytes()))
			m.SetId(string(hId))
			m.SetMessageGroupId(clt.srv.config.URL)
			clt.batch = append(clt.batch, m)

			clt.batchSize += r.Len()

		case <-clt.tick.C:
			if time.Since(clt.lastFlushed) >= recordsTimeout && (len(clt.buff) > 0 || len(clt.batch) > 0) {
				clt.flush()
			}
		case <-clt.done:
			lib.Debugf("SQS client %d: closing..", clt.ID)
			clt.finish <- true
			return
		}
	}
}

// flush build the last record if need and send the records slice to AWS SQS
func (clt *Client) flush() {
	clt.lastFlushed = time.Now()

	// Don't send empty batch
	if len(clt.batch) == 0 {
		return
	}

	clt.putRecordBatch()

	clt.batchSize = 0
	clt.batch = nil
	clt.count = 0
}

// putRecordBatch is the client connection to AWS SQS
func (clt *Client) putRecordBatch() {

	s := &sqs.SendMessageBatchInput{
		QueueUrl: &clt.srv.config.URL,
		Entries:  clt.batch,
	}

	if err := s.Validate(); err != nil {
		log.Printf("SQS Validate ERROR: %s", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	req, _ := clt.srv.awsSvc.SendMessageBatchRequest(s)

	req.SetContext(ctx)
	if err := req.Send(); err != nil {
		log.Printf("SQS Send ERROR: %s", err)
		return
	}

	lib.Debugf("SQS client %d: sent batch with %d records, %d bytes", clt.ID, len(clt.batch), clt.batchSize)
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	defer lib.Debugf("SQS client %d: Exit, %d records lost", clt.ID, len(clt.batch))

	clt.done <- true
	<-clt.finish

	clt.flush()
}
