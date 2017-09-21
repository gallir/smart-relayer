package rsqs

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout  = 2 * time.Second // Maximum time after send a batch
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

			s, id := r.StringUniqID()

			// The maximum is 262,144 bytes (256 KB)
			if len(s) >= maxRecordSize {
				// Save in new record
				log.Printf("SQS ERROR: the message is over %dKB can't be send", maxRecordSize/1024)
				continue
			}

			m := &sqs.SendMessageBatchRequestEntry{}
			m.SetId(id)
			m.SetMessageBody(s)
			if clt.srv.fifo {
				m.SetMessageGroupId(clt.srv.config.GroupID)
			}
			clt.batch = append(clt.batch, m)

			clt.batchSize += len(s)

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

	req, output := clt.srv.awsSvc.SendMessageBatchRequest(s)
	req.SetContext(ctx)
	if err := req.Send(); err != nil {
		log.Printf("SQS Send ERROR: %s", err)
		return
	}

	if len(output.Failed) > 0 {
		log.Printf("SQS client %d ERROR: sent batch with %d records, %d bytes, %d failed: %s - %s",
			clt.ID, len(clt.batch), clt.batchSize, len(output.Failed), *output.Failed[0].Code, *output.Failed[0].Message)
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
