package fh

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout  = 15 * time.Second
	maxRecordSize   = 1000 * 1024     // The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB
	maxBatchRecords = 500             // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize    = 4 * 1024 * 1024 // 4 MB per call
)

var (
	clientCount int64 = 0
	newLine           = []byte("\n")
)

var pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0)
	},
}

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	buff        []byte
	count       int
	batch       []*firehose.Record
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
		lib.Debugf("Firehose client %d ERROR: reload %s", clt.ID, err)
		return nil
	}

	lib.Debugf("Firehose client %d ready", clt.ID)

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
	defer lib.Debugf("Firehose client %d: Closed listener", clt.ID)

	clt.status = 1

	clt.buff = pool.Get().([]byte)

	for {

		select {
		case r := <-clt.srv.recordsCh:
			// ignore empty messages
			if r.len() <= 0 {
				continue
			}

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			if clt.count >= clt.srv.config.MaxRecords || clt.batchSize+r.len()+1 >= maxBatchSize || len(clt.batch)+1 >= maxBatchRecords {
				// Force flush
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if len(clt.buff)+r.len()+1 >= maxRecordSize || clt.count+1 >= clt.srv.config.MaxRecords {
				// Save in new record
				clt.appendRecord(clt.buff)
				clt.buff = pool.Get().([]byte)
			}

			clt.buff = append(clt.buff, r.bytes()...)
			clt.buff = append(clt.buff, newLine...)

			clt.count++
			clt.batchSize += len(clt.buff)

		case <-clt.tick.C:
			if time.Since(clt.lastFlushed) >= recordsTimeout && (len(clt.buff) > 0 || len(clt.batch) > 0) {
				if len(clt.buff) > 0 {
					clt.appendRecord(clt.buff)
					clt.buff = pool.Get().([]byte)
				}
				clt.flush()
			}
		case <-clt.done:
			lib.Debugf("Firehose client %d: closing..", clt.ID)
			clt.finish <- true
			return
		}
	}
}

// appendRecord store the current buffer in a new record and append it to the batch
func (clt *Client) appendRecord(buff []byte) {
	if len(buff) <= 0 {
		// Don't create empty records
		return
	}

	clt.batch = append(clt.batch, &firehose.Record{Data: buff})
}

// flush build the last record if need and send the records slice to AWS Firehose
func (clt *Client) flush() {
	clt.lastFlushed = time.Now()

	// Don't send empty batch
	if len(clt.batch) == 0 {
		return
	}

	// Send the batch to AWS Firehose
	clt.putRecordBatch(clt.batch)

	// Put slice in the pull after sent to AWS Firehose
	for _, r := range clt.batch {
		//fmt.Println(string(r.Data))
		r.Data = r.Data[:0]
		pool.Put(r.Data)
	}

	clt.batchSize = 0
	clt.batch = nil
	clt.count = 0
}

// putRecordBatch is the client connection to AWS Firehose
func (clt *Client) putRecordBatch(records []*firehose.Record) {
	req, _ := clt.srv.awsSvc.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.config.StreamName),
		Records:            records,
	})

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	req.SetContext(ctx)

	err := req.Send()
	if err != nil {
		if req.IsErrorThrottle() {
			lib.Debugf("Firehose client %d: ERROR IsErrorThrottle: %s", clt.ID, err)
		} else {
			lib.Debugf("Firehose client %d: ERROR PutRecordBatch->Send: %s", clt.ID, err)
		}
		clt.srv.failure()
		return
	}

	lib.Debugf("Firehose client %d: sent batch with %d records, %d lines, %d bytes", clt.ID, len(records), clt.count, clt.batchSize)
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	defer lib.Debugf("Firehose client %d: Exit, %d records lost", clt.ID, len(clt.batch))

	clt.done <- true
	<-clt.finish

	if len(clt.buff) > 0 {
		clt.appendRecord(clt.buff)
	}

	clt.flush()
}
