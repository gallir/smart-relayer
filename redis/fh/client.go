package fh

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout = 15 * time.Second
	maxRecordSize  = 1000000 // The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB
)

var (
	clientCount int64 = 0
	buffPool          = &bytebufferpool.Pool{}
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	records     []*record
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

	for {
		select {
		case r := <-clt.srv.recordsCh:
			// ignore empty messages
			if r.len() <= 0 {
				continue
			}

			clt.records = append(clt.records, r)
			if len(clt.records) >= clt.srv.config.MaxRecords {
				clt.flush()
			}
		case <-clt.tick.C:
			if time.Since(clt.lastFlushed) >= recordsTimeout {
				clt.flush()
			}
		case <-clt.done:
			lib.Debugf("Firehose client %d: closing..", clt.ID)
			clt.finish <- true
			return
		}
	}

}

func (clt *Client) flush() {
	clt.lastFlushed = time.Now()

	if len(clt.records) <= 0 {
		return
	}

	records := make([]*firehose.Record, 0)

	b := buffPool.Get()

	for _, r := range clt.records {
		if r.len()+b.Len()+1 >= maxRecordSize {
			bs := append([]byte(nil), b.Bytes()...)
			buffPool.Put(b)
			b = buffPool.Get()
			records = append(records, &firehose.Record{Data: bs})
		}

		b.Write(r.bytes())
		b.Write([]byte("\n"))
	}

	if b.Len() > 0 {
		bs := append([]byte(nil), b.Bytes()...)

		buffPool.Put(b)
		records = append(records, &firehose.Record{Data: bs})
	}

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

	lib.Debugf("Firehose: sent %d", len(records))

	for _, r := range clt.records {
		putPool(r)
	}
	clt.records = nil
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	defer lib.Debugf("Firehose client %d: Exit, %d records lost", clt.ID, len(clt.records))

	clt.done <- true
	<-clt.finish

	clt.flush()
}
