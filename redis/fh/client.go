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
	recordsTimeout = 15 * time.Second
)

var (
	clientCount int64 = 0
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv     *Server
	mode    int
	records []record
	status  int
	finish  chan bool
	done    chan bool
	ID      int
	tick    *time.Ticker
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
			if r.Len() <= 0 {
				continue
			}

			clt.records = append(clt.records, r)
			if len(clt.records) >= clt.srv.config.MaxRecords {
				clt.flush()
			}
		case <-clt.tick.C:
			clt.flush()
		case <-clt.done:
			lib.Debugf("Firehose client %d: closing..", clt.ID)
			clt.finish <- true
			return
		}
	}

}

func (clt *Client) flush() {
	if len(clt.records) <= 0 {
		return
	}

	defer func() {
		for _, r := range clt.records {
			putRecord(r)
		}
		clt.records = nil
	}()

	records := make([]*firehose.Record, 0, len(clt.records))
	for _, r := range clt.records {
		b := append(r.Bytes(), []byte("\n")...)
		records = append(records, &firehose.Record{Data: b})
	}

	req, resp := clt.srv.awsSvc.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.config.StreamName),
		Records:            records,
	})

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	req.SetContext(ctx)

	errPut := req.Send()
	if errPut != nil {
		if req.IsErrorThrottle() {
			lib.Debugf("Firehose client %d: ERROR IsErrorThrottle")
		}

		lib.Debugf("Firehose client %d: ERROR PutRecordBatch->Send %s", clt.ID, errPut)
		clt.srv.failure()
		return
	}

	if *resp.FailedPutCount > 0 {
		lib.Debugf("Firehose client %d: ERROR PutRecordBatch->FailedPutCount %d", clt.ID, *resp.FailedPutCount)
	}

	lib.Debugf("Firehose: sent %d", len(records))
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	defer lib.Debugf("Firehose client %d: Exit, %d records lost", clt.ID, len(clt.records))

	clt.done <- true
	<-clt.finish

	clt.flush()
}
