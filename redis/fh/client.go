package fh

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout = 1 * time.Second
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
	}

	if err := clt.Reload(); err != nil {
		clt.log("Error on reload firehose client:", err)
		return nil
	}

	clt.debug("Firehose client ready")

	return clt
}

func (clt *Client) listen() {
	defer clt.debug("[%d] Closed listener", clt.ID)
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
		case <-time.Tick(recordsTimeout):
			clt.flush()
		case <-clt.done:
			clt.debug("Closing listener %d", clt.ID)
			clt.finish <- true
			return
		}
	}

}

func (clt *Client) Reload() error {
	clt.Lock()
	defer clt.Unlock()

	if clt.status > 0 {
		clt.Exit()
	}

	go clt.listen()

	return nil
}

func (clt *Client) Exit() {
	clt.debug("exiting..")

	clt.done <- true

	clt.debug("exiting.. chan1")
	<-clt.finish
	clt.debug("exiting.. chan2")

	clt.debug("Exit Firehose client, pending records %d", len(clt.records))
	clt.flush()
}

func (clt *Client) flush() {
	if len(clt.records) <= 0 {
		return
	}

	records := make([]*firehose.Record, 0, len(clt.records))
	for _, r := range clt.records {
		records = append(records, &firehose.Record{Data: r.Bytes()})
		putRecord(r)
	}

	req, resp := clt.srv.awsSvc.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.config.StreamName),
		Records:            records,
	})

	ctx := context.Background()
	var cancelFn func()
	ctx, cancelFn = context.WithTimeout(ctx, 2*time.Second)
	defer cancelFn()

	req.SetContext(ctx)

	errPut := req.Send()
	if errPut != nil {
		clt.log("Error PutRecordBatch: %s", errPut)
		clt.srv.failure()
		return
	}

	if *resp.FailedPutCount > 0 {
		clt.log("ERROR: Failed %d records", *resp.FailedPutCount)
	}

	clt.records = nil
	clt.debug("Records sent: %d", len(resp.RequestResponses))
}

func (clt *Client) debug(format string, v ...interface{}) {
	format = fmt.Sprint("[fh %d] ", format)
	v = append([]interface{}{clt.ID}, v...)
	lib.Debugf(format, v...)
}

func (clt *Client) log(format string, v ...interface{}) {
	format = fmt.Sprint("[fh %d] ", format)
	v = append([]interface{}{clt.ID}, v...)
	log.Printf(format, v...)
}
