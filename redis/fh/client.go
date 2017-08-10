package fh

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/smart-relayer/lib"
)

const (
	recordsTimeout = 1 * time.Second
	connectRetry   = 5 * time.Second
	maxFailures    = 5
	windowFailures = 10 * time.Second
)

var (
	clientCount int64 = 0
)

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	config    lib.RelayerConfig
	mode      int
	awsSess   *session.Session
	awsSvc    *firehose.Firehose
	records   []record
	recordsCh chan record

	exiting     bool
	finish      chan bool
	done        chan bool
	lastFailure time.Time
	failures    int
	ID          int
}

// NewClient creates a new client that connect to a Redis server
func NewClient(c *lib.RelayerConfig, ch chan record) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:      make(chan bool),
		finish:    make(chan bool),
		exiting:   false,
		recordsCh: ch,
		ID:        int(n),
	}

	if err := clt.Reload(c); err != nil {
		clt.log("Error on reload firehose client:", err)
		return nil
	}

	clt.debug("Client %s for target %s ready", clt.config.Listen, clt.config.Host())

	return clt
}

func (clt *Client) listen() {
	defer clt.debug("[%d] Closed listener", clt.ID)

	for {
		select {
		case r := <-clt.recordsCh:
			// ignore empty messages
			if r.Len() <= 0 {
				continue
			}

			clt.records = append(clt.records, r)
			if len(clt.records) >= clt.config.MaxRecords {
				clt.flush()
			}
		case <-time.Tick(recordsTimeout):
			clt.flush()
		case <-clt.done:
			clt.debug("[%d] Closing listener", clt.ID)
			clt.finish <- true
			return
		}
	}

}

func (clt *Client) Reload(c *lib.RelayerConfig) error {
	clt.Lock()
	defer clt.Unlock()

	clt.config = *c

	var err error

	if clt.config.Profile != "" {
		clt.awsSess, err = session.NewSessionWithOptions(session.Options{Profile: clt.config.Profile})
	} else {
		clt.awsSess, err = session.NewSession()
	}

	if err != nil {
		clt.log("Error session: %s", err)
		clt.failure()
		return err
	}

	clt.awsSvc = firehose.New(clt.awsSess, &aws.Config{Region: aws.String(clt.config.Region)})

	stream := &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: &clt.config.StreamName,
	}

	l, err := clt.awsSvc.DescribeDeliveryStream(stream)
	if err != nil {
		clt.log("ERROR connecting to kinesis/firehost: %s", err)
		clt.failure()
		return err
	}

	clt.debug("Connected to kinesis/firehost deliver to %s (%s) status %s",
		*l.DeliveryStreamDescription.DeliveryStreamName,
		*l.DeliveryStreamDescription.DeliveryStreamARN,
		*l.DeliveryStreamDescription.DeliveryStreamStatus)

	clt.exiting = false
	clt.failures = 0

	go clt.listen()

	return nil
}

func (clt *Client) failure() {

	if time.Now().Sub(clt.lastFailure) > windowFailures {
		clt.debug("reset failures %s : %s -> %s", time.Now(), time.Now().Sub(clt.lastFailure), clt.lastFailure)
		clt.failures = 0
	}

	clt.failures++
	clt.lastFailure = time.Now()

	if clt.failures >= maxFailures {
		clt.debug("Over failures limit")
		go func() {
			clt.Exit()
			<-time.After(time.Second * 2)
			clt.Reload(&clt.config)
		}()
		return
	}

}

func (clt *Client) Exit() {

	if clt.exiting {
		log.Println("Duplicate call to Exit")
		return
	}

	clt.debug("Exit")

	clt.exiting = true

	clt.debug("Done")
	clt.done <- true
	clt.debug("Finish")
	<-clt.finish
	clt.debug("End")

	// send messages to firehose just if the connection is fine
	if clt.failures > maxFailures {
		clt.flush()
	}

	clt.debug("Exit Firehose client")
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

	clt.records = nil

	pr := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.config.StreamName),
		Records:            records,
	}

	o, errPut := clt.awsSvc.PutRecordBatch(pr)
	if errPut != nil {
		clt.log("Error PutRecordBatch: %s", errPut)
		clt.failure()
		return
	}

	if *o.FailedPutCount > 0 {
		clt.log("ERROR: Failed %d records", *o.FailedPutCount)
		clt.failure()
		return
	}

	clt.debug("Records sent: %d", len(o.RequestResponses))
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
