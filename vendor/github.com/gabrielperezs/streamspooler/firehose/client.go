package firehosePool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/redis"
)

const (
	recordsTimeout  = 15 * time.Second
	maxRecordSize   = 1000 * 1024     // The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB
	maxBatchRecords = 500             // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize    = 4 * 1024 * 1024 // 4 MB per call
	failureWait     = 1 * time.Second
	failureMaxTries = 3
)

var (
	clientCount int64
	newLine     = []byte("\n")
)

var pool = &bytebufferpool.Pool{}

// Client is the thread that connect to the remote redis server
type Client struct {
	sync.Mutex
	srv         *Server
	mode        int
	buff        *bytebufferpool.ByteBuffer
	count       int
	batch       []*bytebufferpool.ByteBuffer
	batchSize   int
	records     []*firehose.Record
	done        chan bool
	finish      chan bool
	ID          int64
	t           *time.Timer
	lastFlushed time.Time
}

// NewClient creates a new client that connects to a Firehose
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:    make(chan bool),
		finish:  make(chan bool),
		srv:     srv,
		ID:      n,
		t:       time.NewTimer(recordsTimeout),
		batch:   make([]*bytebufferpool.ByteBuffer, 0, maxBatchRecords),
		records: make([]*firehose.Record, 0, maxBatchRecords),
		buff:    pool.Get(),
	}

	go clt.listen()

	return clt
}

func (clt *Client) listen() {
	log.Printf("Firehose client %s [%d]: ready", clt.srv.cfg.StreamName, clt.ID)
	for {

		select {
		case ri := <-clt.srv.C:

			var r []byte
			if clt.srv.cfg.Serializer != nil {
				var err error
				if r, err = clt.srv.cfg.Serializer(ri); err != nil {
					log.Printf("Firehose client %s [%d]: ERROR serializer: %s", clt.srv.cfg.StreamName, clt.ID, err)
					continue
				}
			} else {
				r = ri.([]byte)
			}

			recordSize := len(r)

			if recordSize <= 0 {
				continue
			}

			clt.count++

			if clt.srv.cfg.Compress {
				// All the message will be compress. This will work with raw and json messages.
				r = compress.Bytes(r)
				// Update the record size using the compression []byte result
				recordSize = len(r)
			}

			if recordSize+1 >= maxRecordSize {
				log.Printf("Firehose client %s [%d]: ERROR: one record is over the limit %d/%d", clt.srv.cfg.StreamName, clt.ID, recordSize, maxRecordSize)
				continue
			}

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch)+1 > maxBatchRecords || clt.batchSize+recordSize+1 >= maxBatchSize {
				// log.Printf("flush: count %d/%d | batch %d/%d | size [%d] %d/%d",
				// 	clt.count, clt.srv.cfg.MaxRecords, len(clt.batch), maxBatchRecords, recordSize, (clt.batchSize+recordSize+1)/1024, maxBatchSize/1024)
				// Force flush
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Firehose, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.count+1 > clt.srv.cfg.MaxRecords {
				if clt.buff.Len() > 0 {
					// Save in new record
					buff := clt.buff
					clt.batch = append(clt.batch, buff)
					clt.buff = pool.Get()
				}
			}

			clt.buff.Write(r)
			clt.buff.Write(newLine)

			clt.batchSize += clt.buff.Len()

		case <-clt.t.C:
			if clt.buff.Len() > 0 {
				buff := clt.buff
				clt.batch = append(clt.batch, buff)
				clt.buff = pool.Get()
			}
			clt.flush()

		case <-clt.finish:
			//Stop and drain the timer channel
			if !clt.t.Stop() {
				select {
				case <-clt.t.C:
				default:
				}
			}

			if clt.buff.Len() > 0 {
				clt.batch = append(clt.batch, clt.buff)
			}

			clt.flush()
			if l := len(clt.batch); l > 0 {
				log.Printf("Firehose client %s [%d]: Exit, %d records lost", clt.srv.cfg.StreamName, clt.ID, l)
				return
			}

			log.Printf("Firehose client %s [%d]: Exit", clt.srv.cfg.StreamName, clt.ID)
			clt.done <- true
			return
		}
	}
}

// flush build the last record if need and send the records slice to AWS Firehose
func (clt *Client) flush() {

	if !clt.t.Stop() {
		select {
		case <-clt.t.C:
		default:
		}
	}
	clt.t.Reset(recordsTimeout)

	size := len(clt.batch)
	// Don't send empty batch
	if size == 0 {
		return
	}

	// Create slice with the struct need by firehose
	for _, b := range clt.batch {
		clt.records = append(clt.records, &firehose.Record{Data: b.B})
	}

	// Create the request
	req, _ := clt.srv.awsSvc.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(clt.srv.cfg.StreamName),
		Records:            clt.records,
	})

	// Add context timeout to the request
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	req.SetContext(ctx)

	// Send the request
	err := req.Send()
	if err != nil {
		if req.IsErrorThrottle() {
			log.Printf("Firehose client %s [%d]: ERROR IsErrorThrottle: %s", clt.srv.cfg.StreamName, clt.ID, err)
		} else {
			log.Printf("Firehose client %s [%d]: ERROR PutRecordBatch->Send: %s", clt.srv.cfg.StreamName, clt.ID, err)
		}
		clt.srv.failure()

		// Finish if is not critical stream
		if clt.srv.cfg.Critical {
			log.Printf("Firehose client %s [%d]: ERROR Critical records lost, %d messages lost", clt.srv.cfg.StreamName, clt.ID, size)
		}
	}

	// Put slice bytes in the pull after sent
	for _, b := range clt.batch {
		pool.Put(b)
	}

	clt.batchSize = 0
	clt.count = 0
	clt.batch = nil
	clt.records = nil
}

// Exit finish the go routine of the client
func (clt *Client) Exit() {
	clt.finish <- true
	<-clt.done
}
