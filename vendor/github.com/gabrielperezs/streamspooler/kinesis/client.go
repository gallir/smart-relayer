package kinesisPool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/redis"
)

const (
	recordsTimeout     = 15 * time.Second
	maxRecordSize      = 1000 * 1024     // The maximum size of a record sent to Kinesis Kinesis, before base64-encoding, is 1000 KB
	maxBatchRecords    = 500             // The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
	maxBatchSize       = 4 * 1024 * 1024 // 4 MB per call
	partialFailureWait = 200 * time.Millisecond
	totalFailureWait   = 500 * time.Millisecond

	kinesisError = "InternalFailure"
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
	records     []*kinesis.PutRecordsRequestEntry
	done        chan bool
	finish      chan bool
	ID          int64
	t           *time.Timer
	lastFlushed time.Time
}

// NewClient creates a new client that connects to a kinesis
func NewClient(srv *Server) *Client {
	n := atomic.AddInt64(&clientCount, 1)

	clt := &Client{
		done:    make(chan bool),
		finish:  make(chan bool),
		srv:     srv,
		ID:      n,
		t:       time.NewTimer(recordsTimeout),
		batch:   make([]*bytebufferpool.ByteBuffer, 0, maxBatchRecords),
		records: make([]*kinesis.PutRecordsRequestEntry, 0, maxBatchRecords),
		buff:    pool.Get(),
	}

	go clt.listen()

	return clt
}

func (clt *Client) listen() {
	log.Printf("Kinesis client %s [%d]: ready", clt.srv.cfg.StreamName, clt.ID)
	for {

		select {
		case ri := <-clt.srv.C:

			var r []byte
			if clt.srv.cfg.Serializer != nil {
				var err error
				if r, err = clt.srv.cfg.Serializer(ri); err != nil {
					log.Printf("Kinesis client %s [%d]: ERROR serializer: %s", clt.srv.cfg.StreamName, clt.ID, err)
					continue
				}
			} else {
				r = ri.([]byte)
			}

			recordSize := len(r)

			if recordSize <= 0 {
				continue
			}

			if clt.srv.cfg.Compress {
				// All the message will be compress. This will work with raw and json messages.
				r = compress.Bytes(r)
				// Update the record size using the compression []byte result
				recordSize = len(r)
			}

			if recordSize > maxRecordSize {
				log.Printf("Kinesis client %s [%d]: ERROR: one record is over the limit %d/%d", clt.srv.cfg.StreamName, clt.ID, recordSize, maxRecordSize)
				continue
			}

			clt.count++

			// The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller. This limit cannot be changed.
			if clt.count >= clt.srv.cfg.MaxRecords || len(clt.batch) >= maxBatchRecords || clt.batchSize+recordSize+1 >= maxBatchSize {
				// log.Printf("flush: count %d/%d | batch %d/%d | size [%d] %d/%d",
				// 	clt.count, clt.srv.cfg.MaxRecords, len(clt.batch), maxBatchRecords, recordSize, (clt.batchSize+recordSize+1)/1024, maxBatchSize/1024)
				// Force flush
				clt.flush()
			}

			// The maximum size of a record sent to Kinesis Kinesis, before base64-encoding, is 1000 KB.
			if !clt.srv.cfg.ConcatRecords || clt.buff.Len()+recordSize+1 >= maxRecordSize || clt.count >= clt.srv.cfg.MaxRecords {
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
				log.Printf("Kinesis client %s [%d]: Exit, %d records lost", clt.srv.cfg.StreamName, clt.ID, l)
				return
			}

			log.Printf("Kinesis client %s [%d]: Exit", clt.srv.cfg.StreamName, clt.ID)
			clt.done <- true
			return
		}
	}
}

// flush build the last record if need and send the records slice to AWS Kinesis
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

	// Create slice with the struct need by Kinesis
	for _, b := range clt.batch {
		m1 := murmur3.Sum64(b.B)
		clt.records = append(clt.records, &kinesis.PutRecordsRequestEntry{
			Data:         b.B,
			PartitionKey: aws.String(fmt.Sprintf("%02x", m1)),
		})
	}

	// Add context timeout to the request
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	// Create the request
	output, err := clt.srv.awsSvc.PutRecordsWithContext(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(clt.srv.cfg.StreamName),
		Records:    clt.records,
	})

	if err != nil {
		log.Printf("Kinesis client %s [%d]: ERROR PutRecordsWithContext: %s", clt.srv.cfg.StreamName, clt.ID, err)
		clt.srv.failure()
		time.Sleep(totalFailureWait)

		for i := range clt.batch {
			go func(b []byte) {
				clt.srv.C <- b
			}(append([]byte(nil), clt.batch[i].B...))
		}
	} else if *output.FailedRecordCount > 0 {
		log.Printf("Kinesis client %s [%d]: partial failed, %d sent back to the buffer", clt.srv.cfg.StreamName, clt.ID, *output.FailedRecordCount)
		// Sleep few millisecond because the partial failure
		time.Sleep(partialFailureWait)
		// From oficial package comments:
		//
		// PutRecords results.
		// Please also see https://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecordsOutput
		// An array of successfully and unsuccessfully processed record results, correlated
		// with the request by natural ordering. A record that is successfully added
		// to a stream includes SequenceNumber and ShardId in the result. A record that
		// fails to be added to a stream includes ErrorCode and ErrorMessage in the
		// result.
		for i, r := range output.Records {
			if r == nil || r.ErrorCode != nil {
				continue
			}

			if *r.ErrorCode == kinesisError {
				log.Printf("Kinesis client %s [%d]: ERROR in AWS: %s - %s", clt.srv.cfg.StreamName, clt.ID, *r.ErrorCode, *r.ErrorMessage)
			}
			// Every message with error code means that message wasn't stored by Kinesis
			// stream. We send back to the main channel every failed message. To be sure
			// that we don't have problems with sync.pool the slice of bytes are copied
			// and send to the main channel in a goroutine in order to don't block the
			// operation if the channel is full.
			go func(b []byte) {
				clt.srv.C <- b
			}(append([]byte(nil), clt.batch[i].B...))

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
