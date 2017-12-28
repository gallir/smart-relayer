package stream

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/gallir/bytebufferpool"
	"github.com/gallir/radix.improved/redis"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	msgBytesPool = &bytebufferpool.Pool{}
	msgPool      = &sync.Pool{
		New: func() interface{} {
			return &Msg{}
		},
	}
)

const (
	ext       = "log"
	indexName = "index.idx"
)

func getMsg(base *string) *Msg {
	m := msgPool.Get().(*Msg)
	m.t = time.Now()
	m.b = msgBytesPool.Get()
	m.base = base
	return m
}

func putMsg(m *Msg) {
	msgBytesPool.Put(m.b)
	m.b = nil
	m.k = ""
	m.base = nil
	msgPool.Put(m)
}

type Msg struct {
	t    time.Time
	k    string
	b    *bytebufferpool.ByteBuffer
	base *string
	s3   bool
}

func (m *Msg) path() string {
	return fmt.Sprintf("%s/%d/%d/%d/%d/%d", *m.base, m.t.Year(), m.t.Month(), m.t.Day(), m.t.Hour(), m.t.Minute())
}

func (m *Msg) filename() string {
	return fmt.Sprintf("%d-%s.%s", m.t.Unix(), m.k, ext)
}

func (m *Msg) index() string {
	return fmt.Sprintf("%d/%d/%d/%d/%s", m.t.Year(), m.t.Month(), m.t.Day(), m.t.Hour(), indexName)
}

func (m *Msg) indexFile() string {
	return fmt.Sprintf("%s/%s", *m.base, m.index())
}

func (m *Msg) parse(r []*redis.Resp) (err error) {

	// Read first argument to be parsed as Int64
	var i int64
	i, err = r[1].Int64()
	if err != nil {
		return err
	}

	// Convert int64 to Time
	m.t = time.Unix(i, 0)

	// Read and store the string key
	if m.k, err = r[2].Str(); err != nil {
		return err
	}

	return nil
}

func (m *Msg) Bytes() (b []byte, err error) {

	if b, err = m.bytesFile(); err == nil {
		return b, err
	}

	b, err = m.bytesS3()
	return b, err
}

func (m *Msg) bytesFile() ([]byte, error) {
	// Build the path + filename
	filename := fmt.Sprintf("%s/%s", m.path(), m.filename())

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)

	return b, nil
}

func (m *Msg) bytesS3() ([]byte, error) {
	// Specify profile to load for the session's config
	sess, _ := session.NewSessionWithOptions(session.Options{
		Profile: "dotw",
		Config: aws.Config{
			Region: aws.String("eu-west-1"),
		},
	})

	return m.findIndex(sess)
}

func (m *Msg) findIndex(sess *session.Session) ([]byte, error) {
	downloader := s3manager.NewDownloader(sess)

	file, err := os.OpenFile(m.indexFile(), os.O_CREATE, 0444)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String("dotw-testing-nfs-logs"), // Required
			Key:    aws.String(m.index()),               // Required
		})
	if err != nil {
		return nil, err
	}

	file.Seek(0, 0)

	var b []byte
	b, err = ioutil.ReadAll(file)

	return b, err
}
