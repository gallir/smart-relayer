package stream

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gallir/bytebufferpool"
	"github.com/gallir/radix.improved/redis"

	"github.com/aws/aws-sdk-go/aws"
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
	ext = "log"
)

func getMsg(srv *Server) *Msg {
	m := msgPool.Get().(*Msg)
	m.t = time.Now()
	m.b = msgBytesPool.Get()
	m.srv = srv
	return m
}

func putMsg(m *Msg) {
	msgBytesPool.Put(m.b)
	m.b = nil
	m.k = ""
	msgPool.Put(m)
}

type Msg struct {
	t   time.Time
	k   string
	b   *bytebufferpool.ByteBuffer
	srv *Server
}

func (m *Msg) fullpath() string {
	return m.srv.fullpath(m.t)
}

func (m *Msg) path() string {
	return m.srv.path(m.t)
}

func (m *Msg) filename() string {
	return fmt.Sprintf("%d-%s.%s", m.t.Unix(), m.k, ext)
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

	// if b, err = m.bytesFile(); err == nil {
	// 	return b, err
	// }

	b, err = m.bytesS3()
	return b, err
}

func (m *Msg) bytesFile() ([]byte, error) {
	// Build the path + filename
	filename := fmt.Sprintf("%s/%s", m.fullpath(), m.filename())

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)

	return b, nil
}

func (m *Msg) bytesS3() ([]byte, error) {

	downloader := s3manager.NewDownloader(m.srv.s3sess)

	file, err := ioutil.TempFile("", "logs-download")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: &m.srv.config.S3Bucket,                               // Required
			Key:    aws.String(fmt.Sprintf("%s/records-1.gz", m.path())), // Required
		})
	if err != nil {
		return nil, err
	}

	file.Seek(0, 0)

	r, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	b := bytebufferpool.Get()
	buf := make([]byte, 4096)
	keyFound := false

	unix := ""
	key := ""

	for {
		if _, err := r.Read(buf); err != nil {
			if keyFound {
				if err == io.EOF {
					b.Write(buf)
					break
				}
				return nil, err
			}
			return nil, errors.New("Key not found")
		}

		// Check new line
	N:
		if bytes.Contains(buf, newLine) {
			i := bytes.IndexByte(buf, newLine[0])
			if keyFound {
				b.Write(buf[0:i])
				break
			}
			buf = buf[i+1:]

			// Go to check "newLine" again
			goto N
		}

		// Check again "sep"
	S:
		if bytes.Contains(buf, sep) && !keyFound {

			iu := bytes.IndexByte(buf, sep[0])
			if unix == "" && key == "" {
				unix = string(buf[0:iu])
			} else if unix != "" && key == "" {
				key = string(buf[0:iu])
			}
			buf = buf[iu+1:]

			if unix != "" && key != "" {

				if key == m.k {
					keyFound = true
				}

				unix = ""
				key = ""
				// Go to check "newLine" again
				goto N
			}

			// Go to check "sep" again
			goto S
		}

		if keyFound {
			b.Write(buf)
		}
	}

	log.Printf("D: %s", string(b.B[8044053-1024:8044053+1024]))
	log.Printf("D: %d", b.Len())
	return base64.StdEncoding.DecodeString(b.String())
}
