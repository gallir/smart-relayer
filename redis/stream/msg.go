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

	recFound := false

	unixFound := false
	unix := make([]byte, 0)
	keyFound := false
	key := make([]byte, 0)

	for {

		buf := make([]byte, 1*1024)
		if _, err := r.Read(buf); err != nil {
			if recFound {
				if err == io.EOF {
					b.Write(buf)
					break
				}
				return nil, err
			}
			return nil, errors.New("Key not found")
		}

	S:
		// Check again "sep"
		if !unixFound {
			n := -1
			nb := byte('\n')
			for n, nb = range buf {
				if nb == sep[0] {
					unixFound = true
					break
				}
				unix = append(unix, nb)
			}
			if n > -1 {
				buf = buf[n+1:]
				//log.Printf("UNIX: %d |%s|: |%s|", n, string(unix), string(buf))
				if len(buf) == 0 {
					continue
				}
			} else {
				log.Printf("U == %s (%d)", string(buf), len(buf))
			}
		}

		if !keyFound {
			n := -1
			nb := byte('\n')
			for n, nb = range buf {
				if nb == sep[0] {
					keyFound = true
					//log.Printf("COMPARE: %s", string(buf))
					log.Printf("COMPARE: %d |%s| |%s| == |%s|", n, string(unix), string(key), m.k)
					if string(key) == m.k {
						recFound = true
					}
					break
				}
				key = append(key, nb)
			}
			if n > -1 {
				buf = buf[n+1:]
				//log.Printf("KEY: %d |%s| |%s|: |%s|", n, string(unix), string(key), string(buf))
				if len(buf) == 0 {
					continue
				}
			} else {
				log.Printf("K == %s (%d)", string(buf), len(buf))
			}
		}

		if bytes.Contains(buf, newLine) {
			i := bytes.IndexByte(buf, newLine[0])

			if recFound {
				last := buf[0:i]
				b.Write(last)
				//log.Printf("BREAK: %v |%s| |%s| |%s|", keyFound, unix, key, string(last))
				break
			}

			recFound = false
			unixFound = false
			unix = make([]byte, 0)
			keyFound = false
			key = make([]byte, 0)

			buf = buf[i+1:]
			//log.Printf("N: %v |%s| |%s| |%s|", recFound, string(unix), string(key), string(buf[len(buf)-100:len(buf)]))
			goto S
		}

		if recFound {
			b.Write(buf)
		}
	}

	//log.Printf("S: %s", string(b.B[0:1024]))
	//log.Printf("E: %s", string(b.B[b.Len()-1024:b.Len()]))
	//log.Printf("L: %d", b.Len())
	return base64.StdEncoding.DecodeString(b.String())
}
