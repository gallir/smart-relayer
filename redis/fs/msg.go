package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/gallir/bytebufferpool"
	"github.com/gallir/smart-relayer/redis/fs/ifaceS3"
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
	m.b = msgBytesPool.Get()

	m.srv = srv
	return m
}

func putMsg(m *Msg) {

	msgBytesPool.Put(m.b)
	m.b = nil

	m.k = ""
	m.project = ""
	m.t = time.Now()

	msgPool.Put(m)
}

type Msg struct {
	project string
	k       string
	t       time.Time
	b       *bytebufferpool.ByteBuffer
	srv     *Server
}

func (m *Msg) fullpath() string {
	t := m.t
	return m.srv.fullpath(m.project, t)
}

func (m *Msg) path() string {
	t := m.t
	return m.srv.path(m.project, t)
}

func (m *Msg) hourpath() string {
	t := m.t
	return m.srv.hourpath(m.project, t)
}

func (m *Msg) filename() string {
	return fmt.Sprintf("%s.%s", m.k, ext)
}

func (m *Msg) Bytes() (b []byte, err error) {

	b, err = m.bytesFile()
	if err == nil {
		return b, err
	}

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
	r := ifaceS3.NewReaderUncompress(m.srv.s3sess, m.srv.config.S3Bucket)
	return r.Get(m.k, m.hourpath(), m.t)
}
