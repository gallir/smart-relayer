package stream

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/gallir/smart-relayer/lib"
	gzip "github.com/klauspost/pgzip"
)

// NewWriteCompress create a pointer and start the temp file and the gz interface
func NewWriteCompress(path string, fn func(name string, f *os.File) error) (*WriteCompress, error) {
	s := &WriteCompress{
		path: path,
		fn:   fn,
	}
	if err := s.start(); err != nil {
		return nil, err
	}

	return s, nil
}

// WriteCompress is an structure to create gz files with limits of lines and size
type WriteCompress struct {
	tmp     *os.File
	gz      *gzip.Writer
	id      int
	count   int
	size    int
	path    string
	RecDone int
	fn      func(name string, f *os.File) error
}

func (s *WriteCompress) start() (err error) {

	s.id++
	s.count = 0
	s.size = 0

	s.tmp, err = ioutil.TempFile("", "logs")
	if err != nil {
		return
	}

	s.gz = lib.GzipPool.Get().(*gzip.Writer)
	s.gz.Reset(s.tmp)
	s.gz.Name = fmt.Sprintf("records-%d.csv", s.id)

	return
}

// Control will close the tmp file and start a new one if cross one of the limits
func (s *WriteCompress) Control() (err error) {

	s.RecDone++

	if s.count < defaultLimitRecords && s.size < defaultFileSize {
		s.count++
		return
	}

	if err = s.Close(); err != nil {
		return err
	}

	if err = s.start(); err != nil {
		return err
	}

	return
}

// Write in the gzip
func (s *WriteCompress) Write(b []byte) (int, error) {
	size, err := s.gz.Write(b)
	s.size += size
	return size, err
}

// Close the temp file and the gz interface. The gz will be returned to the pool
func (s *WriteCompress) Close() (err error) {

	s.gz.Comment = fmt.Sprintf("%d records stored", s.count)
	// Close the gz interface
	if err = s.gz.Close(); err != nil {
		return
	}

	// Point the the gz interface to /dev/null and return it to the pool
	s.gz.Reset(ioutil.Discard)
	lib.GzipPool.Put(s.gz)
	s.gz = nil

	if s.fn == nil {
		s.tmp.Close()
		return
	}

	if err := s.fn(fmt.Sprintf("%s/records-%d.gz", s.path, s.id), s.tmp); err != nil {
		return err
	}

	s.tmp.Close()
	return os.Remove(s.tmp.Name())
}
