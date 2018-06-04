package lib

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/klauspost/compress/gzip"
)

const (
	GzCompressionLevel = 3
)

var gzipWriterPool = sync.Pool{}

func GetGzipWriterLevel(w io.Writer, level int) *gzip.Writer {
	zw := gzipWriterPool.Get().(*gzip.Writer)
	if zw == nil {
		zw, _ = gzip.NewWriterLevel(nil, level)
	}
	zw.Reset(w)
	return zw
}

func GetGzipWriter(w io.Writer) *gzip.Writer {
	return GetGzipWriterLevel(w, GzCompressionLevel)
}

func PutGzipWriter(zw *gzip.Writer) {
	zw.Reset(ioutil.Discard)
	gzipWriterPool.Put(zw)
}

var gzipReaderPool = sync.Pool{
	New: func() interface{} {
		return new(gzip.Reader)
	},
}

func GetGzipReader(r io.Reader) (*gzip.Reader, error) {
	zr := gzipReaderPool.Get().(*gzip.Reader)
	if zr == nil {
		zrNew, err := gzip.NewReader(r)
		return zrNew, err
	}

	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

func PutGzipReader(zr *gzip.Reader) {
	gzipReaderPool.Put(zr)
}
