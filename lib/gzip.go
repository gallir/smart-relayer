package lib

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"sync"
)

const (
	GzCompressionLevel = 3
)

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		w, _ := gzip.NewWriterLevel(ioutil.Discard, GzCompressionLevel)
		return w
	},
}

func GetGzipWriter(w io.Writer) *gzip.Writer {
	zw := gzipWriterPool.Get().(*gzip.Writer)
	zw.Reset(w)
	return zw
	return zw
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
