package lib

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/klauspost/compress/gzip"
)

const (
	GzCompressionLevel = 1
)

var (
	gzipWriterPool = sync.Pool{}
	gzipReaderPool = sync.Pool{}
)

func GetGzipWriterLevel(w io.Writer, level int) *gzip.Writer {
	var zw *gzip.Writer
	zwi := gzipWriterPool.Get()
	if zwi == nil {
		zw, _ = gzip.NewWriterLevel(nil, level)
	} else {
		zw = zwi.(*gzip.Writer)
	}
	zw.Reset(w)
	return zw
}

func GetGzipWriter(w io.Writer) *gzip.Writer {
	return GetGzipWriterLevel(w, GzCompressionLevel)
}

func PutGzipWriter(zw *gzip.Writer) {
	if zw == nil {
		return
	}
	zw.Reset(ioutil.Discard)
	gzipWriterPool.Put(zw)
}

func GetGzipReader(r io.Reader) (*gzip.Reader, error) {
	zri := gzipReaderPool.Get()
	if zri == nil {
		return gzip.NewReader(r)
	}

	zr := zri.(*gzip.Reader)
	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

func PutGzipReader(zr *gzip.Reader) {
	if zr == nil {
		return
	}
	gzipReaderPool.Put(zr)
}
