package fs

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

var tempDir = os.Getenv("TEST_DIR")
var msgSize = os.Getenv("TEST_SIZE")

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func benchmarkConcurrentWriter(cw int, b *testing.B) {

	size := int64(1024)
	if msgSize != "" {
		size, _ = strconv.ParseInt(msgSize, 10, 64)
	}
	content := RandStringBytes(size)

	srv := &Server{
		C:       make(chan *Msg, cw),
		writers: make(chan *writer, 1000),
		shards:  uint32(16),
	}

	if tempDir != "" {
		srv.config.Path = fmt.Sprintf("%s/%d", tempDir, cw)
	} else {
		srv.config.Path = fmt.Sprintf("/tmp/test/%d", cw)
	}

	for i := 0; i < cw; i++ {
		srv.writers <- newWriter(srv)
	}

	for i := 0; i < b.N; i++ {
		m := getMsg(srv)
		m.k = fmt.Sprintf("test-%d-%d", cw, i)
		m.b.Write(content)

		srv.C <- m
	}

	close(srv.writers)
	for w := range srv.writers {
		w.exit()
	}
}

func BenchmarkConcurrentWriter1(b *testing.B) {
	benchmarkConcurrentWriter(1, b)
}

func BenchmarkConcurrentWriter10(b *testing.B) {
	benchmarkConcurrentWriter(10, b)
}

func BenchmarkConcurrentWriter20(b *testing.B) {
	benchmarkConcurrentWriter(20, b)
}

func BenchmarkConcurrentWriter30(b *testing.B) {
	benchmarkConcurrentWriter(30, b)
}

func BenchmarkConcurrentWriter40(b *testing.B) {
	benchmarkConcurrentWriter(40, b)
}

func BenchmarkConcurrentWriter50(b *testing.B) {
	benchmarkConcurrentWriter(50, b)
}

func BenchmarkConcurrentWriter100(b *testing.B) {
	benchmarkConcurrentWriter(100, b)
}
