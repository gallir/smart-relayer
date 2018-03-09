package fs

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
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

func benchmarkConcurrentWriter(cw int, shards int, b *testing.B) {

	size := int64(1024)
	if msgSize != "" {
		size, _ = strconv.ParseInt(msgSize, 10, 64)
	}
	content := RandStringBytes(size)

	srv := &Server{
		C:       make(chan *Msg, 100000),
		writers: make(chan *writer, 1000),
		shards:  uint32(shards),
	}

	if tempDir == "" {
		tempDir = "/tmp/test"
	}
	srv.config.Path = fmt.Sprintf("%s/test-writers-%d-shards-%d", tempDir, cw, shards)

	for i := 0; i < cw; i++ {
		srv.writers <- newWriter(srv)
	}
	close(srv.writers)

	for i := 0; i < b.N; i++ {
		m := getMsg(srv)
		m.k = fmt.Sprintf("test-%d-%d", cw, i)
		m.b.Write(content)
		m.t = time.Now()

		srv.C <- m
	}
	close(srv.C)

	srv.exiting = true
	wg := &sync.WaitGroup{}
	for w := range srv.writers {
		wg.Add(1)
		go func(w *writer, wg *sync.WaitGroup) {
			w.exit()
			wg.Done()
		}(w, wg)
	}
	wg.Wait()

	if len(srv.C) > 0 {
		b.Errorf("Messages lost %d", len(srv.C))
	}
}

// Worker: 1
func BenchmarkWriters1Shards1(b *testing.B) {
	benchmarkConcurrentWriter(1, 1, b)
}

func BenchmarkWriters1Shards2(b *testing.B) {
	benchmarkConcurrentWriter(1, 2, b)
}

func BenchmarkWriters1Shards16(b *testing.B) {
	benchmarkConcurrentWriter(1, 16, b)
}

func BenchmarkWriters1Shards64(b *testing.B) {
	benchmarkConcurrentWriter(1, 64, b)
}

// Worker: 2
func BenchmarkWriters2Shards1(b *testing.B) {
	benchmarkConcurrentWriter(2, 1, b)
}

func BenchmarkWriters2Shards2(b *testing.B) {
	benchmarkConcurrentWriter(2, 2, b)
}

func BenchmarkWriters2Shards16(b *testing.B) {
	benchmarkConcurrentWriter(2, 16, b)
}

func BenchmarkWriters2Shards64(b *testing.B) {
	benchmarkConcurrentWriter(2, 64, b)
}

// Worker: 16
func BenchmarkWriters16Shards1(b *testing.B) {
	benchmarkConcurrentWriter(16, 1, b)
}

func BenchmarkWriters16Shards2(b *testing.B) {
	benchmarkConcurrentWriter(16, 2, b)
}
func BenchmarkWriters16Shards16(b *testing.B) {
	benchmarkConcurrentWriter(16, 16, b)
}

func BenchmarkWriters16Shards64(b *testing.B) {
	benchmarkConcurrentWriter(16, 64, b)
}

// Worker: 32
func BenchmarkWriters32Shards1(b *testing.B) {
	benchmarkConcurrentWriter(32, 1, b)
}

func BenchmarkWriters32Shards2(b *testing.B) {
	benchmarkConcurrentWriter(32, 2, b)
}
func BenchmarkWriters32Shards16(b *testing.B) {
	benchmarkConcurrentWriter(32, 16, b)
}

func BenchmarkWriters32Shards64(b *testing.B) {
	benchmarkConcurrentWriter(32, 64, b)
}

// Worker: 64
func BenchmarkWriters64Shards1(b *testing.B) {
	benchmarkConcurrentWriter(64, 1, b)
}

func BenchmarkWriters64Shards2(b *testing.B) {
	benchmarkConcurrentWriter(64, 2, b)
}
func BenchmarkWriters64Shards16(b *testing.B) {
	benchmarkConcurrentWriter(64, 16, b)
}

func BenchmarkWriters64Shards64(b *testing.B) {
	benchmarkConcurrentWriter(64, 64, b)
}
