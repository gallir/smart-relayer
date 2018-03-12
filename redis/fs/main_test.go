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

var (
	tempDir  = os.Getenv("TEST_DIR")
	msgSize  = os.Getenv("TEST_SIZE")
	testList = make([]tests, 0)
	cw       = []int{1, 2, 16, 32, 64}
)

type tests struct {
	shards  int
	wirters int
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func shardsPrivateWriters(shards, cw int, b *testing.B) {

	size := int64(1024)
	if msgSize != "" {
		size, _ = strconv.ParseInt(msgSize, 10, 64)
	}
	content := RandStringBytes(size)

	srv := &Server{
		shards: uint32(shards),
	}
	srv.config.Shards = shards
	srv.shardsWriters = cw

	if tempDir == "" {
		tempDir = "/tmp/test"
	}
	srv.config.Path = fmt.Sprintf("%s/test-shards-%d", tempDir, shards)

	srv.shardServer = NewShardsServer(srv, shards)

	for i := 0; i < b.N; i++ {
		m := getMsg(srv)
		m.k = fmt.Sprintf("test-%d", i)
		m.b.Write(content)
		m.t = time.Now()

		if s, err := srv.shardServer.get(m.getShard()); err == nil {
			s.C <- m
		} else {
			b.Logf("M: %#v", m)
			b.Logf("Shards: %#v", srv.shardServer)
			b.Errorf("Shard error: %s", err)
			continue
		}
	}

	srv.shardServer.Exit()
}

func independentWriters(shards, cw int, b *testing.B) {

	size := int64(1024)
	if msgSize != "" {
		size, _ = strconv.ParseInt(msgSize, 10, 64)
	}
	content := RandStringBytes(size)

	writers := make(chan *writer, 1000)
	C := make(chan *Msg, 10000000)

	srv := &Server{
		shards: uint32(shards),
	}
	srv.config.Shards = shards

	if tempDir == "" {
		tempDir = "/tmp/test"
	}
	srv.config.Path = fmt.Sprintf("%s/test-shards-%d", tempDir, shards)

	for i := 0; i < cw; i++ {
		writers <- newWriter(srv, C)
	}
	close(writers)

	for i := 0; i < b.N; i++ {
		m := getMsg(srv)
		m.k = fmt.Sprintf("test-%d", i)
		m.b.Write(content)
		m.t = time.Now()

		C <- m
	}

	close(C)

	srv.exiting = true
	wg := &sync.WaitGroup{}
	for w := range writers {
		wg.Add(1)
		go func(w *writer, wg *sync.WaitGroup) {
			w.exit()
			wg.Done()
		}(w, wg)
	}
	wg.Wait()

	if len(C) > 0 {
		b.Errorf("Messages lost %d", len(C))
	}
}

func BenchmarkShardsPrivateWriters(b *testing.B) {
	for i := 0; i < len(cw); i++ {
		for x := 0; x < len(cw); x++ {
			b.Run(fmt.Sprintf("shards-%.2d-writers-%.2d", cw[i], cw[x]), func(b *testing.B) {
				shardsPrivateWriters(cw[i], cw[x], b)
			})
		}
	}
}

func BenchmarkIndependentWriters(b *testing.B) {
	for i := 0; i < len(cw); i++ {
		for x := 0; x < len(cw); x++ {
			b.Run(fmt.Sprintf("shards-%.2d-writers-%.2d", cw[i], cw[x]), func(b *testing.B) {
				independentWriters(cw[i], cw[x], b)
			})
		}
	}
}
