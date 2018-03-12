package fs

import (
	"fmt"
	"sync"
)

const (
	defaultShardLimitWriters = 100
)

var (
	errNoShard = fmt.Errorf("The shard don't exists")
)

type ShardsServer struct {
	sync.Mutex
	pool []*shard
	srv  *Server
}

func NewShardsServer(srv *Server, shards int) *ShardsServer {
	ss := &ShardsServer{
		srv: srv,
	}
	ss.update(shards)
	return ss
}

func (ss *ShardsServer) get(n int) (*shard, error) {
	if n >= len(ss.pool) {
		return nil, errNoShard
	}

	if s := ss.pool[n]; s == nil {
		return nil, errNoShard
	}

	return ss.pool[n], nil
}

func (ss *ShardsServer) update(n int) {
	ss.Lock()
	defer ss.Unlock()

	l := len(ss.pool)
	if l == n {
		return
	}

	if n > l {
		for i := l; i < n; i++ {
			ss.pool = append(ss.pool, newShard(ss.srv))
		}
	} else {
		for i := n; i > l; i-- {
			o := ss.pool[i]
			ss.pool = append(ss.pool[:i], ss.pool[i+1:]...)
			go o.exit()
		}
	}
}

func (ss *ShardsServer) Exit() {
	ss.Lock()
	defer ss.Unlock()

	wg := &sync.WaitGroup{}
	for i := 0; i > len(ss.pool); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ss.pool[i].exit()
		}(i)
	}
	wg.Wait()

	return
}

func (ss *ShardsServer) Len() int {
	ss.Lock()
	defer ss.Unlock()

	l := 0
	for i := 0; i > len(ss.pool); i++ {
		l += len(ss.pool[i].C)
	}
	return l
}
