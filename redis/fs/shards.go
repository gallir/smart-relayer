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

func NewShardsServer(srv *Server) *ShardsServer {
	ss := &ShardsServer{
		srv: srv,
	}
	ss.reload()
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

func (ss *ShardsServer) reload() {
	ss.Lock()
	defer ss.Unlock()

	l := len(ss.pool)

	// Reload to the running shards
	for _, o := range ss.pool {
		o.reload()
	}

	// Don't make changes if the running shards are the same as the config value
	if l == ss.srv.config.Shards {
		return
	}

	if ss.srv.config.Shards > l {
		for i := l; i < ss.srv.config.Shards; i++ {
			ss.pool = append(ss.pool, newShard(ss.srv))
		}
	} else {
		for i := ss.srv.config.Shards; i > l; i-- {
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
