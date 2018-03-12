package fs

import "sync"

type shard struct {
	srv *Server
	C   chan *Msg
	w   chan *writer
}

func newShard(srv *Server) *shard {
	s := &shard{
		srv: srv,
		C:   make(chan *Msg, srv.config.Buffer),
		w:   make(chan *writer, defaultShardLimitWriters),
	}
	s.updateWriters()
	return s
}

func (s *shard) updateWriters() {
	l := len(s.w)
	n := s.srv.shardsWriters

	if l == n {
		return
	}

	if l < n {
		for i := l; i < n; i++ {
			s.w <- newWriter(s.srv, s.C)
		}
	} else {
		for i := n; i < l; i++ {
			w := <-s.w
			// Exit without blocking
			go w.exit()
		}
	}
}

// exit close he channel to recive messases and close the channels
// for the writers. And each writer is forced to exit but waiting until
// all messages in chan C are stored
func (s *shard) exit() {
	close(s.C)
	close(s.w)

	wg := &sync.WaitGroup{}
	for w := range s.w {
		wg.Add(1)
		go func(w *writer, wg *sync.WaitGroup) {
			defer wg.Done()
			w.exit()
		}(w, wg)
	}
	wg.Wait()
}
