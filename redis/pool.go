package redis

import (
	"math/rand"
	"sync"
)

type elem struct {
	id      int
	counter int
	client  *Client
}

// Pool keep a list of clients' elements
type pool struct {
	sync.Mutex
	clients []*elem
	free    []*elem
	max     int
	server  *Server
}

// New returns a new pool manager
func newPool(server *Server, max int) (p *pool) {
	p = &pool{
		server: server,
		max:    max,
	}
	p.clients = make([]*elem, 0, max)
	p.free = make([]*elem, 0, max)
	return
}

func (p *pool) get() (e *elem) {
	p.Lock()
	defer p.Unlock()

	if len(p.free) == 0 {
		if len(p.clients) < p.max {
			e = p._createElem()
		} else {
			e = p.clients[rand.Intn(len(p.clients))]
		}
	} else {
		e = p.free[0]
		p.free = p.free[1:]
	}
	e.counter++
	return
}

func (p *pool) close(e *elem) {
	p.Lock()
	defer p.Unlock()

	e.counter--
	if e.counter == 0 {
		p.free = append(p.free, e)
	}
}

func (p *pool) _createElem() (e *elem) {
	cl := NewClient(p.server)
	e = &elem{
		id:     len(p.clients),
		client: cl,
	}
	p.clients = append(p.clients, e)
	return
}
