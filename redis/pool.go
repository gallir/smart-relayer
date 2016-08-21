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
	clients      []*elem
	free         []*elem
	idle         []*elem
	max, maxIdle int
	server       *Server
}

// New returns a new pool manager
func newPool(server *Server, max, maxIdle int) (p *pool) {
	p = &pool{
		server:  server,
		max:     max,
		maxIdle: maxIdle,
	}
	p.clients = make([]*elem, 0, max)
	p.free = make([]*elem, 0, max)
	p.idle = make([]*elem, 0, max)
	return
}

func (p *pool) get() (e *elem) {
	p.Lock()
	defer p.Unlock()

	if len(p.free) == 0 {
		if len(p.clients) < p.max {
			e = p._createElem()
		} else {
			e = p._pickNonFree()
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
		if p.maxIdle > 0 && len(p.free) > p.maxIdle {
			p.idle = append(p.idle, e)
		} else {
			p.free = append(p.free, e)
		}
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

func (p *pool) _pickNonFree() (e *elem) {
	if l := len(p.idle); l > 0 {
		// Select the last element added to idle
		e = p.idle[l-1]
		p.idle = p.idle[:l-1]
	} else {
		// Otherwise pick a random element from all
		e = p.clients[rand.Intn(len(p.clients))]
	}
	return
}
