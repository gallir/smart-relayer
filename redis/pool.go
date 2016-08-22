package redis

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

const (
	minAddIdlePeriod = 5 * time.Second
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
	lastNonFree  time.Time
	lastIdle     time.Time
}

// New returns a new pool manager
func newPool(server *Server, c *lib.RelayerConfig) (p *pool) {
	p = &pool{
		server: server,
	}
	p.readConfig(c)
	p.clients = make([]*elem, 0, p.max)
	p.free = make([]*elem, 0, p.max)
	p.idle = make([]*elem, 0, p.max)
	return
}

func (p *pool) readConfig(c *lib.RelayerConfig) {
	p.Lock()
	defer p.Unlock()

	p.max = c.MaxConnections
	if p.max <= 0 {
		p.max = 1 // Default number of connections
	}
	p.maxIdle = c.MaxIdleConnections
	if p.maxIdle > p.max {
		p.maxIdle = p.max
	}

	if len(p.clients) > p.max {
		log.Printf("Reducing the pool size from %d to %d", len(p.clients), p.max)
		p.clients = p.clients[:p.max]
	}

}

func (p *pool) reset(c *lib.RelayerConfig) {
	for _, e := range p.clients {
		e.client.exit()
	}
	p.clients = nil
	p.free = nil
	p.idle = nil
}

func (p *pool) get() (e *elem, ok bool) {
	p.Lock()
	defer p.Unlock()

	e, ok = p._pickFree()
	if ok {
		return
	}
	p.lastNonFree = time.Now()

	e, ok = p._pickIdle()
	if ok {
		return
	}

	if len(p.clients) < p.max {
		e = p._createElem()
		e.counter++
		return e, true
	}

	e, ok = p._pickNonFree()
	if ok {
		e.counter++
	}
	return
}

func (p *pool) close(e *elem) {
	p.Lock()
	defer p.Unlock()

	e.counter--

	if e.id >= len(p.clients) {
		lib.Debugf("Pool: exceeded limit, %d counter %d", e.id, e.counter)
		if e.counter <= 0 && e.client != nil {
			lib.Debugf("Pool: for exit of the client")
			e.client.exit()
		}
		return
	}

	if p.clients[e.id].client != e.client {
		lib.Debugf("Pool: tried to close a non existing client")
		return
	}

	if e.counter <= 0 {
		e.counter = 0
		now := time.Now()
		timeLimit := now.Add(-minAddIdlePeriod)
		if p.maxIdle > 0 && len(p.free) > p.maxIdle &&
			p.lastNonFree.Before(timeLimit) &&
			p.lastIdle.Before(timeLimit) {
			p.idle = append(p.idle, e)
			p.lastIdle = now
			// lib.Debugf("Pool: added to idle %d counter %d", e.id, e.counter)
		} else {
			// lib.Debugf("Pool: added to free %d counter %d", e.id, e.counter)
			p.free = append(p.free, e)
		}
	}
}

func (p *pool) _createElem() (e *elem) {
	cl := newClient(p.server)
	e = &elem{
		id:     len(p.clients),
		client: cl,
	}
	p.clients = append(p.clients, e)
	lib.Debugf("Pool: created new client %d", e.id)
	return
}

func (p *pool) _pickFree() (e *elem, ok bool) {
	if len(p.free) == 0 {
		return nil, false
	}

	e = p.free[0]
	e.counter++
	p.free = p.free[1:]
	return e, true
}

func (p *pool) _pickIdle() (e *elem, ok bool) {
	if l := len(p.idle); p.maxIdle > 0 && l > 0 {
		// Select the last element added to idle
		e = p.idle[l-1]
		e.counter++
		p.idle = p.idle[:l-1]
		ok = true
	}
	return
}

func (p *pool) _pickNonFree() (e *elem, ok bool) {
	// Otherwise pick a random element from clients
	elems := len(p.clients)
	if elems > p.max {
		elems = p.max
	}
	e = p.clients[rand.Intn(elems)]
	e.counter++
	return e, true
}
