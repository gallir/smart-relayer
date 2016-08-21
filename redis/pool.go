package redis

import (
	"log"
	"math/rand"
	"sync"

	"github.com/gallir/smart-relayer/lib"
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
	p.max = c.MaxConnections
	if p.max <= 0 {
		p.max = 1 // Default number of connections
	}
	p.maxIdle = c.MaxIdleConnections

}

func (p *pool) reset(c *lib.RelayerConfig) {
	for _, e := range p.clients {
		e.client.exit()
	}
	p.clients = nil
	p.free = nil
	p.idle = nil
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

	if e.id >= len(p.clients) || p.clients[e.id].client != e.client {
		log.Println("Pool: tried to close a non existing client")
		return
	}

	e.counter--
	if e.counter == 0 {
		if p.maxIdle > 0 && len(p.free) > p.maxIdle {
			p.idle = append(p.idle, e)
			// lib.Debugf("Pool: added to idle %d", e.id)
		} else {
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

func (p *pool) _pickNonFree() (e *elem) {
	if l := len(p.idle); p.maxIdle > 0 && l > 0 {
		// Select the last element added to idle
		e = p.idle[l-1]
		p.idle = p.idle[:l-1]
		// lib.Debugf("Pool: picked from idle %d", e.id)
	} else {
		// Otherwise pick a random element from all
		elems := len(p.clients)
		if elems > p.max {
			elems = p.max
		}
		e = p.clients[rand.Intn(elems)]
	}
	return
}
