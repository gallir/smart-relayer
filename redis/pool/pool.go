package pool

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
	ID      int
	Counter int
	Client  lib.RelayerClient
}

type CreateFunction func(*lib.RelayerConfig) lib.RelayerClient

// Pool keep a list of clients' elements
type Pool struct {
	sync.Mutex
	cf           CreateFunction
	config       *lib.RelayerConfig
	clients      []*elem
	free         []*elem
	idle         []*elem
	max, maxIdle int
	lastNonFree  time.Time
	lastIdle     time.Time
}

// New returns a new pool manager
func New(c *lib.RelayerConfig, cf CreateFunction) (p *Pool) {
	p = &Pool{
		cf: cf,
	}
	p.ReadConfig(c)
	p.clients = make([]*elem, 0, p.max)
	p.free = make([]*elem, 0, p.max)
	p.idle = make([]*elem, 0, p.max)
	return
}

func (p *Pool) ReadConfig(c *lib.RelayerConfig) {
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

	for _, e := range p.clients {
		e.Client.Reload(c)
	}

	p.config = c

}

func (p *Pool) Reset() {
	for _, e := range p.clients {
		e.Client.Exit()
	}
	p.clients = nil
	p.free = nil
	p.idle = nil
}

func (p *Pool) Get() (e *elem, ok bool) {
	p.Lock()
	defer p.Unlock()

	e, ok = p._get()
	if ok {
		// Check the element is ok
		if !e.Client.IsValid() {
			// Replace it
			i := e.ID
			log.Printf("Error in client %d, replacing it", i)
			e = p._createElem(e.ID)
			e.Counter++
			p.clients[i] = e
		}
	}
	return
}

func (p *Pool) _get() (e *elem, ok bool) {
	e, ok = p._pickFree()
	if ok {
		return
	}
	p.lastNonFree = time.Now()

	e, ok = p._pickIdle()
	if ok {
		return
	}

	if l := len(p.clients); l < p.max {
		e = p._createElem(l)
		e.Counter++
		return e, true
	}

	e, ok = p._pickNonFree()
	if ok {
		e.Counter++
	}
	return
}

func (p *Pool) Close(e *elem) {
	p.Lock()
	defer p.Unlock()

	e.Counter--

	if e.ID >= len(p.clients) {
		lib.Debugf("Pool: exceeded limit, %d counter %d", e.ID, e.Counter)
		if e.Counter <= 0 && e.Client != nil {
			e.Client.Exit()
		}
		return
	}

	if p.clients[e.ID].Client != e.Client {
		lib.Debugf("Pool: tried to close a non existing client")
		return
	}

	if e.Counter <= 0 {
		e.Counter = 0
		now := time.Now()
		timeLimit := now.Add(-minAddIdlePeriod)
		if p.maxIdle > 0 && len(p.free) > p.maxIdle &&
			p.lastNonFree.Before(timeLimit) &&
			p.lastIdle.Before(timeLimit) {
			p.idle = append(p.idle, e)
			p.lastIdle = now
			lib.Debugf("Pool: added to idle %d counter %d", e.ID, e.Counter)
		} else {
			// lib.Debugf("Pool: added to free %d counter %d", e.ID, e.counter)
			p.free = append(p.free, e)
		}
	}
}

func (p *Pool) _createElem(id int) (e *elem) {
	//cl := p.server.NewClient()
	cl := p.cf(p.config)
	e = &elem{
		ID:     id,
		Client: cl,
	}
	p.clients = append(p.clients, e)
	lib.Debugf("Pool: created new client %d", e.ID)
	return
}

func (p *Pool) _pickFree() (*elem, bool) {
	l := len(p.free)
	if l == 0 {
		return nil, false
	}

	e := p.free[l-1]
	e.Counter++
	p.free = p.free[:l-1]

	return e, true
}

func (p *Pool) _pickIdle() (e *elem, ok bool) {
	if l := len(p.idle); p.maxIdle > 0 && l > 0 {
		// Select the last element added to idle
		e = p.idle[l-1]
		e.Counter++
		p.idle = p.idle[:l-1]
		ok = true
	}
	return
}

func (p *Pool) _pickNonFree() (e *elem, ok bool) {
	// Otherwise pick a random element from clients
	elems := len(p.clients)
	if elems > p.max {
		elems = p.max
	}
	e = p.clients[rand.Intn(elems)]
	e.Counter++
	return e, true
}
