package redis2

import (
	"log"
	"sync"

	"github.com/gallir/smart-relayer/lib"
)

type CreateFunction func(*lib.RelayerConfig) *Client

// Pool keep a list of clients' elements
type Pool struct {
	sync.Mutex
	config  lib.RelayerConfig
	free    chan *Client
	maxIdle int
}

// New returns a new pool manager
func NewPool(cfg *lib.RelayerConfig) (p *Pool) {
	p = &Pool{}
	p.ReadConfig(cfg)
	return
}

func (p *Pool) ReadConfig(cfg *lib.RelayerConfig) {
	p.Lock()
	defer p.Unlock()

	maxIdle := cfg.MaxIdleConnections
	if maxIdle == 0 {
		maxIdle = 1
	}

	if p.free != nil {
		if maxIdle != p.maxIdle {
			ch := make(chan *Client, maxIdle)
		LOOP:
			for {
				select {
				case c := <-p.free:
					select {
					case ch <- c:
						c.Reload(cfg)
					default:
						c.Exit()
					}
				default:
					break LOOP
				}
			}
			log.Printf("Changed the pool size from %d to %d", p.maxIdle, maxIdle)
			p.free = ch
		}
	} else {
		p.free = make(chan *Client, maxIdle)
	}

	p.maxIdle = maxIdle
	p.config = *cfg
}

func (p *Pool) Reset() {
	p.Lock()
	defer p.Unlock()

	for c := range p.free {
		c.Exit()
	}
}

// Get return a client from the pool or creates a new one
func (p *Pool) Get() (c *Client) {
LOOP:
	for c == nil {
		select {
		case item := <-p.free:
			if !item.IsValid() {
				item.Exit()
				lib.Debugf("Error in client, ignoring it")
				continue
			}
			c = item
		default:
			break LOOP
		}
	}

	if c == nil {
		lib.Debugf("Pool: created new client")
		c = NewClient(&p.config)
	}
	return
}

// Put stores a client in the idle pool if still valid
func (p *Pool) Put(c *Client) {
	if c == nil {
		return
	}

	// Don't insert it if it's disconnected or not valid
	if !c.IsValid() {
		c.Exit()
		return
	}

	// Update the config before go back to the pool
	if c.config != &p.config {
		c.config = &p.config
	}

	select {
	case p.free <- c:
	default:
		lib.Debugf("Pool: exceeded limit %d/%d", len(p.free), p.maxIdle)
		c.Exit()
	}
}
