package redis2

import (
	"log"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

type CreateFunction func(*lib.RelayerConfig) *Client

// Pool keep a list of clients' elements
type Pool struct {
	sync.Mutex
	config       lib.RelayerConfig
	free         chan *Client
	maxIdle      int
	minIdle      int
	maxConnected time.Duration
	monitorCh    chan bool
}

// New returns a new pool manager
func NewPool(cfg *lib.RelayerConfig) (p *Pool) {
	p = &Pool{
		monitorCh: make(chan bool, 1),
	}
	p.Reload(cfg)
	go p.monitor()
	return
}

func (p *Pool) Reload(cfg *lib.RelayerConfig) {
	p.Lock()
	defer p.Unlock()

	p.maxConnected = time.Duration(cfg.MaxConnectedSecs) * time.Second
	p.minIdle = cfg.MinIdleConnections
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

func (p *Pool) monitor() {
	for _ = range p.monitorCh {
		if len(p.free) < p.minIdle {
			p.free <- NewClient(&p.config)
		}
	}
}

func (p *Pool) Reset() {
	p.Lock()
	defer p.Unlock()

	close(p.monitorCh)
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
		lib.Debugf("Pool: created new client in get")
		c = NewClient(&p.config)
	}

	// Check min idle connections
	select {
	case p.monitorCh <- true:
	default:
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

	if p.maxConnected > 0 && time.Since(c.connectedAt) > p.maxConnected {
		lib.Debugf("Pool: exceeded connected duration %s", time.Since(c.connectedAt))
		c.Exit()
		// Check min idle connections
		select {
		case p.monitorCh <- true:
		default:
		}
		return
	}

	// Update the config before send it back to the pool
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
