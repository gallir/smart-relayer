package pool

import (
	"log"
	"sync"

	"github.com/gallir/smart-relayer/lib"
)

type CreateFunction func(*lib.RelayerConfig) lib.RelayerClient

// Pool keep a list of clients' elements
type Pool struct {
	sync.Mutex
	cf      CreateFunction
	config  lib.RelayerConfig
	free    chan lib.RelayerClient
	maxIdle int
}

// New returns a new pool manager
func New(c *lib.RelayerConfig, cf CreateFunction) (p *Pool) {
	p = &Pool{
		cf: cf,
	}
	p.ReadConfig(c)
	return
}

func (p *Pool) ReadConfig(c *lib.RelayerConfig) {
	p.Lock()
	defer p.Unlock()

	maxIdle := c.MaxIdleConnections
	if maxIdle == 0 {
		maxIdle = 1
	}

	if p.free != nil {
		if maxIdle != p.maxIdle {
			ch := make(chan lib.RelayerClient, maxIdle)
		LOOP:
			for {
				select {
				case e := <-p.free:
					select {
					case ch <- e:
						e.Reload(c)
					default:
						e.Exit()
					}
				default:
					break LOOP
				}
			}
			log.Printf("Changed the pool size from %d to %d", p.maxIdle, maxIdle)
			p.free = ch
		}
	} else {
		p.free = make(chan lib.RelayerClient, maxIdle)
	}

	p.maxIdle = maxIdle
	p.config = *c
}

func (p *Pool) Reset() {
	p.Lock()
	defer p.Unlock()

	for e := range p.free {
		e.Exit()
	}
}

func (p *Pool) Get() (e lib.RelayerClient) {
LOOP:
	for e == nil {
		select {
		case c := <-p.free:
			if !c.IsValid() {
				c.Exit()
				lib.Debugf("Error in client, ignoring it")
				continue
			}
			e = c
		default:
			break LOOP
		}
	}

	if e == nil {
		lib.Debugf("Pool: created new client")
		e = p.cf(&p.config)
	}
	return
}

func (p *Pool) Close(e lib.RelayerClient) {
	if e == nil {
		return
	}

	select {
	case p.free <- e:
	default:
		lib.Debugf("Pool: exceeded limit %d/%d", len(p.free), p.maxIdle)
		e.Exit()
	}
}
