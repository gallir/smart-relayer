package monad

import (
	"sync"
	"time"
)

var (
	defaultInterval        = time.Duration(500 * time.Millisecond)
	defaultCooldDownPeriod = time.Duration(10 * time.Second)
)

type Config struct {
	Min            uint64
	Max            uint64
	Desired        uint64
	Interval       time.Duration
	CoolDownPeriod time.Duration
	WarmFn         func() bool
	DesireFn       func(n uint64)
}

type Monad struct {
	sync.Mutex
	cfg         *Config
	t           *time.Ticker
	done        chan struct{}
	finish      chan struct{}
	desired     uint64
	lastActivty time.Time
}

func New(cfg *Config) *Monad {
	m := &Monad{
		done:   make(chan struct{}, 1),
		finish: make(chan struct{}, 1),
	}
	m.Reload(cfg)

	go m.monitor()

	return m
}

func (m *Monad) Reload(newCfg *Config) {
	m.Lock()
	defer m.Unlock()

	m.cfg = &Config{}
	*m.cfg = *newCfg

	if m.cfg.Interval.Nanoseconds() == 0 {
		m.cfg.Interval = defaultInterval
	}

	if m.cfg.CoolDownPeriod.Nanoseconds() == 0 {
		m.cfg.CoolDownPeriod = defaultCooldDownPeriod
	}

	if m.t == nil {
		m.t = time.NewTicker(m.cfg.Interval)
	} else {
		m.done <- struct{}{}
		<-m.finish
		m.t = time.NewTicker(m.cfg.Interval)
		go m.monitor()
	}
}

func (m *Monad) Exit() {
	m.done <- struct{}{}
	<-m.finish
}

func (m *Monad) monitor() {
	defer func() {
		m.finish <- struct{}{}
	}()

	for {
		select {
		case <-m.done:
			m.t.Stop()
			return
		case <-m.t.C:

			l := m.cfg.WarmFn()

			switch {
			case (l == true && m.desired < m.cfg.Max) || m.desired < m.cfg.Min:
				m.desired++
				m.cfg.DesireFn(m.desired)
				m.lastActivty = time.Now()
				break
			case (l == false && m.desired > m.cfg.Min) || m.desired > m.cfg.Max:
				if m.lastActivty.Add(m.cfg.CoolDownPeriod).Before(time.Now()) {
					m.desired--
					m.cfg.DesireFn(m.desired)
					m.lastActivty = time.Now()
				}
				break
			}

			//log.Printf("Queue: %v, Desired %d, lastActivity: %s", l, m.desired, m.lastActivty)

		}
	}
}
