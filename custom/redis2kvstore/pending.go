package redis2kvstore

import (
	"sync"
)

var (
	pendingPool = sync.Pool{}
)

func getPending() map[string]*Hmset {
	o := pendingPool.Get()
	if p, ok := o.(map[string]*Hmset); ok {
		return p
	}
	return make(map[string]*Hmset, 256)
}

func putPending(p map[string]*Hmset) {
	pendingPool.Put(p)
}
