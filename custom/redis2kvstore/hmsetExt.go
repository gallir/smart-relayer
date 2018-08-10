package redis2kvstore

import (
	"sync"

	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
)

var (
	hmsetPool sync.Pool
)

func getPoolHMSet() *Hmset {
	m := hmsetPool.Get()
	if m == nil {
		return &Hmset{}
	}
	m.(*Hmset).Reset()
	return m.(*Hmset)
}

func putPoolHMSet(m *Hmset) {
	hmsetPool.Put(m)
}

func (h *Hmset) processItems(items []*redis.Resp) {
	for i := 0; i < len(items); i++ {
		s, _ := items[i].Str()
		i++
		b, _ := items[i].Bytes()
		h.Fields = append(h.Fields, &Field{
			Name:  s,
			Value: b,
		})
	}
}
