package fh

import (
	"log"
	"sync"
	"time"

	"github.com/pquerna/ffjson/ffjson"
)

type record struct {
	types     int
	Timestamp float64                `json:"_ts"`
	Data      map[string]interface{} `json:"data"`
	raw       []byte                 // 0 json, 1 raw bytes
}

var reqPool = sync.Pool{
	New: func() interface{} {
		return &record{
			Data: make(map[string]interface{}),
		}
	},
}

func fromPool() *record {
	r := reqPool.Get().(*record)
	r.Timestamp = float64(time.Now().UnixNano()) / float64(time.Nanosecond)
	return r
}

func putPool(r *record) {
	r.types = 0
	r.raw = nil
	if len(r.Data) < 100 {
		for i := range r.Data {
			delete(r.Data, i)
		}
	} else {
		r.Data = make(map[string]interface{})
	}
	reqPool.Put(r)
}

func (r *record) add(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *record) sadd(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *record) mhset(key, k string, v interface{}) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

func (r *record) bytes() []byte {
	if r.types != 0 {
		return r.raw
	}

	buf, err := ffjson.Marshal(r)
	if err != nil {
		log.Panic(err)
	}

	return buf
}

func (r *record) len() int {
	if r.types != 0 {
		return len(r.raw)
	}

	return len(r.Data)
}
