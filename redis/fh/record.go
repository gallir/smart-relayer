package fh

import (
	"log"

	"github.com/pquerna/ffjson/ffjson"
)

type interRecord struct {
	types     int
	Timestamp float64                `json:"_ts"`
	Data      map[string]interface{} `json:"data"`
	raw       []byte                 // 0 json, 1 raw bytes
}

func (r *interRecord) add(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *interRecord) sadd(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *interRecord) mhset(key, k string, v interface{}) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

func (r *interRecord) bytes() []byte {
	if r.types != 0 {
		return r.raw
	}

	buf, err := ffjson.Marshal(r)
	if err != nil {
		log.Panic(err)
	}

	return buf
}

func (r *interRecord) len() int {
	if r.types != 0 {
		return len(r.raw)
	}

	return len(r.Data)
}
