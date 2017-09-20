package lib

import (
	"log"

	"github.com/pquerna/ffjson/ffjson"
)

type InterRecord struct {
	Types     int
	Timestamp float64                `json:"_ts"`
	Data      map[string]interface{} `json:"data"`
	Raw       []byte                 // 0 json, 1 Raw bytes
}

func (r *InterRecord) Add(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *InterRecord) Sadd(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *InterRecord) Mhset(key, k string, v interface{}) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

func (r *InterRecord) Bytes() []byte {
	if r.Types != 0 {
		return r.Raw
	}

	buf, err := ffjson.Marshal(r)
	if err != nil {
		log.Panic(err)
	}

	return buf
}

func (r *InterRecord) Len() int {
	if r.Types != 0 {
		return len(r.Raw)
	}

	return len(r.Data)
}
