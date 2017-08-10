package fh

import (
	"strings"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/lib"
)

const unknownDB = -1

// Request stores the data for each client request
type Request struct {
	resp            *redis.Resp
	items           []*redis.Resp
	command         string
	responseChannel chan *redis.Resp // Channel to send the response to the original client
	database        int              // The current database at the time the request was issued
}

func newRequest(resp *redis.Resp, c *lib.RelayerConfig) *Request {
	r := &Request{
		database: unknownDB,
	}

	if resp != nil {
		r.resp = resp
		ms, err := resp.Array()
		if err != nil || len(ms) < 1 {
			return nil
		}
		// Store already split
		r.items = ms

		cmd, err := ms[0].Str()
		if err != nil {
			return nil
		}

		r.command = strings.ToUpper(cmd)
		if r.command == selectCommand && len(ms) > 1 {
			db, e := ms[1].Int()
			if e == nil {
				r.database = db
			}
		}

	}
	return r

}
