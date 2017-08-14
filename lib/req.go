package lib

import (
	"strings"

	"github.com/gallir/radix.improved/redis"
)

const (
	unknownDB     = -1
	selectCommand = "SELECT"
)

// Request stores the data for each client request
type Request struct {
	resp            *redis.Resp
	Items           []*redis.Resp
	Command         string
	responseChannel chan *redis.Resp // Channel to send the response to the original client
	database        int              // The current database at the time the request was issued
}

func NewRequest(resp *redis.Resp, c *RelayerConfig) *Request {
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
		r.Items = ms

		cmd, err := ms[0].Str()
		if err != nil {
			return nil
		}

		r.Command = strings.ToUpper(cmd)
		if r.Command == selectCommand && len(ms) > 1 {
			db, e := ms[1].Int()
			if e == nil {
				r.database = db
			}
		}

	}
	return r

}
