package redis2

import (
	"strings"

	"github.com/gallir/smart-relayer/lib"
	"github.com/mediocregopher/radix.v2/redis"
)

const unknownDB = -1

// Request stores the data for each client request
type Request struct {
	resp            *redis.Resp
	command         string
	responseChannel chan *redis.Resp // Channel to send the response to the original client
	database        int              // The current database at the time the request was issued
}

func newRequest(resp *redis.Resp, c *lib.RelayerConfig) *Request {
	r := &Request{resp, "", nil, unknownDB}

	if resp != nil {
		ms, err := resp.Array()
		if err != nil || len(ms) < 1 {
			return nil
		}

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
