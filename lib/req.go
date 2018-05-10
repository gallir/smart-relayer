package lib

import (
	"io"
	"strings"

	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
)

const (
	UnknownDB     = -1
	selectCommand = "SELECT"
)

// Request stores the data for each client request
type Request struct {
	Resp     *redis.Resp
	Items    []*redis.Resp
	Command  string
	Conn     io.Writer // Writer to send the response to the original client
	Database int       // The current database at the time the request was issued
}

func NewRequest(resp *redis.Resp, c *RelayerConfig) *Request {
	r := &Request{
		Database: UnknownDB,
	}

	if resp != nil {
		r.Resp = resp
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
				r.Database = db
			}
		}

	}
	return r

}
