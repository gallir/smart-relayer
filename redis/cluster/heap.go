package cluster

import "github.com/mediocregopher/radix.v2/redis"

type seqResp struct {
	seq  uint64
	resp *redis.Resp
}

type seqRespHeap []*reqData

func (h *seqRespHeap) Len() int           { return len(*h) }
func (h *seqRespHeap) Less(i, j int) bool { return (*h)[i].seq < (*h)[j].seq }
func (h *seqRespHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *seqRespHeap) Push(x interface{}) {
	*h = append(*h, x.(*reqData))
}

func (h *seqRespHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
