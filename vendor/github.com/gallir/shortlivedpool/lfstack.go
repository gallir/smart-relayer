package shortlivedpool

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type Element struct {
	value interface{}
	next  unsafe.Pointer
}

type LFStack struct {
	head unsafe.Pointer
	len  int64
}

func NewStack() *LFStack {
	return &LFStack{}
}

func (s *LFStack) Push(value interface{}) {
	nElem := &Element{}
	nElem.value = value
	for {
		nElem.next = s.head
		if atomic.CompareAndSwapPointer(&s.head, nElem.next, unsafe.Pointer(nElem)) {
			atomic.AddInt64(&s.len, 1)
			return
		}
		runtime.Gosched()
	}
}

func (s *LFStack) Pop() (value interface{}) {
	for {
		oldHead := s.head
		if oldHead == nil {
			return nil
		}
		if atomic.CompareAndSwapPointer(&s.head, oldHead, (*Element)(oldHead).next) {
			atomic.AddInt64(&s.len, -1)
			v := (*Element)(oldHead).value
			(*Element)(oldHead).value = nil
			return v
		}
		runtime.Gosched()
	}
}

func (s *LFStack) Len() int64 {
	return atomic.LoadInt64(&s.len)
}
