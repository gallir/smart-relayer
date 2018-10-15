package shortlivedpool

import (
	"sync"
)

const (
	defaultMinSize = 64
)

// SlicedStack implements a simple stack on a slice
type SlicedStack struct {
	sync.Mutex
	vec []interface{}
}

// Put pushes en element into the stack
func (s *SlicedStack) Push(x interface{}) {
	s.Lock()
	s.vec = append(s.vec, x)

	if len(s.vec) > defaultMinSize && cap(s.vec) > len(s.vec)*4 {
		// Shrink the slice
		newVec := make([]interface{}, len(s.vec), len(s.vec)*2)
		copy(newVec, s.vec)
		s.vec = newVec
	}
	s.Unlock()
}

// Pop gets the last element inserted
func (s *SlicedStack) Pop() interface{} {
	s.Lock()
	l := len(s.vec)
	if l == 0 {
		s.Unlock()
		return nil
	}
	x := s.vec[l-1]
	s.vec[l-1] = nil
	s.vec = s.vec[:l-1]
	s.Unlock()
	return x
}

// Len returns the number of elements in the array
func (s *SlicedStack) Len() int {
	return len(s.vec)
}
