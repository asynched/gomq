package concurrency

import (
	"sync"
)

type Mutex[T any] struct {
	mutex sync.Mutex
	state *T
}

func NewMutex[T any](state *T) Mutex[T] {
	return Mutex[T]{
		mutex: sync.Mutex{},
		state: state,
	}
}

func (m *Mutex[T]) Lock() *T {
	m.mutex.Lock()
	return m.state
}

func (m *Mutex[T]) Unlock() {
	m.mutex.Unlock()
}
