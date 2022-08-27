package concurrency

import (
	"sync"
)

type Mutex[T any] struct {
	mutex sync.Mutex
	state *T
}

func (m *Mutex[T]) Lock() *T {
	m.mutex.Lock()
	return m.state
}

func (m *Mutex[T]) Unlock() {
	m.mutex.Unlock()
}
