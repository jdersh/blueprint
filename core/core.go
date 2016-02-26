package core

import (
	"log"
	"sync"
)

// Subprocess represents something that can be set up, started, and stopped. E.g. a server.
type Subprocess interface {
	Setup() error
	Start()
	Stop()
}

// SubprocessManager oversees multiple subprocesses.
type SubprocessManager struct {
	Processes []Subprocess
	wg        *sync.WaitGroup
}

// Start all subprocesses. If any return an error, exits the process.
func (s *SubprocessManager) Start() {
	var wg sync.WaitGroup
	for _, sp := range s.Processes {
		err := sp.Setup()
		if err != nil {
			log.Fatal("Failed to set up subprocess", sp, err)
		}

		fn := sp
		wg.Add(1)
		go func() {
			fn.Start()
			wg.Done()
		}()
	}
	s.wg = &wg
}

// Wait for all subprocesses to finish, i.e. return from their Start method.
func (s *SubprocessManager) Wait() {
	s.wg.Wait()
}

// Stop all subprocesses.
func (s *SubprocessManager) Stop() {
	for _, sp := range s.Processes {
		sp.Stop()
	}
}
