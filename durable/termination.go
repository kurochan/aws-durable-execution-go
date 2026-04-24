package durable

import "sync"

type TerminationDetails struct {
	Reason  TerminationReason
	Message string
	Error   error
}

type TerminationManager struct {
	mu         sync.Mutex
	done       chan TerminationDetails
	terminated bool
	details    TerminationDetails
}

func NewTerminationManager() *TerminationManager {
	return &TerminationManager{done: make(chan TerminationDetails, 1)}
}

func (m *TerminationManager) Terminate(details TerminationDetails) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminated {
		return
	}
	if details.Reason == "" {
		details.Reason = TerminationReasonOperationTerminated
	}
	if details.Message == "" {
		details.Message = "operation terminated"
	}
	m.terminated = true
	m.details = details
	m.done <- details
}

func (m *TerminationManager) Channel() <-chan TerminationDetails {
	return m.done
}

func (m *TerminationManager) IsTerminated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminated
}

func (m *TerminationManager) Details() (TerminationDetails, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.details, m.terminated
}
