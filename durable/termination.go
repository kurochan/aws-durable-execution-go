package durable

import "sync"

// TerminationDetails describes why the current invocation should stop early.
type TerminationDetails struct {
	Reason  TerminationReason
	Message string
	Error   error
}

// TerminationManager coordinates early termination across concurrent durable
// operations in one invocation.
type TerminationManager struct {
	mu         sync.Mutex
	done       chan TerminationDetails
	terminated bool
	details    TerminationDetails
}

// NewTerminationManager creates an unterminated manager.
func NewTerminationManager() *TerminationManager {
	return &TerminationManager{done: make(chan TerminationDetails, 1)}
}

// Terminate records termination details and notifies waiters once.
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

// Channel is closed by sending the first termination details.
func (m *TerminationManager) Channel() <-chan TerminationDetails {
	return m.done
}

// IsTerminated reports whether Terminate has been called.
func (m *TerminationManager) IsTerminated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminated
}

// Details returns termination details and whether termination has happened.
func (m *TerminationManager) Details() (TerminationDetails, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.details, m.terminated
}
