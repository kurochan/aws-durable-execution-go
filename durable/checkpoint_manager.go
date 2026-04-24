package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	maxCheckpointPayloadSize = 750 * 1024
	maxCheckpointItems       = 250
	terminationCooldown      = 20 * time.Millisecond
)

type queuedCheckpoint struct {
	ctx    context.Context
	stepID string
	update OperationUpdate
	done   chan error
}

type queuedForceCheckpoint struct {
	ctx  context.Context
	done chan error
}

type operationInfo struct {
	state        OperationLifecycleState
	metadata     OperationMetadata
	endTimestamp *time.Time
}

type CheckpointManager struct {
	mu sync.Mutex

	durableExecutionArn string
	stepData            map[string]Operation
	stepDataMu          *sync.RWMutex
	client              DurableExecutionClient
	termination         *TerminationManager
	currentToken        string
	logger              Logger

	queue      []queuedCheckpoint
	forceQueue []queuedForceCheckpoint
	processing bool

	terminating bool

	operations        map[string]*operationInfo // key: original step ID
	finishedAncestors map[string]struct{}

	terminationTimer  *time.Timer
	terminationReason TerminationReason
}

func NewCheckpointManager(
	durableExecutionArn string,
	stepData map[string]Operation,
	stepDataMu *sync.RWMutex,
	client DurableExecutionClient,
	termination *TerminationManager,
	checkpointToken string,
	logger Logger,
) *CheckpointManager {
	if logger == nil {
		logger = NopLogger{}
	}
	return &CheckpointManager{
		durableExecutionArn: durableExecutionArn,
		stepData:            stepData,
		stepDataMu:          stepDataMu,
		client:              client,
		termination:         termination,
		currentToken:        checkpointToken,
		logger:              logger,
		operations:          make(map[string]*operationInfo),
		finishedAncestors:   make(map[string]struct{}),
	}
}

func (m *CheckpointManager) SetTerminating() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminating = true
}

func (m *CheckpointManager) MarkAncestorFinished(stepID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finishedAncestors[stepID] = struct{}{}
}

func (m *CheckpointManager) getParentID(stepID string) string {
	for i := len(stepID) - 1; i >= 0; i-- {
		if stepID[i] == '-' {
			return stepID[:i]
		}
	}
	return ""
}

func (m *CheckpointManager) hasFinishedAncestor(stepID string) bool {
	parent := m.getParentID(stepID)
	for parent != "" {
		if _, ok := m.finishedAncestors[parent]; ok {
			return true
		}
		parent = m.getParentID(parent)
	}
	return false
}

func (m *CheckpointManager) Checkpoint(ctx context.Context, stepID string, update OperationUpdate) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	if m.terminating || m.hasFinishedAncestor(stepID) {
		m.mu.Unlock()
		return fmt.Errorf("checkpoint skipped due to termination/finished ancestor")
	}
	done := make(chan error, 1)
	m.queue = append(m.queue, queuedCheckpoint{ctx: ctx, stepID: stepID, update: update, done: done})
	if !m.processing {
		m.processing = true
		go m.processLoop()
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (m *CheckpointManager) ForceCheckpoint(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.terminating {
		m.mu.Unlock()
		return fmt.Errorf("force checkpoint skipped due to termination")
	}
	done := make(chan error, 1)
	m.forceQueue = append(m.forceQueue, queuedForceCheckpoint{ctx: ctx, done: done})
	if !m.processing {
		m.processing = true
		go m.processLoop()
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (m *CheckpointManager) WaitForQueueCompletion(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		done := len(m.queue) == 0 && len(m.forceQueue) == 0 && !m.processing
		m.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *CheckpointManager) processLoop() {
	for {
		batch, force, ok := m.nextBatch()
		if !ok {
			m.mu.Lock()
			m.processing = false
			m.mu.Unlock()
			return
		}

		err := m.processBatch(batch, force)
		if err != nil {
			classified := ClassifyCheckpointError(err)
			m.termination.Terminate(TerminationDetails{
				Reason:  TerminationReasonCheckpointFailed,
				Message: classified.Error(),
				Error:   classified,
			})
			for _, it := range batch {
				it.done <- classified
			}
			for _, f := range force {
				f.done <- classified
			}
			m.failAllPending(classified)
			m.mu.Lock()
			m.processing = false
			m.mu.Unlock()
			return
		}

		for _, it := range batch {
			it.done <- nil
		}
		for _, f := range force {
			f.done <- nil
		}
	}
}

func (m *CheckpointManager) failAllPending(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, it := range m.queue {
		it.done <- err
	}
	for _, f := range m.forceQueue {
		f.done <- err
	}
	m.queue = nil
	m.forceQueue = nil
}

func (m *CheckpointManager) nextBatch() ([]queuedCheckpoint, []queuedForceCheckpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queue) == 0 && len(m.forceQueue) == 0 {
		return nil, nil, false
	}

	baseSize := len(m.currentToken) + 100
	currentSize := baseSize
	batch := make([]queuedCheckpoint, 0, maxCheckpointItems)

	for len(m.queue) > 0 {
		next := m.queue[0]
		payload, _ := json.Marshal(next.update)
		sz := len(payload) + len(next.stepID) + 64
		if len(batch) > 0 && (currentSize+sz > maxCheckpointPayloadSize || len(batch) >= maxCheckpointItems) {
			break
		}
		batch = append(batch, next)
		m.queue = m.queue[1:]
		currentSize += sz
	}

	force := m.forceQueue
	m.forceQueue = nil
	return batch, force, true
}

func checkpointContext(batch []queuedCheckpoint, force []queuedForceCheckpoint) context.Context {
	for _, item := range batch {
		if item.ctx != nil {
			return item.ctx
		}
	}
	for _, item := range force {
		if item.ctx != nil {
			return item.ctx
		}
	}
	return context.Background()
}

func (m *CheckpointManager) processBatch(batch []queuedCheckpoint, force []queuedForceCheckpoint) error {
	updates := make([]OperationUpdate, 0, len(batch))
	for _, item := range batch {
		u := item.update
		u.ID = HashID(item.stepID)
		if item.update.ParentID != "" {
			u.ParentID = HashID(item.update.ParentID)
		}
		if u.Type == "" {
			u.Type = OperationTypeStep
		}
		if u.Action == "" {
			u.Action = OperationActionStart
		}
		updates = append(updates, u)
	}

	resp, err := m.client.Checkpoint(checkpointContext(batch, force), CheckpointRequest{
		DurableExecutionArn: m.durableExecutionArn,
		CheckpointToken:     m.currentToken,
		Updates:             updates,
	})
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if resp.CheckpointToken != "" {
		m.currentToken = resp.CheckpointToken
	}
	if resp.NewExecutionState != nil {
		m.stepDataMu.Lock()
		for _, op := range resp.NewExecutionState.Operations {
			if op.ID == "" {
				continue
			}
			m.stepData[op.ID] = op
		}
		m.stepDataMu.Unlock()
	}
	return nil
}

func (m *CheckpointManager) getOperationStatus(stepID string) OperationStatus {
	m.stepDataMu.RLock()
	defer m.stepDataMu.RUnlock()
	op, ok := m.stepData[HashID(stepID)]
	if !ok {
		return ""
	}
	return op.Status
}

func isTerminalStatus(status OperationStatus) bool {
	switch status {
	case OperationStatusSucceeded, OperationStatusFailed, OperationStatusCancelled, OperationStatusStopped, OperationStatusTimedOut:
		return true
	default:
		return false
	}
}

func (m *CheckpointManager) MarkOperationState(stepID string, state OperationLifecycleState, metadata OperationMetadata, endTimestamp *time.Time) {
	m.mu.Lock()
	op := m.operations[stepID]
	if op == nil {
		op = &operationInfo{metadata: metadata}
		m.operations[stepID] = op
	}
	op.state = state
	if endTimestamp != nil {
		t := *endTimestamp
		op.endTimestamp = &t
	}
	m.mu.Unlock()

	if state != OperationLifecycleIdleNotAwaited {
		m.checkAndMaybeTerminate()
	}
}

func (m *CheckpointManager) MarkOperationAwaited(stepID string) {
	m.mu.Lock()
	op := m.operations[stepID]
	if op == nil {
		m.mu.Unlock()
		return
	}
	if op.state == OperationLifecycleIdleNotAwaited {
		op.state = OperationLifecycleIdleAwaited
	}
	m.mu.Unlock()
	m.checkAndMaybeTerminate()
}

func (m *CheckpointManager) GetOperationState(stepID string) OperationLifecycleState {
	m.mu.Lock()
	defer m.mu.Unlock()
	op := m.operations[stepID]
	if op == nil {
		return ""
	}
	return op.state
}

func (m *CheckpointManager) WaitForRetryTimer(ctx context.Context, stepID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	op := m.operations[stepID]
	if op == nil {
		m.mu.Unlock()
		return fmt.Errorf("operation %s not found", stepID)
	}
	if op.state != OperationLifecycleRetryWaiting {
		m.mu.Unlock()
		return fmt.Errorf("operation %s must be RETRY_WAITING, got %s", stepID, op.state)
	}
	endTS := op.endTimestamp
	m.mu.Unlock()

	if isTerminalStatus(m.getOperationStatus(stepID)) {
		return nil
	}
	if endTS != nil {
		delay := time.Until(*endTS)
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return m.WaitForStatusChange(ctx, stepID)
}

func (m *CheckpointManager) WaitForStatusChange(ctx context.Context, stepID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	op := m.operations[stepID]
	if op == nil {
		m.mu.Unlock()
		return fmt.Errorf("operation %s not found", stepID)
	}
	if op.state != OperationLifecycleIdleAwaited && op.state != OperationLifecycleRetryWaiting {
		m.mu.Unlock()
		return fmt.Errorf("operation %s must be IDLE_AWAITED/RETRY_WAITING, got %s", stepID, op.state)
	}
	m.mu.Unlock()

	if isTerminalStatus(m.getOperationStatus(stepID)) {
		return nil
	}

	delay := time.Second
	start := time.Now()
	for {
		if time.Since(start) > 15*time.Minute {
			return nil
		}
		if err := m.ForceCheckpoint(ctx); err != nil {
			return err
		}
		if isTerminalStatus(m.getOperationStatus(stepID)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < 10*time.Second {
			delay += time.Second
		}
	}
}

func (m *CheckpointManager) checkAndMaybeTerminate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queue) > 0 || len(m.forceQueue) > 0 || m.processing {
		m.abortTerminationLocked()
		return
	}

	hasExecuting := false
	hasWaiting := false
	var reason TerminationReason

	for stepID, op := range m.operations {
		if op.state == OperationLifecycleExecuting {
			hasExecuting = true
			break
		}
		if op.state == OperationLifecycleRetryWaiting || op.state == OperationLifecycleIdleNotAwaited || op.state == OperationLifecycleIdleAwaited {
			if m.hasFinishedAncestor(stepID) {
				delete(m.operations, stepID)
				continue
			}
			hasWaiting = true
			if reason == "" {
				switch {
				case op.state == OperationLifecycleRetryWaiting && op.metadata.SubType == OperationSubTypeStep:
					reason = TerminationReasonRetryScheduled
				case op.metadata.SubType == OperationSubTypeWait:
					reason = TerminationReasonWaitScheduled
				default:
					reason = TerminationReasonCallbackPending
				}
			}
		}
	}

	if hasExecuting || !hasWaiting {
		m.abortTerminationLocked()
		return
	}

	if reason == "" {
		reason = TerminationReasonCallbackPending
	}
	m.scheduleTerminationLocked(reason)
}

func (m *CheckpointManager) abortTerminationLocked() {
	if m.terminationTimer != nil {
		m.terminationTimer.Stop()
		m.terminationTimer = nil
		m.terminationReason = ""
	}
}

func (m *CheckpointManager) scheduleTerminationLocked(reason TerminationReason) {
	if m.terminationTimer != nil && m.terminationReason == reason {
		return
	}
	m.abortTerminationLocked()
	m.terminationReason = reason
	m.terminationTimer = time.AfterFunc(terminationCooldown, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if len(m.queue) > 0 || len(m.forceQueue) > 0 || m.processing {
			m.abortTerminationLocked()
			return
		}
		m.termination.Terminate(TerminationDetails{Reason: reason, Message: string(reason)})
		m.abortTerminationLocked()
	})
}
