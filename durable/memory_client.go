package durable

import (
	"sort"
	"sync"
	"time"
)

// InMemoryClient is a local DurableExecutionClient implementation for tests and local execution.
type InMemoryClient struct {
	mu         sync.Mutex
	tokenSeq   int
	operations map[string]Operation // key: hashed ID
}

func NewInMemoryClient() *InMemoryClient {
	return &InMemoryClient{
		tokenSeq:   1,
		operations: map[string]Operation{},
	}
}

func (c *InMemoryClient) GetExecutionState(_ GetExecutionStateRequest) (GetExecutionStateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshTimedStatusesLocked()
	ops := make([]Operation, 0, len(c.operations))
	for _, op := range c.operations {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID < ops[j].ID })
	return GetExecutionStateResponse{Operations: ops}, nil
}

func (c *InMemoryClient) Checkpoint(input CheckpointRequest) (CheckpointResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshTimedStatusesLocked()

	for _, u := range input.Updates {
		op := c.operations[u.ID]
		op.ID = u.ID
		op.ParentID = u.ParentID
		op.Type = u.Type
		op.SubType = u.SubType
		op.Name = u.Name
		now := time.Now()
		op.StartTimestamp = &now

		switch u.Action {
		case OperationActionStart:
			op.Status = OperationStatusStarted
			if u.Type == OperationTypeWait {
				end := now.Add(time.Duration(u.WaitOptions.WaitSeconds) * time.Second)
				op.WaitDetails = &WaitDetails{ScheduledEndTimestamp: &end}
			}
			if u.Type == OperationTypeChainedInvoke {
				// Local-memory behavior: complete invoke immediately with echo payload.
				op.Status = OperationStatusSucceeded
				op.ChainedInvokeDetails = &ChainedInvokeDetails{Result: u.Payload}
			}
			if u.Type == OperationTypeCallback {
				cbID := u.ID
				if op.CallbackDetails == nil {
					op.CallbackDetails = &CallbackDetails{}
				}
				op.CallbackDetails.CallbackID = cbID
				if u.CallbackOptions != nil && u.CallbackOptions.TimeoutSeconds > 0 {
					end := now.Add(time.Duration(u.CallbackOptions.TimeoutSeconds) * time.Second)
					op.CallbackDetails.ScheduledTimeoutTimestamp = &end
				}
			}
		case OperationActionSucceed:
			op.Status = OperationStatusSucceeded
			switch u.Type {
			case OperationTypeStep:
				op.StepDetails = &StepDetails{Result: u.Payload}
			case OperationTypeChainedInvoke:
				op.ChainedInvokeDetails = &ChainedInvokeDetails{Result: u.Payload}
			case OperationTypeCallback:
				if op.CallbackDetails == nil {
					op.CallbackDetails = &CallbackDetails{CallbackID: u.ID}
				}
				op.CallbackDetails.Result = u.Payload
			case OperationTypeContext:
				op.ContextDetails = &ContextDetails{Result: u.Payload}
				if u.ContextOptions != nil {
					op.ContextDetails.ReplayChildren = u.ContextOptions.ReplayChildren
				}
			case OperationTypeExecution:
				op.ExecutionDetails = &ExecutionDetails{InputPayload: u.Payload}
			}
		case OperationActionFail:
			op.Status = OperationStatusFailed
			switch u.Type {
			case OperationTypeStep:
				op.StepDetails = &StepDetails{Error: u.Error}
			case OperationTypeChainedInvoke:
				op.ChainedInvokeDetails = &ChainedInvokeDetails{Error: u.Error}
			case OperationTypeCallback:
				if op.CallbackDetails == nil {
					op.CallbackDetails = &CallbackDetails{CallbackID: u.ID}
				}
				op.CallbackDetails.Error = u.Error
			case OperationTypeContext:
				op.ContextDetails = &ContextDetails{Error: u.Error}
			}
		case OperationActionRetry:
			op.Status = OperationStatusPending
			delay := 1
			if u.StepOptions != nil && u.StepOptions.NextAttemptDelaySeconds > 0 {
				delay = u.StepOptions.NextAttemptDelaySeconds
			}
			next := now.Add(time.Duration(delay) * time.Second)
			if op.StepDetails == nil {
				op.StepDetails = &StepDetails{}
			}
			op.StepDetails.Attempt++
			op.StepDetails.NextAttemptTimestamp = &next
			op.StepDetails.Error = u.Error
			if u.Payload != "" {
				op.StepDetails.Result = u.Payload
			}
		}

		c.operations[u.ID] = op
	}

	c.tokenSeq++
	newToken := input.CheckpointToken + "-" + time.Now().Format("150405.000")
	ops := make([]Operation, 0, len(c.operations))
	for _, op := range c.operations {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID < ops[j].ID })

	return CheckpointResponse{
		CheckpointToken:   newToken,
		NewExecutionState: &ExecutionState{Operations: ops},
	}, nil
}

func (c *InMemoryClient) SetOperation(op Operation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations[op.ID] = op
}

func (c *InMemoryClient) CompleteCallback(callbackID string, payload string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	op := c.operations[callbackID]
	op.ID = callbackID
	op.Type = OperationTypeCallback
	op.SubType = OperationSubTypeCallback
	op.Status = OperationStatusSucceeded
	if op.CallbackDetails == nil {
		op.CallbackDetails = &CallbackDetails{CallbackID: callbackID}
	}
	op.CallbackDetails.CallbackID = callbackID
	op.CallbackDetails.Result = payload
	now := time.Now()
	op.StartTimestamp = &now
	c.operations[callbackID] = op
}

func (c *InMemoryClient) FailCallback(callbackID string, errObj *ErrorObject, timedOut bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	op := c.operations[callbackID]
	op.ID = callbackID
	op.Type = OperationTypeCallback
	op.SubType = OperationSubTypeCallback
	if timedOut {
		op.Status = OperationStatusTimedOut
	} else {
		op.Status = OperationStatusFailed
	}
	if op.CallbackDetails == nil {
		op.CallbackDetails = &CallbackDetails{CallbackID: callbackID}
	}
	op.CallbackDetails.CallbackID = callbackID
	op.CallbackDetails.Error = errObj
	now := time.Now()
	op.StartTimestamp = &now
	c.operations[callbackID] = op
}

func (c *InMemoryClient) refreshTimedStatusesLocked() {
	now := time.Now()
	for id, op := range c.operations {
		if op.Status == OperationStatusStarted && op.Type == OperationTypeWait && op.WaitDetails != nil && op.WaitDetails.ScheduledEndTimestamp != nil {
			if now.After(*op.WaitDetails.ScheduledEndTimestamp) {
				op.Status = OperationStatusSucceeded
				c.operations[id] = op
			}
		}
		if op.Status == OperationStatusPending && op.StepDetails != nil && op.StepDetails.NextAttemptTimestamp != nil {
			if now.After(*op.StepDetails.NextAttemptTimestamp) {
				op.Status = OperationStatusStarted
				c.operations[id] = op
			}
		}
		if op.Status == OperationStatusStarted && op.Type == OperationTypeCallback && op.CallbackDetails != nil && op.CallbackDetails.ScheduledTimeoutTimestamp != nil {
			if now.After(*op.CallbackDetails.ScheduledTimeoutTimestamp) {
				op.Status = OperationStatusTimedOut
				if op.CallbackDetails.Error == nil {
					op.CallbackDetails.Error = (&DurableOperationError{
						Type:    "CallbackTimeoutError",
						Message: "Callback timed out",
					}).ToErrorObject()
				}
				c.operations[id] = op
			}
		}
	}
}
