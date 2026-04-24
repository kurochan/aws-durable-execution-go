package durable

import (
	"context"
	"fmt"
	"sync"
)

// ExecutionContext holds durable execution state for one wrapped handler
// invocation.
//
// Most applications interact with DurableContext instead of ExecutionContext.
type ExecutionContext struct {
	durableExecutionClient DurableExecutionClient
	stepData               map[string]Operation // hashed IDs from backend
	stepDataMu             *sync.RWMutex
	terminationManager     *TerminationManager
	durableExecutionArn    string
	pendingCompletions     map[string]struct{}
	requestID              string
	tenantID               string
}

// DurableExecutionClient returns the backend client for this execution.
func (e *ExecutionContext) DurableExecutionClient() DurableExecutionClient {
	return e.durableExecutionClient
}

// TerminationManager returns the invocation termination manager.
func (e *ExecutionContext) TerminationManager() *TerminationManager { return e.terminationManager }

// DurableExecutionArn returns the durable execution ARN for this invocation.
func (e *ExecutionContext) DurableExecutionArn() string { return e.durableExecutionArn }

// GetStepData returns checkpoint data for a durable operation ID.
//
// stepID is the SDK-generated unhashed operation ID. The lookup hashes it to
// match backend operation IDs.
func (e *ExecutionContext) GetStepData(stepID string) *Operation {
	hashed := HashID(stepID)
	e.stepDataMu.RLock()
	op, ok := e.stepData[hashed]
	e.stepDataMu.RUnlock()
	if !ok {
		return nil
	}
	copy := op
	return &copy
}

func (e *ExecutionContext) setStepData(op Operation) {
	if op.ID == "" {
		return
	}
	e.stepDataMu.Lock()
	e.stepData[op.ID] = op
	e.stepDataMu.Unlock()
}

// InitializeExecutionContext constructs an ExecutionContext from InvocationInput
// and loads any additional paginated backend state.
func InitializeExecutionContext(ctx context.Context, input InvocationInput, client DurableExecutionClient, requestID, tenantID string) (*ExecutionContext, DurableExecutionMode, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if input.DurableExecutionArn == "" || input.CheckpointToken == "" {
		return nil, "", "", fmt.Errorf("unexpected payload provided to start durable execution")
	}
	if client == nil {
		return nil, "", "", fmt.Errorf("durable execution client is required")
	}

	operations := make([]Operation, 0, len(input.InitialExecutionState.Operations))
	operations = append(operations, input.InitialExecutionState.Operations...)
	marker := input.InitialExecutionState.NextMarker
	for marker != "" {
		resp, err := client.GetExecutionState(ctx, GetExecutionStateRequest{
			DurableExecutionArn: input.DurableExecutionArn,
			CheckpointToken:     input.CheckpointToken,
			Marker:              marker,
			MaxItems:            1000,
		})
		if err != nil {
			return nil, "", "", err
		}
		operations = append(operations, resp.Operations...)
		marker = resp.NextMarker
	}

	mode := ExecutionMode
	if len(operations) > 1 {
		mode = ReplayMode
	}

	stepData := make(map[string]Operation)
	for _, op := range operations {
		if op.ID == "" {
			continue
		}
		stepData[op.ID] = op
	}

	ec := &ExecutionContext{
		durableExecutionClient: client,
		stepData:               stepData,
		stepDataMu:             &sync.RWMutex{},
		terminationManager:     NewTerminationManager(),
		durableExecutionArn:    input.DurableExecutionArn,
		pendingCompletions:     map[string]struct{}{},
		requestID:              requestID,
		tenantID:               tenantID,
	}
	return ec, mode, input.CheckpointToken, nil
}
