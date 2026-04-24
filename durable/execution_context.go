package durable

import (
	"fmt"
	"sync"
)

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

func (e *ExecutionContext) DurableExecutionClient() DurableExecutionClient {
	return e.durableExecutionClient
}
func (e *ExecutionContext) TerminationManager() *TerminationManager { return e.terminationManager }
func (e *ExecutionContext) DurableExecutionArn() string             { return e.durableExecutionArn }

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

func InitializeExecutionContext(input InvocationInput, client DurableExecutionClient, requestID, tenantID string) (*ExecutionContext, DurableExecutionMode, string, error) {
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
		resp, err := client.GetExecutionState(GetExecutionStateRequest{
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
