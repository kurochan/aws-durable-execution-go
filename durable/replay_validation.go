package durable

import "fmt"

type operationDescriptor struct {
	Type    OperationType
	Name    string
	SubType OperationSubType
}

// ValidateReplayConsistency verifies that the current durable operation matches
// the checkpointed operation at the same call position.
func ValidateReplayConsistency(stepID string, current operationDescriptor, checkpointData *Operation, execCtx *ExecutionContext) error {
	if checkpointData == nil || checkpointData.Type == "" {
		return nil
	}
	if checkpointData.Type != current.Type {
		err := NewUnrecoverableExecutionError(TerminationReasonCustom,
			fmt.Sprintf("non-deterministic execution: operation type mismatch for step %q: expected %q, got %q", stepID, checkpointData.Type, current.Type), nil)
		execCtx.TerminationManager().Terminate(TerminationDetails{Reason: TerminationReasonCustom, Message: err.Error(), Error: err})
		return err
	}
	if checkpointData.Name != current.Name {
		err := NewUnrecoverableExecutionError(TerminationReasonCustom,
			fmt.Sprintf("non-deterministic execution: operation name mismatch for step %q: expected %q, got %q", stepID, checkpointData.Name, current.Name), nil)
		execCtx.TerminationManager().Terminate(TerminationDetails{Reason: TerminationReasonCustom, Message: err.Error(), Error: err})
		return err
	}
	if checkpointData.SubType != current.SubType {
		err := NewUnrecoverableExecutionError(TerminationReasonCustom,
			fmt.Sprintf("non-deterministic execution: operation subtype mismatch for step %q: expected %q, got %q", stepID, checkpointData.SubType, current.SubType), nil)
		execCtx.TerminationManager().Terminate(TerminationDetails{Reason: TerminationReasonCustom, Message: err.Error(), Error: err})
		return err
	}
	return nil
}
