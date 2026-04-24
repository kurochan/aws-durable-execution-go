package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const lambdaResponseSizeLimit = 6*1024*1024 - 50

// DurableExecutionHandler is the user handler executed inside a durable
// invocation.
//
// event is the customer payload extracted from the execution operation in the
// invocation input. dctx is used to schedule durable operations. The handler
// should call Await on futures whose results are required before returning.
type DurableExecutionHandler func(ctx context.Context, event any, dctx *DurableContext) (any, error)

// DurableExecutionConfig configures WithDurableExecution.
type DurableExecutionConfig struct {
	// Client persists and loads durable execution state.
	Client DurableExecutionClient
	// Logger receives SDK log messages. NopLogger is used when Logger is nil.
	Logger Logger
	// RequestID is optional metadata for the current platform invocation.
	RequestID string
	// TenantID is optional metadata for multi-tenant deployments.
	TenantID string
}

// WrappedHandler is the handler shape returned by WithDurableExecution.
type WrappedHandler func(ctx context.Context, input InvocationInput) (InvocationOutput, error)

// WithDurableExecution wraps a DurableExecutionHandler with replay,
// checkpointing, and pending-status handling.
//
// The returned handler consumes InvocationInput from the durable backend. It
// returns InvocationStatusPending when execution stopped because a durable wait,
// callback, retry timer, or child operation is still pending. Infrastructure and
// unrecoverable invocation errors are returned as Go errors.
func WithDurableExecution(handler DurableExecutionHandler, config DurableExecutionConfig) WrappedHandler {
	return func(ctx context.Context, input InvocationInput) (InvocationOutput, error) {
		execCtx, mode, checkpointToken, err := InitializeExecutionContext(ctx, input, config.Client, config.RequestID, config.TenantID)
		if err != nil {
			return InvocationOutput{}, err
		}
		logger := config.Logger
		if logger == nil {
			logger = NopLogger{}
		}

		checkpoint := NewCheckpointManager(execCtx.DurableExecutionArn(), execCtx.stepData, execCtx.stepDataMu, execCtx.DurableExecutionClient(), execCtx.TerminationManager(), checkpointToken, logger)
		dctx := NewDurableContext(execCtx, mode, logger, "", "", checkpoint)

		customerEvent := extractCustomerEvent(input)

		type handlerResult struct {
			value any
			err   error
		}
		handlerCh := make(chan handlerResult, 1)
		go func() {
			activeCtx := WithActiveOperation(ctx, ActiveOperation{ContextID: "root", ParentID: "", Mode: mode})
			res, herr := handler(activeCtx, customerEvent, dctx)
			handlerCh <- handlerResult{value: res, err: herr}
		}()

		select {
		case term := <-execCtx.TerminationManager().Channel():
			checkpoint.SetTerminating()
			_ = checkpoint.WaitForQueueCompletion(ctx)

			switch term.Reason {
			case TerminationReasonCheckpointFailed:
				if term.Error != nil {
					return InvocationOutput{}, term.Error
				}
				return InvocationOutput{}, errors.New(term.Message)
			case TerminationReasonSerdesFailed:
				if term.Error != nil {
					return InvocationOutput{}, term.Error
				}
				return InvocationOutput{}, errors.New(term.Message)
			case TerminationReasonContextValidation:
				return InvocationOutput{Status: InvocationStatusFailed, Error: CreateErrorObjectFromError(term.Error, "")}, nil
			default:
				return InvocationOutput{Status: InvocationStatusPending}, nil
			}
		case result := <-handlerCh:
			_ = checkpoint.WaitForQueueCompletion(ctx)
			if result.err != nil {
				if IsUnrecoverableInvocationError(result.err) {
					return InvocationOutput{}, result.err
				}
				return InvocationOutput{Status: InvocationStatusFailed, Error: CreateErrorObjectFromError(result.err, "")}, nil
			}

			serialized, err := json.Marshal(result.value)
			if err != nil {
				errObj := CreateErrorObjectFromError(err, "")
				return InvocationOutput{Status: InvocationStatusFailed, Error: errObj}, nil
			}

			if len(serialized) > lambdaResponseSizeLimit {
				stepID := fmt.Sprintf("execution-result-%d", time.Now().UnixNano())
				if err := checkpoint.Checkpoint(ctx, stepID, OperationUpdate{
					ID:      stepID,
					Action:  OperationActionSucceed,
					Type:    OperationTypeExecution,
					Payload: string(serialized),
				}); err != nil {
					return InvocationOutput{}, err
				}
				_ = checkpoint.WaitForQueueCompletion(ctx)
				return InvocationOutput{Status: InvocationStatusSucceeded, Result: ""}, nil
			}
			return InvocationOutput{Status: InvocationStatusSucceeded, Result: string(serialized)}, nil
		}
	}
}

func extractCustomerEvent(input InvocationInput) any {
	for _, op := range input.InitialExecutionState.Operations {
		if op.ExecutionDetails != nil && op.ExecutionDetails.InputPayload != "" {
			var v any
			if err := json.Unmarshal([]byte(op.ExecutionDetails.InputPayload), &v); err == nil {
				return v
			}
		}
	}
	return map[string]any{}
}
