package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const lambdaResponseSizeLimit = 6*1024*1024 - 50

type DurableExecutionHandler func(ctx context.Context, event any, dctx *DurableContext) (any, error)

type DurableExecutionConfig struct {
	Client    DurableExecutionClient
	Logger    Logger
	RequestID string
	TenantID  string
}

type WrappedHandler func(ctx context.Context, input InvocationInput) (InvocationOutput, error)

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
