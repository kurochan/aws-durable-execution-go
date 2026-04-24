package durable

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func callbackErrorFromOperation(op *Operation) error {
	if op == nil {
		return NewCallbackError("callback failed", nil, "")
	}
	if op.CallbackDetails != nil && op.CallbackDetails.Error != nil {
		msg := op.CallbackDetails.Error.ErrorMessage
		data := op.CallbackDetails.Error.ErrorData
		if op.Status == OperationStatusTimedOut {
			return NewCallbackTimeoutError(msg, nil, data)
		}
		return NewCallbackError(msg, nil, data)
	}
	if op.Status == OperationStatusTimedOut {
		return NewCallbackTimeoutError("callback timed out", nil, "")
	}
	return NewCallbackError("callback failed", nil, "")
}

func (c *DurableContext) callbackPromise(stepID, name string, serdes Serdes) *Future[any] {
	if serdes == nil {
		serdes = PassThroughSerdes{}
	}
	return NewFuture(func(awaitCtx context.Context) (any, error) {
		c.checkpoint.MarkOperationAwaited(stepID)
		if err := c.checkpoint.WaitForStatusChange(awaitCtx, stepID); err != nil {
			return nil, err
		}
		stepData := c.execCtx.GetStepData(stepID)
		metadata := OperationMetadata{
			StepID:   stepID,
			Name:     name,
			Type:     OperationTypeCallback,
			SubType:  OperationSubTypeCallback,
			ParentID: c.parentID,
		}
		c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
		if stepData != nil && stepData.Status == OperationStatusSucceeded && stepData.CallbackDetails != nil {
			return SafeDeserialize(serdes, stepData.CallbackDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		}
		return nil, callbackErrorFromOperation(stepData)
	})
}

func (c *DurableContext) callbackResolvedOrRejectedFuture(stepID, name string, serdes Serdes) (*Future[any], string, error) {
	if serdes == nil {
		serdes = PassThroughSerdes{}
	}
	stepData := c.execCtx.GetStepData(stepID)
	if stepData == nil || stepData.CallbackDetails == nil || stepData.CallbackDetails.CallbackID == "" {
		return nil, "", NewCallbackError(fmt.Sprintf("no callback ID found for callback: %s", stepID), nil, "")
	}
	callbackID := stepData.CallbackDetails.CallbackID
	if stepData.Status == OperationStatusSucceeded {
		v, err := SafeDeserialize(serdes, stepData.CallbackDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		if err != nil {
			return nil, "", err
		}
		return NewResolvedFuture(v), callbackID, nil
	}
	return NewRejectedFuture[any](callbackErrorFromOperation(stepData)), callbackID, nil
}

// CreateCallback creates a durable callback operation and returns its callback
// ID plus a promise for the eventual callback result.
//
// The callback ID is intended to be given to an external system. The returned
// promise completes when the durable backend records callback success, failure,
// or timeout.
func (c *DurableContext) CreateCallback(ctx context.Context, name string, cfg *CreateCallbackConfig) *Future[CreateCallbackResult] {
	ValidateContextUsage(ctx, c.stepPrefix, "createCallback", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[CreateCallbackResult] {
		stepID := c.createStepID()
		phaseDone := make(chan struct{})
		var phaseErr error
		var callbackID string
		var callbackPromise *Future[any]
		var serdes Serdes = PassThroughSerdes{}
		if cfg != nil && cfg.Serdes != nil {
			serdes = cfg.Serdes
		}

		go func() {
			defer close(phaseDone)
			stepData := c.execCtx.GetStepData(stepID)
			if err := ValidateReplayConsistency(stepID, operationDescriptor{Type: OperationTypeCallback, Name: name, SubType: OperationSubTypeCallback}, stepData, c.execCtx); err != nil {
				phaseErr = err
				return
			}

			metadata := OperationMetadata{
				StepID:   stepID,
				Name:     name,
				Type:     OperationTypeCallback,
				SubType:  OperationSubTypeCallback,
				ParentID: c.parentID,
			}
			if stepData != nil {
				switch stepData.Status {
				case OperationStatusSucceeded, OperationStatusFailed, OperationStatusTimedOut:
					c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
					callbackPromise, callbackID, phaseErr = c.callbackResolvedOrRejectedFuture(stepID, name, serdes)
					return
				}
			}

			if stepData == nil {
				update := OperationUpdate{
					ID:       stepID,
					ParentID: c.parentID,
					Action:   OperationActionStart,
					SubType:  OperationSubTypeCallback,
					Type:     OperationTypeCallback,
					Name:     name,
				}
				if cfg != nil {
					update.CallbackOptions = &CallbackOptions{}
					if cfg.Timeout != nil {
						update.CallbackOptions.TimeoutSeconds = cfg.Timeout.ToSeconds()
					}
					if cfg.HeartbeatTimeout != nil {
						update.CallbackOptions.HeartbeatTimeoutSeconds = cfg.HeartbeatTimeout.ToSeconds()
					}
				}
				if err := c.checkpoint.Checkpoint(ctx, stepID, update); err != nil {
					phaseErr = err
					return
				}
				stepData = c.execCtx.GetStepData(stepID)
			}

			if stepData == nil || stepData.CallbackDetails == nil || stepData.CallbackDetails.CallbackID == "" {
				phaseErr = NewCallbackError(fmt.Sprintf("no callback ID found for started callback: %s", stepID), nil, "")
				return
			}

			callbackID = stepData.CallbackDetails.CallbackID
			callbackPromise = c.callbackPromise(stepID, name, serdes)
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleIdleNotAwaited, metadata, nil)
		}()

		return NewFuture(func(awaitCtx context.Context) (CreateCallbackResult, error) {
			select {
			case <-awaitCtx.Done():
				return CreateCallbackResult{}, awaitCtx.Err()
			case <-phaseDone:
			}
			if phaseErr != nil {
				return CreateCallbackResult{}, phaseErr
			}
			if callbackPromise == nil {
				return CreateCallbackResult{}, NewCallbackError("failed to initialize callback promise", nil, "")
			}
			return CreateCallbackResult{
				Promise:    callbackPromise,
				CallbackID: callbackID,
			}, nil
		})
	})
}

// WaitForCallback creates a callback, runs submitter in a durable step, and
// waits for the callback result.
//
// submitter receives the callback ID and should send it to the external system
// that will complete the callback. submitter failures follow cfg.RetryStrategy
// when provided.
func (c *DurableContext) WaitForCallback(ctx context.Context, name string, submitter WaitForCallbackSubmitterFunc, cfg *WaitForCallbackConfig) *Future[any] {
	ValidateContextUsage(ctx, c.stepPrefix, "waitForCallback", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[any] {
		phaseDone := make(chan struct{})
		var phaseErr error
		var rawResult any
		stepID := c.nextStepID()

		go func() {
			defer close(phaseDone)
			if submitter == nil {
				phaseErr = errors.New("waitForCallback requires a submitter function")
				return
			}
			var childCfg *ChildConfig
			var createCfg *CreateCallbackConfig
			if cfg != nil {
				createCfg = &CreateCallbackConfig{
					Timeout:          cfg.Timeout,
					HeartbeatTimeout: cfg.HeartbeatTimeout,
				}
			}
			childCfg = &ChildConfig{
				SubType: OperationSubTypeWaitForCallback,
				ErrorMapper: func(err error) error {
					var de *DurableOperationError
					if errors.As(err, &de) {
						switch de.Type {
						case "CallbackError", "CallbackTimeoutError":
							return err
						case "StepError":
							return NewCallbackSubmitterError(de.Message, err, de.Data)
						}
					}
					return NewChildContextError(err.Error(), err, "")
				},
			}

			result, err := c.RunInChildContext(ctx, name, func(childCtx context.Context, child *DurableContext) (any, error) {
				cb, err := child.CreateCallback(childCtx, "", createCfg).Await(childCtx)
				if err != nil {
					return nil, err
				}
				stepCfg := &StepConfig{}
				if cfg != nil && cfg.RetryStrategy != nil {
					stepCfg.RetryStrategy = cfg.RetryStrategy
				}
				_, err = child.Step(childCtx, "callback-submitter", func(_ context.Context, stepCtx StepContext) (any, error) {
					return nil, submitter(cb.CallbackID, WaitForCallbackContext{Logger: stepCtx.Logger})
				}, stepCfg).Await(childCtx)
				if err != nil {
					return nil, err
				}
				return cb.Promise.Await(childCtx)
			}, childCfg).Await(ctx)
			if err != nil {
				phaseErr = err
				return
			}
			rawResult = result
		}()

		return NewFuture(func(awaitCtx context.Context) (any, error) {
			select {
			case <-awaitCtx.Done():
				return nil, awaitCtx.Err()
			case <-phaseDone:
			}
			if phaseErr != nil {
				return nil, phaseErr
			}

			serdes := Serdes(PassThroughSerdes{})
			if cfg != nil && cfg.Serdes != nil {
				serdes = cfg.Serdes
			}

			if rawResult == nil {
				return nil, nil
			}
			if s, ok := rawResult.(string); ok {
				return SafeDeserialize(serdes, s, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			}
			serialized, err := SafeSerialize(JSONSerdes{}, rawResult, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			if err != nil {
				return nil, err
			}
			return SafeDeserialize(serdes, serialized, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		})
	})
}

// WaitForCondition repeatedly runs check until cfg.WaitStrategy decides to
// stop waiting.
//
// The state returned by check is checkpointed between attempts. When the wait
// strategy returns ShouldContinue, the SDK schedules a retry delay and the
// invocation becomes pending until the backend resumes it.
func (c *DurableContext) WaitForCondition(ctx context.Context, name string, check WaitForConditionCheckFunc, cfg *WaitForConditionConfig) *Future[any] {
	ValidateContextUsage(ctx, c.stepPrefix, "waitForCondition", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[any] {
		stepID := c.createStepID()
		phaseDone := make(chan struct{})
		var phaseResult any
		var phaseErr error

		go func() {
			defer close(phaseDone)
			phaseResult, phaseErr = c.executeWaitForConditionPhase1(ctx, stepID, name, check, cfg)
		}()

		return NewFuture(func(awaitCtx context.Context) (any, error) {
			c.checkpoint.MarkOperationAwaited(stepID)
			select {
			case <-awaitCtx.Done():
				return nil, awaitCtx.Err()
			case <-phaseDone:
				return phaseResult, phaseErr
			}
		})
	})
}

func (c *DurableContext) executeWaitForConditionPhase1(ctx context.Context, stepID, name string, check WaitForConditionCheckFunc, cfg *WaitForConditionConfig) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg == nil || cfg.WaitStrategy == nil {
		return nil, errors.New("waitForCondition requires config with waitStrategy and initialState")
	}
	if check == nil {
		return nil, errors.New("waitForCondition requires a check function")
	}

	serdes := cfg.Serdes
	if serdes == nil {
		serdes = JSONSerdes{}
	}

	metadata := OperationMetadata{
		StepID:   stepID,
		Name:     name,
		Type:     OperationTypeStep,
		SubType:  OperationSubTypeWaitForCondition,
		ParentID: c.parentID,
	}

	for {
		stepData := c.execCtx.GetStepData(stepID)
		if err := ValidateReplayConsistency(stepID, operationDescriptor{Type: OperationTypeStep, Name: name, SubType: OperationSubTypeWaitForCondition}, stepData, c.execCtx); err != nil {
			return nil, err
		}

		if stepData != nil {
			switch stepData.Status {
			case OperationStatusSucceeded:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
				if stepData.StepDetails != nil {
					return SafeDeserialize(serdes, stepData.StepDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
				}
				return nil, nil
			case OperationStatusFailed:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
				if stepData.StepDetails != nil && stepData.StepDetails.Error != nil {
					return nil, DurableOperationErrorFromErrorObject(stepData.StepDetails.Error)
				}
				return nil, NewWaitForConditionError("waitForCondition failed", nil, "")
			case OperationStatusPending:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleRetryWaiting, metadata, stepData.StepDetails.NextAttemptTimestamp)
				if err := c.checkpoint.WaitForRetryTimer(ctx, stepID); err != nil {
					return nil, err
				}
				continue
			}
		}

		currentState := cfg.InitialState
		if stepData != nil &&
			(stepData.Status == OperationStatusStarted || stepData.Status == OperationStatusReady) &&
			stepData.StepDetails != nil &&
			stepData.StepDetails.Result != "" {
			des, err := SafeDeserialize(serdes, stepData.StepDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			if err == nil {
				currentState = des
			}
		}

		attempt := 1
		if stepData != nil && stepData.StepDetails != nil && stepData.StepDetails.Attempt > 0 {
			attempt = stepData.StepDetails.Attempt + 1
		}

		if stepData == nil || stepData.Status != OperationStatusStarted {
			if err := c.checkpoint.Checkpoint(ctx, stepID, OperationUpdate{
				ID:       stepID,
				ParentID: c.parentID,
				Action:   OperationActionStart,
				SubType:  OperationSubTypeWaitForCondition,
				Type:     OperationTypeStep,
				Name:     name,
			}); err != nil {
				return nil, err
			}
		}

		c.checkpoint.MarkOperationState(stepID, OperationLifecycleExecuting, metadata, nil)
		nextState, err := check(currentState, WaitForConditionContext{Logger: c.logger})
		if err != nil {
			errObj := CreateErrorObjectFromError(err, "")
			_ = c.checkpoint.Checkpoint(ctx, stepID, OperationUpdate{
				ID:       stepID,
				ParentID: c.parentID,
				Action:   OperationActionFail,
				SubType:  OperationSubTypeWaitForCondition,
				Type:     OperationTypeStep,
				Error:    errObj,
				Name:     name,
			})
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
			return nil, NewWaitForConditionError(err.Error(), err, "")
		}

		serialized, err := SafeSerialize(serdes, nextState, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		if err != nil {
			return nil, err
		}
		normalized, err := SafeDeserialize(serdes, serialized, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		if err != nil {
			return nil, err
		}

		decision := cfg.WaitStrategy(normalized, attempt)
		if !decision.ShouldContinue {
			if err := c.checkpoint.Checkpoint(ctx, stepID, OperationUpdate{
				ID:       stepID,
				ParentID: c.parentID,
				Action:   OperationActionSucceed,
				SubType:  OperationSubTypeWaitForCondition,
				Type:     OperationTypeStep,
				Payload:  serialized,
				Name:     name,
			}); err != nil {
				return nil, err
			}
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
			return normalized, nil
		}

		delay := decision.Delay.ToSeconds()
		if delay <= 0 {
			delay = 1
		}
		if err := c.checkpoint.Checkpoint(ctx, stepID, OperationUpdate{
			ID:       stepID,
			ParentID: c.parentID,
			Action:   OperationActionRetry,
			SubType:  OperationSubTypeWaitForCondition,
			Type:     OperationTypeStep,
			Payload:  serialized,
			Name:     name,
			StepOptions: &StepOptions{
				NextAttemptDelaySeconds: delay,
			},
		}); err != nil {
			return nil, err
		}

		end := time.Now().Add(time.Duration(delay) * time.Second)
		c.checkpoint.MarkOperationState(stepID, OperationLifecycleRetryWaiting, metadata, &end)
		if err := c.checkpoint.WaitForRetryTimer(ctx, stepID); err != nil {
			return nil, err
		}
	}
}
