package durable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

type StepContext struct {
	Logger Logger
}

type StepFunc func(ctx context.Context, stepCtx StepContext) (any, error)

type RetryStrategy func(err error, attempt int) RetryDecision

type StepConfig struct {
	RetryStrategy RetryStrategy
	Semantics     StepSemantics
	Serdes        Serdes
}

type InvokeConfig struct {
	PayloadSerdes Serdes
	ResultSerdes  Serdes
}

type ChildFunc func(ctx context.Context, child *DurableContext) (any, error)

type ChildConfig struct {
	Serdes           Serdes
	SubType          OperationSubType
	SummaryGenerator func(value any) string
	ErrorMapper      func(error) error
}

type DurableContext struct {
	mu sync.Mutex

	execCtx     *ExecutionContext
	checkpoint  *CheckpointManager
	mode        DurableExecutionMode
	stepPrefix  string
	stepCounter int
	parentID    string
	logger      Logger
}

func NewDurableContext(execCtx *ExecutionContext, mode DurableExecutionMode, logger Logger, stepPrefix, parentID string, checkpoint *CheckpointManager) *DurableContext {
	if logger == nil {
		logger = NopLogger{}
	}
	return &DurableContext{
		execCtx:    execCtx,
		checkpoint: checkpoint,
		mode:       mode,
		stepPrefix: stepPrefix,
		parentID:   parentID,
		logger:     logger,
	}
}

func (c *DurableContext) createStepID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepCounter++
	if c.stepPrefix == "" {
		return fmt.Sprintf("%d", c.stepCounter)
	}
	return fmt.Sprintf("%s-%d", c.stepPrefix, c.stepCounter)
}

func (c *DurableContext) nextStepID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := c.stepCounter + 1
	if c.stepPrefix == "" {
		return fmt.Sprintf("%d", next)
	}
	return fmt.Sprintf("%s-%d", c.stepPrefix, next)
}

func withModeManagement[T any](c *DurableContext, builder func() *Future[T]) *Future[T] {
	shouldSwitch := false
	nextID := c.nextStepID()
	next := c.execCtx.GetStepData(nextID)
	if c.mode == ReplayMode && next != nil && !isTerminalStatus(next.Status) {
		shouldSwitch = true
	}

	if c.mode == ReplayMode && next == nil {
		c.mode = ExecutionMode
	}

	if c.mode == ReplaySucceededContext && next != nil && !isTerminalStatus(next.Status) {
		return NewNeverFuture[T]()
	}

	f := builder()
	if shouldSwitch {
		c.mode = ExecutionMode
	}
	return f
}

func defaultRetryStrategy(_ error, attempt int) RetryDecision {
	if attempt >= 6 {
		return RetryDecision{ShouldRetry: false}
	}
	base := 5 * math.Pow(2, float64(attempt-1))
	delay := max(int(math.Min(base, 60)), 1)
	return RetryDecision{ShouldRetry: true, Delay: Duration{Seconds: delay}}
}

func operationErrorFromStepData(stepData *Operation, fallback error) error {
	if stepData != nil && stepData.StepDetails != nil && stepData.StepDetails.Error != nil {
		return DurableOperationErrorFromErrorObject(stepData.StepDetails.Error)
	}
	if fallback != nil {
		return fallback
	}
	return errors.New("operation failed")
}

func (c *DurableContext) Step(ctx context.Context, name string, fn StepFunc, cfg *StepConfig) *Future[any] {
	ValidateContextUsage(ctx, c.stepPrefix, "step", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[any] {
		stepID := c.createStepID()
		phaseDone := make(chan struct{})
		var phaseResult any
		var phaseErr error

		go func() {
			defer close(phaseDone)
			phaseResult, phaseErr = c.executeStepPhase1(stepID, name, fn, cfg)
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

func (c *DurableContext) executeStepPhase1(stepID, name string, fn StepFunc, cfg *StepConfig) (any, error) {
	if cfg == nil {
		cfg = &StepConfig{}
	}
	semantics := cfg.Semantics
	if semantics == "" {
		semantics = StepSemanticsAtLeastOncePerRetry
	}
	retryStrategy := cfg.RetryStrategy
	if retryStrategy == nil {
		retryStrategy = defaultRetryStrategy
	}
	serdes := cfg.Serdes
	if serdes == nil {
		serdes = JSONSerdes{}
	}

	metadata := OperationMetadata{StepID: stepID, Name: name, Type: OperationTypeStep, SubType: OperationSubTypeStep, ParentID: c.parentID}

	for {
		stepData := c.execCtx.GetStepData(stepID)
		if err := ValidateReplayConsistency(stepID, operationDescriptor{Type: OperationTypeStep, Name: name, SubType: OperationSubTypeStep}, stepData, c.execCtx); err != nil {
			return nil, err
		}

		if stepData != nil {
			switch stepData.Status {
			case OperationStatusSucceeded:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
				return SafeDeserialize(serdes, stepData.StepDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			case OperationStatusFailed:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
				return nil, operationErrorFromStepData(stepData, NewStepError("step failed", nil, ""))
			case OperationStatusPending:
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleRetryWaiting, metadata, stepData.StepDetails.NextAttemptTimestamp)
				if err := c.checkpoint.WaitForRetryTimer(context.Background(), stepID); err != nil {
					return nil, err
				}
				continue
			case OperationStatusStarted:
				if semantics == StepSemanticsAtMostOncePerRetry {
					interrupted := errors.New("step execution was interrupted")
					attempt := stepData.StepDetails.Attempt + 1
					decision := retryStrategy(interrupted, attempt)
					if !decision.ShouldRetry {
						errObj := (&DurableOperationError{Type: "StepError", Message: interrupted.Error(), Cause: interrupted}).ToErrorObject()
						_ = c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
							ID:       stepID,
							ParentID: c.parentID,
							Action:   OperationActionFail,
							SubType:  OperationSubTypeStep,
							Type:     OperationTypeStep,
							Error:    errObj,
							Name:     name,
						})
						c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
						return nil, NewStepError(interrupted.Error(), interrupted, "")
					}
					delay := decision.Delay.ToSeconds()
					if delay <= 0 {
						delay = 1
					}
					end := time.Now().Add(time.Duration(delay) * time.Second)
					_ = c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
						ID:          stepID,
						ParentID:    c.parentID,
						Action:      OperationActionRetry,
						SubType:     OperationSubTypeStep,
						Type:        OperationTypeStep,
						Error:       (&DurableOperationError{Type: "StepInterruptedError", Message: interrupted.Error()}).ToErrorObject(),
						Name:        name,
						StepOptions: &StepOptions{NextAttemptDelaySeconds: delay},
					})
					c.checkpoint.MarkOperationState(stepID, OperationLifecycleRetryWaiting, metadata, &end)
					if err := c.checkpoint.WaitForRetryTimer(context.Background(), stepID); err != nil {
						return nil, err
					}
					continue
				}
			}
		}

		startUpdate := OperationUpdate{
			ID:       stepID,
			ParentID: c.parentID,
			Action:   OperationActionStart,
			SubType:  OperationSubTypeStep,
			Type:     OperationTypeStep,
			Name:     name,
		}
		if stepData == nil || stepData.Status != OperationStatusStarted {
			if err := c.checkpoint.Checkpoint(context.Background(), stepID, startUpdate); err != nil {
				return nil, err
			}
		}

		c.checkpoint.MarkOperationState(stepID, OperationLifecycleExecuting, metadata, nil)

		active := WithActiveOperation(context.Background(), ActiveOperation{ContextID: stepID, ParentID: c.parentID, Mode: ExecutionMode})
		result, err := fn(active, StepContext{Logger: c.logger})
		if err == nil {
			serialized, serr := SafeSerialize(serdes, result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			if serr != nil {
				return nil, serr
			}
			if err := c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
				ID:       stepID,
				ParentID: c.parentID,
				Action:   OperationActionSucceed,
				SubType:  OperationSubTypeStep,
				Type:     OperationTypeStep,
				Payload:  serialized,
				Name:     name,
			}); err != nil {
				return nil, err
			}
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
			return SafeDeserialize(serdes, serialized, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		}

		if IsUnrecoverableError(err) {
			var u UnrecoverableError
			_ = errors.As(err, &u)
			c.execCtx.TerminationManager().Terminate(TerminationDetails{Reason: u.TerminationReason(), Message: err.Error(), Error: err})
			return nil, err
		}

		currentAttempt := 1
		if stepData != nil && stepData.StepDetails != nil {
			currentAttempt = stepData.StepDetails.Attempt + 1
		}
		decision := retryStrategy(err, currentAttempt)
		if !decision.ShouldRetry {
			errObj := (&DurableOperationError{Type: "StepError", Message: err.Error(), Cause: err}).ToErrorObject()
			_ = c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
				ID:       stepID,
				ParentID: c.parentID,
				Action:   OperationActionFail,
				SubType:  OperationSubTypeStep,
				Type:     OperationTypeStep,
				Error:    errObj,
				Name:     name,
			})
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
			return nil, NewStepError(err.Error(), err, "")
		}

		delay := decision.Delay.ToSeconds()
		if delay <= 0 {
			delay = 1
		}
		end := time.Now().Add(time.Duration(delay) * time.Second)
		_ = c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
			ID:          stepID,
			ParentID:    c.parentID,
			Action:      OperationActionRetry,
			SubType:     OperationSubTypeStep,
			Type:        OperationTypeStep,
			Error:       (&DurableOperationError{Type: "StepError", Message: err.Error(), Cause: err}).ToErrorObject(),
			Name:        name,
			StepOptions: &StepOptions{NextAttemptDelaySeconds: delay},
		})
		c.checkpoint.MarkOperationState(stepID, OperationLifecycleRetryWaiting, metadata, &end)
		if err := c.checkpoint.WaitForRetryTimer(context.Background(), stepID); err != nil {
			return nil, err
		}
	}
}

func (c *DurableContext) Wait(ctx context.Context, name string, duration Duration) *Future[struct{}] {
	ValidateContextUsage(ctx, c.stepPrefix, "wait", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[struct{}] {
		stepID := c.createStepID()
		phaseDone := make(chan struct{})
		var isCompleted bool
		var phaseErr error
		seconds := duration.ToSeconds()
		if seconds <= 0 {
			seconds = 1
		}

		go func() {
			defer close(phaseDone)
			stepData := c.execCtx.GetStepData(stepID)
			if err := ValidateReplayConsistency(stepID, operationDescriptor{Type: OperationTypeWait, Name: name, SubType: OperationSubTypeWait}, stepData, c.execCtx); err != nil {
				phaseErr = err
				return
			}

			metadata := OperationMetadata{StepID: stepID, Name: name, Type: OperationTypeWait, SubType: OperationSubTypeWait, ParentID: c.parentID}
			if stepData != nil && stepData.Status == OperationStatusSucceeded {
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
				isCompleted = true
				return
			}

			if stepData == nil {
				if err := c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
					ID:          stepID,
					ParentID:    c.parentID,
					Action:      OperationActionStart,
					SubType:     OperationSubTypeWait,
					Type:        OperationTypeWait,
					Name:        name,
					WaitOptions: &WaitOptions{WaitSeconds: seconds},
				}); err != nil {
					phaseErr = err
					return
				}
				stepData = c.execCtx.GetStepData(stepID)
			}

			var end *time.Time
			if stepData != nil && stepData.WaitDetails != nil {
				end = stepData.WaitDetails.ScheduledEndTimestamp
			}
			if end == nil {
				t := time.Now().Add(time.Duration(seconds) * time.Second)
				end = &t
			}
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleIdleNotAwaited, metadata, end)
		}()

		return NewFuture(func(awaitCtx context.Context) (struct{}, error) {
			select {
			case <-awaitCtx.Done():
				return struct{}{}, awaitCtx.Err()
			case <-phaseDone:
			}
			if phaseErr != nil {
				return struct{}{}, phaseErr
			}
			if isCompleted {
				return struct{}{}, nil
			}
			c.checkpoint.MarkOperationAwaited(stepID)
			if err := c.checkpoint.WaitForStatusChange(awaitCtx, stepID); err != nil {
				return struct{}{}, err
			}
			stepData := c.execCtx.GetStepData(stepID)
			if stepData != nil && stepData.Status == OperationStatusSucceeded {
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, OperationMetadata{}, nil)
				return struct{}{}, nil
			}
			return struct{}{}, fmt.Errorf("wait %s completed with unexpected status %v", stepID, stepData)
		})
	})
}

func (c *DurableContext) Invoke(ctx context.Context, name string, functionName string, input any, cfg *InvokeConfig) *Future[any] {
	ValidateContextUsage(ctx, c.stepPrefix, "invoke", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[any] {
		stepID := c.createStepID()
		phaseDone := make(chan struct{})
		var isCompleted bool
		var phaseErr error
		var payloadSerdes Serdes = JSONSerdes{}
		var resultSerdes Serdes = JSONSerdes{}
		if cfg != nil && cfg.PayloadSerdes != nil {
			payloadSerdes = cfg.PayloadSerdes
		}
		if cfg != nil && cfg.ResultSerdes != nil {
			resultSerdes = cfg.ResultSerdes
		}

		go func() {
			defer close(phaseDone)
			stepData := c.execCtx.GetStepData(stepID)
			if err := ValidateReplayConsistency(stepID, operationDescriptor{Type: OperationTypeChainedInvoke, Name: name, SubType: OperationSubTypeChainedInvoke}, stepData, c.execCtx); err != nil {
				phaseErr = err
				return
			}
			metadata := OperationMetadata{StepID: stepID, Name: name, Type: OperationTypeChainedInvoke, SubType: OperationSubTypeChainedInvoke, ParentID: c.parentID}
			if stepData != nil {
				switch stepData.Status {
				case OperationStatusSucceeded, OperationStatusFailed, OperationStatusTimedOut, OperationStatusStopped:
					c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, metadata, nil)
					isCompleted = true
					return
				}
			}
			if stepData == nil {
				serialized, err := SafeSerialize(payloadSerdes, input, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
				if err != nil {
					phaseErr = err
					return
				}
				if err := c.checkpoint.Checkpoint(context.Background(), stepID, OperationUpdate{
					ID:                   stepID,
					ParentID:             c.parentID,
					Action:               OperationActionStart,
					SubType:              OperationSubTypeChainedInvoke,
					Type:                 OperationTypeChainedInvoke,
					Name:                 name,
					Payload:              serialized,
					ChainedInvokeOptions: &ChainedInvokeOptions{FunctionName: functionName},
				}); err != nil {
					phaseErr = err
					return
				}
			}
			c.checkpoint.MarkOperationState(stepID, OperationLifecycleIdleNotAwaited, metadata, nil)
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
			stepData := c.execCtx.GetStepData(stepID)
			if isCompleted {
				if stepData != nil && stepData.Status == OperationStatusSucceeded {
					return SafeDeserialize(resultSerdes, stepData.ChainedInvokeDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
				}
				if stepData != nil && stepData.ChainedInvokeDetails != nil && stepData.ChainedInvokeDetails.Error != nil {
					return nil, NewInvokeError(stepData.ChainedInvokeDetails.Error.ErrorMessage, nil, stepData.ChainedInvokeDetails.Error.ErrorData)
				}
				return nil, NewInvokeError("invoke failed", nil, "")
			}

			c.checkpoint.MarkOperationAwaited(stepID)
			if err := c.checkpoint.WaitForStatusChange(awaitCtx, stepID); err != nil {
				return nil, err
			}
			stepData = c.execCtx.GetStepData(stepID)
			if stepData != nil && stepData.Status == OperationStatusSucceeded {
				c.checkpoint.MarkOperationState(stepID, OperationLifecycleCompleted, OperationMetadata{}, nil)
				return SafeDeserialize(resultSerdes, stepData.ChainedInvokeDetails.Result, stepID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
			}
			if stepData != nil && stepData.ChainedInvokeDetails != nil && stepData.ChainedInvokeDetails.Error != nil {
				return nil, NewInvokeError(stepData.ChainedInvokeDetails.Error.ErrorMessage, nil, stepData.ChainedInvokeDetails.Error.ErrorData)
			}
			return nil, NewInvokeError("invoke failed", nil, "")
		})
	})
}

func determineChildReplayMode(execCtx *ExecutionContext, stepID string) DurableExecutionMode {
	stepData := execCtx.GetStepData(stepID)
	if stepData == nil {
		return ExecutionMode
	}
	if stepData.Status == OperationStatusSucceeded && stepData.ContextDetails != nil && stepData.ContextDetails.ReplayChildren {
		return ReplaySucceededContext
	}
	if stepData.Status == OperationStatusSucceeded || stepData.Status == OperationStatusFailed {
		return ReplayMode
	}
	return ExecutionMode
}

func (c *DurableContext) newChildContext(mode DurableExecutionMode, stepPrefix, parentID string) *DurableContext {
	return NewDurableContext(c.execCtx, mode, c.logger, stepPrefix, parentID, c.checkpoint)
}

func (c *DurableContext) RunInChildContext(ctx context.Context, name string, fn ChildFunc, cfg *ChildConfig) *Future[any] {
	ValidateContextUsage(ctx, c.stepPrefix, "runInChildContext", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[any] {
		entityID := c.createStepID()
		phaseDone := make(chan struct{})
		var phaseResult any
		var phaseErr error

		go func() {
			defer close(phaseDone)
			phaseResult, phaseErr = c.executeChildPhase1(entityID, name, fn, cfg)
		}()

		return NewFuture(func(awaitCtx context.Context) (any, error) {
			select {
			case <-awaitCtx.Done():
				return nil, awaitCtx.Err()
			case <-phaseDone:
				return phaseResult, phaseErr
			}
		})
	})
}

func (c *DurableContext) executeChildPhase1(entityID, name string, fn ChildFunc, cfg *ChildConfig) (any, error) {
	if cfg == nil {
		cfg = &ChildConfig{}
	}
	serdes := cfg.Serdes
	if serdes == nil {
		serdes = JSONSerdes{}
	}
	subType := cfg.SubType
	if subType == "" {
		subType = OperationSubTypeRunInChild
	}
	stepData := c.execCtx.GetStepData(entityID)
	if err := ValidateReplayConsistency(entityID, operationDescriptor{Type: OperationTypeContext, Name: name, SubType: subType}, stepData, c.execCtx); err != nil {
		return nil, err
	}

	if stepData != nil && (stepData.Status == OperationStatusSucceeded || stepData.Status == OperationStatusFailed) {
		c.checkpoint.MarkAncestorFinished(entityID)
		if stepData.Status == OperationStatusFailed {
			if stepData.ContextDetails != nil && stepData.ContextDetails.Error != nil {
				err := DurableOperationErrorFromErrorObject(stepData.ContextDetails.Error)
				if cfg.ErrorMapper != nil {
					mapped := cfg.ErrorMapper(err)
					if mapped != nil {
						return nil, mapped
					}
					return nil, err
				}
				return nil, NewChildContextError(err.Error(), err, "")
			}
			return nil, NewChildContextError("child context failed", nil, "")
		}
		if stepData.ContextDetails != nil && stepData.ContextDetails.ReplayChildren {
			child := c.newChildContext(ReplaySucceededContext, entityID, entityID)
			active := WithActiveOperation(context.Background(), ActiveOperation{ContextID: entityID, ParentID: entityID, Mode: ReplaySucceededContext})
			return fn(active, child)
		}
		if stepData.ContextDetails != nil {
			return SafeDeserialize(serdes, stepData.ContextDetails.Result, entityID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
		}
		return nil, nil
	}

	if stepData == nil {
		if err := c.checkpoint.Checkpoint(context.Background(), entityID, OperationUpdate{
			ID:       entityID,
			ParentID: c.parentID,
			Action:   OperationActionStart,
			SubType:  subType,
			Type:     OperationTypeContext,
			Name:     name,
		}); err != nil {
			return nil, err
		}
	}

	childMode := determineChildReplayMode(c.execCtx, entityID)
	child := c.newChildContext(childMode, entityID, entityID)
	active := WithActiveOperation(context.Background(), ActiveOperation{ContextID: entityID, ParentID: c.parentID, Mode: childMode})
	result, err := fn(active, child)
	if err != nil {
		c.checkpoint.MarkAncestorFinished(entityID)
		errObj := (&DurableOperationError{Type: "ChildContextError", Message: err.Error(), Cause: err}).ToErrorObject()
		_ = c.checkpoint.Checkpoint(context.Background(), entityID, OperationUpdate{
			ID:       entityID,
			ParentID: c.parentID,
			Action:   OperationActionFail,
			SubType:  subType,
			Type:     OperationTypeContext,
			Name:     name,
			Error:    errObj,
		})
		if cfg.ErrorMapper != nil {
			mapped := cfg.ErrorMapper(err)
			if mapped == nil {
				mapped = err
			}
			return nil, mapped
		}
		return nil, NewChildContextError(err.Error(), err, "")
	}

	serialized, err := SafeSerialize(serdes, result, entityID, name, c.execCtx.TerminationManager(), c.execCtx.DurableExecutionArn())
	if err != nil {
		return nil, err
	}

	replayChildren := false
	payload := serialized
	if len(payload) > 256*1024 {
		replayChildren = true
		if cfg.SummaryGenerator != nil {
			payload = cfg.SummaryGenerator(result)
		} else {
			payload = ""
		}
	}

	c.checkpoint.MarkAncestorFinished(entityID)
	if err := c.checkpoint.Checkpoint(context.Background(), entityID, OperationUpdate{
		ID:             entityID,
		ParentID:       c.parentID,
		Action:         OperationActionSucceed,
		SubType:        subType,
		Type:           OperationTypeContext,
		Name:           name,
		Payload:        payload,
		ContextOptions: &ContextOptions{ReplayChildren: replayChildren},
	}); err != nil {
		return nil, err
	}
	return result, nil
}
