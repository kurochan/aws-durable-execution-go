package durable

import (
	"context"
	"time"
)

// InvocationStatus is the top-level status returned from a wrapped durable
// handler invocation.
type InvocationStatus string

const (
	// InvocationStatusSucceeded means the handler completed and Result contains
	// the serialized handler return value when present.
	InvocationStatusSucceeded InvocationStatus = "SUCCEEDED"
	// InvocationStatusFailed means the handler completed with a business error.
	InvocationStatusFailed InvocationStatus = "FAILED"
	// InvocationStatusPending means execution paused on a durable wait, callback,
	// retry, invoke, or child operation and should be resumed later.
	InvocationStatusPending InvocationStatus = "PENDING"
)

// DurableExecutionMode describes how a DurableContext should interpret
// checkpoint state for the current invocation.
type DurableExecutionMode string

const (
	// ExecutionMode schedules new durable operations.
	ExecutionMode DurableExecutionMode = "ExecutionMode"
	// ReplayMode reads existing operation results before switching to execution.
	ReplayMode DurableExecutionMode = "ReplayMode"
	// ReplaySucceededContext replays children of a context whose full result was
	// replaced by a compact summary.
	ReplaySucceededContext DurableExecutionMode = "ReplaySucceededContext"
)

// OperationType is the durable backend operation category.
type OperationType string

const (
	// OperationTypeExecution represents the top-level handler execution.
	OperationTypeExecution OperationType = "EXECUTION"
	// OperationTypeStep represents a durable step.
	OperationTypeStep OperationType = "STEP"
	// OperationTypeWait represents a durable timer.
	OperationTypeWait OperationType = "WAIT"
	// OperationTypeCallback represents an external callback wait.
	OperationTypeCallback OperationType = "CALLBACK"
	// OperationTypeContext represents a child durable context.
	OperationTypeContext OperationType = "CONTEXT"
	// OperationTypeChainedInvoke represents a durable chained Lambda invoke.
	OperationTypeChainedInvoke OperationType = "CHAINED_INVOKE"
)

// OperationAction is the checkpoint update action sent to the durable backend.
type OperationAction string

const (
	// OperationActionStart starts an operation.
	OperationActionStart OperationAction = "START"
	// OperationActionSucceed marks an operation as succeeded.
	OperationActionSucceed OperationAction = "SUCCEED"
	// OperationActionFail marks an operation as failed.
	OperationActionFail OperationAction = "FAIL"
	// OperationActionRetry schedules a retry for an operation.
	OperationActionRetry OperationAction = "RETRY"
)

// OperationStatus is the status read from durable execution state.
type OperationStatus string

const (
	// OperationStatusStarted means the operation has started.
	OperationStatusStarted OperationStatus = "STARTED"
	// OperationStatusReady means the operation is ready to run.
	OperationStatusReady OperationStatus = "READY"
	// OperationStatusPending means the operation is waiting for a timer,
	// callback, retry, or external completion.
	OperationStatusPending OperationStatus = "PENDING"
	// OperationStatusSucceeded means the operation completed successfully.
	OperationStatusSucceeded OperationStatus = "SUCCEEDED"
	// OperationStatusFailed means the operation completed with an error.
	OperationStatusFailed OperationStatus = "FAILED"
	// OperationStatusCancelled means the operation was canceled.
	OperationStatusCancelled OperationStatus = "CANCELLED"
	// OperationStatusStopped means the operation was stopped.
	OperationStatusStopped OperationStatus = "STOPPED"
	// OperationStatusTimedOut means the operation timed out.
	OperationStatusTimedOut OperationStatus = "TIMED_OUT"
)

// OperationSubType identifies a higher-level SDK primitive within an
// OperationType.
type OperationSubType string

const (
	// OperationSubTypeStep is used by DurableContext.Step.
	OperationSubTypeStep OperationSubType = "Step"
	// OperationSubTypeWait is used by DurableContext.Wait.
	OperationSubTypeWait OperationSubType = "Wait"
	// OperationSubTypeCallback is used by DurableContext.CreateCallback.
	OperationSubTypeCallback OperationSubType = "Callback"
	// OperationSubTypeRunInChild is used by DurableContext.RunInChildContext.
	OperationSubTypeRunInChild OperationSubType = "RunInChildContext"
	// OperationSubTypeMap is the top-level Map context subtype.
	OperationSubTypeMap OperationSubType = "Map"
	// OperationSubTypeMapIteration is the per-item Map child subtype.
	OperationSubTypeMapIteration OperationSubType = "MapIteration"
	// OperationSubTypeParallel is the top-level Parallel context subtype.
	OperationSubTypeParallel OperationSubType = "Parallel"
	// OperationSubTypeParallelBranch is the per-branch Parallel child subtype.
	OperationSubTypeParallelBranch OperationSubType = "ParallelBranch"
	// OperationSubTypeWaitForCallback is used by DurableContext.WaitForCallback.
	OperationSubTypeWaitForCallback OperationSubType = "WaitForCallback"
	// OperationSubTypeWaitForCondition is used by DurableContext.WaitForCondition.
	OperationSubTypeWaitForCondition OperationSubType = "WaitForCondition"
	// OperationSubTypeChainedInvoke is used by DurableContext.Invoke.
	OperationSubTypeChainedInvoke OperationSubType = "ChainedInvoke"
)

// OperationLifecycleState is an in-process state tracked while an invocation is
// running.
type OperationLifecycleState string

const (
	// OperationLifecycleExecuting means the operation is running locally.
	OperationLifecycleExecuting OperationLifecycleState = "EXECUTING"
	// OperationLifecycleRetryWaiting means the operation is waiting for a retry
	// timer.
	OperationLifecycleRetryWaiting OperationLifecycleState = "RETRY_WAITING"
	// OperationLifecycleIdleNotAwaited means the operation has been scheduled but
	// its future has not been awaited.
	OperationLifecycleIdleNotAwaited OperationLifecycleState = "IDLE_NOT_AWAITED"
	// OperationLifecycleIdleAwaited means the operation has been awaited and is
	// waiting for external completion.
	OperationLifecycleIdleAwaited OperationLifecycleState = "IDLE_AWAITED"
	// OperationLifecycleCompleted means the operation is terminal locally.
	OperationLifecycleCompleted OperationLifecycleState = "COMPLETED"
)

// TerminationReason explains why the current invocation stopped before normal
// handler completion.
type TerminationReason string

const (
	// TerminationReasonOperationTerminated means an operation requested
	// termination of the current invocation.
	TerminationReasonOperationTerminated TerminationReason = "OPERATION_TERMINATED"
	// TerminationReasonRetryScheduled means a retry timer was scheduled.
	TerminationReasonRetryScheduled TerminationReason = "RETRY_SCHEDULED"
	// TerminationReasonRetryInterruptedStep means an interrupted at-most-once
	// step scheduled a retry.
	TerminationReasonRetryInterruptedStep TerminationReason = "RETRY_INTERRUPTED_STEP"
	// TerminationReasonWaitScheduled means a wait timer was scheduled.
	TerminationReasonWaitScheduled TerminationReason = "WAIT_SCHEDULED"
	// TerminationReasonCallbackPending means a callback is waiting externally.
	TerminationReasonCallbackPending TerminationReason = "CALLBACK_PENDING"
	// TerminationReasonCheckpointFailed means checkpoint persistence failed.
	TerminationReasonCheckpointFailed TerminationReason = "CHECKPOINT_FAILED"
	// TerminationReasonSerdesFailed means serialization or deserialization failed.
	TerminationReasonSerdesFailed TerminationReason = "SERDES_FAILED"
	// TerminationReasonContextValidation means a durable operation used the wrong
	// context.
	TerminationReasonContextValidation TerminationReason = "CONTEXT_VALIDATION_ERROR"
	// TerminationReasonCustom is available for user-defined termination.
	TerminationReasonCustom TerminationReason = "CUSTOM"
)

// ErrorObject is the serialized durable error shape stored in checkpoints and
// invocation outputs.
type ErrorObject struct {
	ErrorType    string   `json:"ErrorType,omitempty"`
	ErrorMessage string   `json:"ErrorMessage,omitempty"`
	ErrorData    string   `json:"ErrorData,omitempty"`
	StackTrace   []string `json:"StackTrace,omitempty"`
}

// InvocationOutput is returned by a wrapped durable handler.
type InvocationOutput struct {
	Status InvocationStatus `json:"Status"`
	Result string           `json:"Result,omitempty"`
	Error  *ErrorObject     `json:"Error,omitempty"`
}

// ExecutionState is the checkpoint state included in an invocation input or
// returned by a durable backend.
type ExecutionState struct {
	Operations []Operation `json:"Operations"`
	NextMarker string      `json:"NextMarker,omitempty"`
}

// InvocationInput is the payload consumed by a WrappedHandler.
type InvocationInput struct {
	DurableExecutionArn   string         `json:"DurableExecutionArn"`
	CheckpointToken       string         `json:"CheckpointToken"`
	InitialExecutionState ExecutionState `json:"InitialExecutionState"`
}

// ExecutionDetails contains metadata for the top-level execution operation.
type ExecutionDetails struct {
	InputPayload string `json:"InputPayload,omitempty"`
}

// StepDetails contains persisted state for a step-like operation.
type StepDetails struct {
	Result               string       `json:"Result,omitempty"`
	Error                *ErrorObject `json:"Error,omitempty"`
	Attempt              int          `json:"Attempt,omitempty"`
	NextAttemptTimestamp *time.Time   `json:"NextAttemptTimestamp,omitempty"`
}

// WaitDetails contains persisted state for a wait operation.
type WaitDetails struct {
	ScheduledEndTimestamp *time.Time `json:"ScheduledEndTimestamp,omitempty"`
}

// CallbackDetails contains persisted state for a callback operation.
type CallbackDetails struct {
	CallbackID                string       `json:"CallbackId,omitempty"`
	Result                    string       `json:"Result,omitempty"`
	Error                     *ErrorObject `json:"Error,omitempty"`
	ScheduledTimeoutTimestamp *time.Time   `json:"ScheduledTimeoutTimestamp,omitempty"`
}

// ChainedInvokeDetails contains persisted state for a chained invocation.
type ChainedInvokeDetails struct {
	Result string       `json:"Result,omitempty"`
	Error  *ErrorObject `json:"Error,omitempty"`
}

// ContextDetails contains persisted state for a child context operation.
type ContextDetails struct {
	Result         string       `json:"Result,omitempty"`
	ReplayChildren bool         `json:"ReplayChildren,omitempty"`
	Error          *ErrorObject `json:"Error,omitempty"`
}

// Operation is a durable backend operation record.
type Operation struct {
	ID       string           `json:"Id,omitempty"`
	ParentID string           `json:"ParentId,omitempty"`
	Name     string           `json:"Name,omitempty"`
	Type     OperationType    `json:"Type,omitempty"`
	SubType  OperationSubType `json:"SubType,omitempty"`
	Status   OperationStatus  `json:"Status,omitempty"`

	ExecutionDetails     *ExecutionDetails     `json:"ExecutionDetails,omitempty"`
	StepDetails          *StepDetails          `json:"StepDetails,omitempty"`
	WaitDetails          *WaitDetails          `json:"WaitDetails,omitempty"`
	CallbackDetails      *CallbackDetails      `json:"CallbackDetails,omitempty"`
	ChainedInvokeDetails *ChainedInvokeDetails `json:"ChainedInvokeDetails,omitempty"`
	ContextDetails       *ContextDetails       `json:"ContextDetails,omitempty"`

	StartTimestamp *time.Time `json:"StartTimestamp,omitempty"`
}

// StepOptions are checkpoint options for step retry scheduling.
type StepOptions struct {
	NextAttemptDelaySeconds int `json:"NextAttemptDelaySeconds,omitempty"`
}

// WaitOptions are checkpoint options for durable waits.
type WaitOptions struct {
	WaitSeconds int `json:"WaitSeconds,omitempty"`
}

// CallbackOptions are checkpoint options for callbacks.
type CallbackOptions struct {
	TimeoutSeconds          int `json:"TimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds int `json:"HeartbeatTimeoutSeconds,omitempty"`
}

// ChainedInvokeOptions are checkpoint options for chained invocation.
type ChainedInvokeOptions struct {
	FunctionName string `json:"FunctionName,omitempty"`
}

// ContextOptions are checkpoint options for child contexts.
type ContextOptions struct {
	ReplayChildren bool `json:"ReplayChildren,omitempty"`
}

// OperationUpdate is sent to DurableExecutionClient.Checkpoint to change
// operation state.
type OperationUpdate struct {
	ID       string           `json:"Id,omitempty"`
	ParentID string           `json:"ParentId,omitempty"`
	Name     string           `json:"Name,omitempty"`
	Type     OperationType    `json:"Type,omitempty"`
	SubType  OperationSubType `json:"SubType,omitempty"`
	Action   OperationAction  `json:"Action,omitempty"`

	Payload string       `json:"Payload,omitempty"`
	Error   *ErrorObject `json:"Error,omitempty"`

	StepOptions          *StepOptions          `json:"StepOptions,omitempty"`
	WaitOptions          *WaitOptions          `json:"WaitOptions,omitempty"`
	CallbackOptions      *CallbackOptions      `json:"CallbackOptions,omitempty"`
	ChainedInvokeOptions *ChainedInvokeOptions `json:"ChainedInvokeOptions,omitempty"`
	ContextOptions       *ContextOptions       `json:"ContextOptions,omitempty"`
}

// GetExecutionStateRequest requests checkpoint state from a durable backend.
type GetExecutionStateRequest struct {
	DurableExecutionArn string
	CheckpointToken     string
	Marker              string
	MaxItems            int
}

// GetExecutionStateResponse contains a page of durable backend operations.
type GetExecutionStateResponse struct {
	Operations []Operation
	NextMarker string
}

// CheckpointRequest persists operation updates to a durable backend.
type CheckpointRequest struct {
	DurableExecutionArn string
	CheckpointToken     string
	Updates             []OperationUpdate
}

// CheckpointResponse is returned after checkpoint persistence.
type CheckpointResponse struct {
	CheckpointToken   string
	NewExecutionState *ExecutionState
}

// DurableExecutionClient is the backend contract used by the SDK.
//
// Implement this interface to plug in a production backend or use
// NewInMemoryClient for tests and examples.
type DurableExecutionClient interface {
	GetExecutionState(ctx context.Context, input GetExecutionStateRequest) (GetExecutionStateResponse, error)
	Checkpoint(ctx context.Context, input CheckpointRequest) (CheckpointResponse, error)
}

// Duration is a serializable duration used by durable timers and retry delays.
type Duration struct {
	Days    int `json:"days,omitempty"`
	Hours   int `json:"hours,omitempty"`
	Minutes int `json:"minutes,omitempty"`
	Seconds int `json:"seconds,omitempty"`
}

// ToSeconds returns the total duration in seconds.
func (d Duration) ToSeconds() int {
	return d.Days*24*60*60 + d.Hours*60*60 + d.Minutes*60 + d.Seconds
}

// ToDuration converts d to a time.Duration.
func (d Duration) ToDuration() time.Duration {
	return time.Duration(d.ToSeconds()) * time.Second
}

// RetryDecision is returned by a RetryStrategy.
type RetryDecision struct {
	ShouldRetry bool
	Delay       Duration
}

// StepSemantics controls how Step handles an operation that was started but not
// completed before the invocation stopped.
type StepSemantics string

const (
	// StepSemanticsAtLeastOncePerRetry allows a started step to run again on the
	// same retry attempt after replay.
	StepSemanticsAtLeastOncePerRetry StepSemantics = "AT_LEAST_ONCE_PER_RETRY"
	// StepSemanticsAtMostOncePerRetry treats an interrupted started step as a
	// failure and schedules the next retry attempt.
	StepSemanticsAtMostOncePerRetry StepSemantics = "AT_MOST_ONCE_PER_RETRY"
)

// OperationMetadata identifies an operation being tracked locally.
type OperationMetadata struct {
	StepID   string
	Name     string
	Type     OperationType
	SubType  OperationSubType
	ParentID string
}

// Logger is the minimal logging interface used by the SDK.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// NopLogger discards all log messages.
type NopLogger struct{}

// Debugf discards a debug log message.
func (NopLogger) Debugf(string, ...any) {}

// Infof discards an info log message.
func (NopLogger) Infof(string, ...any) {}

// Warnf discards a warning log message.
func (NopLogger) Warnf(string, ...any) {}

// Errorf discards an error log message.
func (NopLogger) Errorf(string, ...any) {}

// CreateCallbackConfig configures DurableContext.CreateCallback.
type CreateCallbackConfig struct {
	Timeout          *Duration
	HeartbeatTimeout *Duration
	Serdes           Serdes
}

// CreateCallbackResult is returned by DurableContext.CreateCallback.
type CreateCallbackResult struct {
	Promise    *Future[any]
	CallbackID string
}

// WaitForCallbackContext is passed to a WaitForCallback submitter.
type WaitForCallbackContext struct {
	Logger Logger
}

// WaitForCallbackSubmitterFunc sends a callback ID to an external system.
type WaitForCallbackSubmitterFunc func(callbackID string, ctx WaitForCallbackContext) error

// WaitForCallbackConfig configures DurableContext.WaitForCallback.
type WaitForCallbackConfig struct {
	Timeout          *Duration
	HeartbeatTimeout *Duration
	RetryStrategy    RetryStrategy
	Serdes           Serdes
}

// WaitForConditionContext is passed to a WaitForCondition check function.
type WaitForConditionContext struct {
	Logger Logger
}

// WaitForConditionCheckFunc evaluates and returns the next condition state.
type WaitForConditionCheckFunc func(state any, ctx WaitForConditionContext) (any, error)

// WaitForConditionWaitStrategyFunc decides whether WaitForCondition should wait
// and retry or complete with the current state.
type WaitForConditionWaitStrategyFunc func(state any, attempt int) WaitForConditionDecision

// WaitForConditionDecision is returned by a WaitForCondition wait strategy.
type WaitForConditionDecision struct {
	ShouldContinue bool
	Delay          Duration
}

// WaitForConditionConfig configures DurableContext.WaitForCondition.
type WaitForConditionConfig struct {
	WaitStrategy WaitForConditionWaitStrategyFunc
	InitialState any
	Serdes       Serdes
}

// BatchItemStatus is the status of one Map, Parallel, or concurrent item.
type BatchItemStatus string

const (
	// BatchItemStatusSucceeded means the item completed successfully.
	BatchItemStatusSucceeded BatchItemStatus = "SUCCEEDED"
	// BatchItemStatusFailed means the item completed with an error.
	BatchItemStatusFailed BatchItemStatus = "FAILED"
	// BatchItemStatusStarted means the item was started but not completed before
	// scheduling stopped.
	BatchItemStatusStarted BatchItemStatus = "STARTED"
)

// BatchCompletionReason explains why a batch operation stopped scheduling or
// completed.
type BatchCompletionReason string

const (
	// BatchCompletionReasonAllCompleted means all scheduled items completed.
	BatchCompletionReasonAllCompleted BatchCompletionReason = "ALL_COMPLETED"
	// BatchCompletionReasonMinSuccessfulReached means MinSuccessful was reached.
	BatchCompletionReasonMinSuccessfulReached BatchCompletionReason = "MIN_SUCCESSFUL_REACHED"
	// BatchCompletionReasonFailureToleranceExceeded means failure tolerance was
	// exceeded.
	BatchCompletionReasonFailureToleranceExceeded BatchCompletionReason = "FAILURE_TOLERANCE_EXCEEDED"
)

// BatchItem is one item result in a BatchResult.
type BatchItem struct {
	Result any             `json:"result,omitempty"`
	Error  *ErrorObject    `json:"error,omitempty"`
	Index  int             `json:"index"`
	Status BatchItemStatus `json:"status"`
}

// BatchResult is returned by Map, Parallel, and ExecuteConcurrently.
type BatchResult struct {
	All              []BatchItem           `json:"all"`
	CompletionReason BatchCompletionReason `json:"completionReason"`
}

// Succeeded returns all successful items.
func (r *BatchResult) Succeeded() []BatchItem {
	if r == nil {
		return nil
	}
	out := make([]BatchItem, 0, len(r.All))
	for _, item := range r.All {
		if item.Status == BatchItemStatusSucceeded {
			out = append(out, item)
		}
	}
	return out
}

// Failed returns all failed items.
func (r *BatchResult) Failed() []BatchItem {
	if r == nil {
		return nil
	}
	out := make([]BatchItem, 0, len(r.All))
	for _, item := range r.All {
		if item.Status == BatchItemStatusFailed {
			out = append(out, item)
		}
	}
	return out
}

// Started returns all items that were started but did not complete.
func (r *BatchResult) Started() []BatchItem {
	if r == nil {
		return nil
	}
	out := make([]BatchItem, 0, len(r.All))
	for _, item := range r.All {
		if item.Status == BatchItemStatusStarted {
			out = append(out, item)
		}
	}
	return out
}

// Status returns BatchItemStatusFailed if any item failed, otherwise
// BatchItemStatusSucceeded.
func (r *BatchResult) Status() BatchItemStatus {
	if r == nil {
		return BatchItemStatusSucceeded
	}
	for _, item := range r.All {
		if item.Status == BatchItemStatusFailed {
			return BatchItemStatusFailed
		}
	}
	return BatchItemStatusSucceeded
}

// HasFailure reports whether any item failed.
func (r *BatchResult) HasFailure() bool {
	return r.Status() == BatchItemStatusFailed
}

// ThrowIfError returns the first failed item error, if any.
func (r *BatchResult) ThrowIfError() error {
	if r == nil {
		return nil
	}
	for _, item := range r.All {
		if item.Status == BatchItemStatusFailed && item.Error != nil {
			return DurableOperationErrorFromErrorObject(item.Error)
		}
	}
	return nil
}

// Results returns successful item results in batch order.
func (r *BatchResult) Results() []any {
	if r == nil {
		return nil
	}
	out := make([]any, 0, len(r.All))
	for _, item := range r.All {
		if item.Status == BatchItemStatusSucceeded {
			out = append(out, item.Result)
		}
	}
	return out
}

// Errors returns errors for failed items.
func (r *BatchResult) Errors() []error {
	if r == nil {
		return nil
	}
	out := make([]error, 0, len(r.All))
	for _, item := range r.All {
		if item.Status == BatchItemStatusFailed && item.Error != nil {
			out = append(out, DurableOperationErrorFromErrorObject(item.Error))
		}
	}
	return out
}

// SuccessCount returns the number of successful items.
func (r *BatchResult) SuccessCount() int {
	if r == nil {
		return 0
	}
	n := 0
	for _, item := range r.All {
		if item.Status == BatchItemStatusSucceeded {
			n++
		}
	}
	return n
}

// FailureCount returns the number of failed items.
func (r *BatchResult) FailureCount() int {
	if r == nil {
		return 0
	}
	n := 0
	for _, item := range r.All {
		if item.Status == BatchItemStatusFailed {
			n++
		}
	}
	return n
}

// StartedCount returns the number of started but incomplete items.
func (r *BatchResult) StartedCount() int {
	if r == nil {
		return 0
	}
	n := 0
	for _, item := range r.All {
		if item.Status == BatchItemStatusStarted {
			n++
		}
	}
	return n
}

// TotalCount returns the number of items represented in the result.
func (r *BatchResult) TotalCount() int {
	if r == nil {
		return 0
	}
	return len(r.All)
}

// CompletionConfig controls when a batch-style operation can stop early.
type CompletionConfig struct {
	MinSuccessful              *int
	ToleratedFailureCount      *int
	ToleratedFailurePercentage *float64
}

// MapFunc processes one item in DurableContext.Map.
type MapFunc func(context *DurableContext, item any, index int, array []any) (any, error)

// MapConfig configures DurableContext.Map.
type MapConfig struct {
	MaxConcurrency   int
	ItemNamer        func(item any, index int) string
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}

// ParallelFunc executes one branch in DurableContext.Parallel.
type ParallelFunc func(context *DurableContext) (any, error)

// NamedParallelBranch names one Parallel branch and provides its function.
type NamedParallelBranch struct {
	Name string
	Func ParallelFunc
}

// ParallelConfig configures DurableContext.Parallel.
type ParallelConfig struct {
	MaxConcurrency   int
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}

// ConcurrentExecutionItem describes one item for ExecuteConcurrently.
type ConcurrentExecutionItem struct {
	ID    string `json:"id"`
	Data  any    `json:"data,omitempty"`
	Index int    `json:"index"`
	Name  string `json:"name,omitempty"`
}

// ConcurrentExecutor processes one ExecuteConcurrently item.
type ConcurrentExecutor func(item ConcurrentExecutionItem, childContext *DurableContext) (any, error)

// ConcurrencyConfig configures DurableContext.ExecuteConcurrently.
type ConcurrencyConfig struct {
	MaxConcurrency   int
	TopLevelSubType  OperationSubType
	IterationSubType OperationSubType
	SummaryGenerator func(result *BatchResult) string
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}
