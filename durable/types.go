package durable

import "time"

type InvocationStatus string

const (
	InvocationStatusSucceeded InvocationStatus = "SUCCEEDED"
	InvocationStatusFailed    InvocationStatus = "FAILED"
	InvocationStatusPending   InvocationStatus = "PENDING"
)

type DurableExecutionMode string

const (
	ExecutionMode          DurableExecutionMode = "ExecutionMode"
	ReplayMode             DurableExecutionMode = "ReplayMode"
	ReplaySucceededContext DurableExecutionMode = "ReplaySucceededContext"
)

type OperationType string

const (
	OperationTypeExecution     OperationType = "EXECUTION"
	OperationTypeStep          OperationType = "STEP"
	OperationTypeWait          OperationType = "WAIT"
	OperationTypeCallback      OperationType = "CALLBACK"
	OperationTypeContext       OperationType = "CONTEXT"
	OperationTypeChainedInvoke OperationType = "CHAINED_INVOKE"
)

type OperationAction string

const (
	OperationActionStart   OperationAction = "START"
	OperationActionSucceed OperationAction = "SUCCEED"
	OperationActionFail    OperationAction = "FAIL"
	OperationActionRetry   OperationAction = "RETRY"
)

type OperationStatus string

const (
	OperationStatusStarted   OperationStatus = "STARTED"
	OperationStatusReady     OperationStatus = "READY"
	OperationStatusPending   OperationStatus = "PENDING"
	OperationStatusSucceeded OperationStatus = "SUCCEEDED"
	OperationStatusFailed    OperationStatus = "FAILED"
	OperationStatusCancelled OperationStatus = "CANCELLED"
	OperationStatusStopped   OperationStatus = "STOPPED"
	OperationStatusTimedOut  OperationStatus = "TIMED_OUT"
)

type OperationSubType string

const (
	OperationSubTypeStep             OperationSubType = "Step"
	OperationSubTypeWait             OperationSubType = "Wait"
	OperationSubTypeCallback         OperationSubType = "Callback"
	OperationSubTypeRunInChild       OperationSubType = "RunInChildContext"
	OperationSubTypeMap              OperationSubType = "Map"
	OperationSubTypeMapIteration     OperationSubType = "MapIteration"
	OperationSubTypeParallel         OperationSubType = "Parallel"
	OperationSubTypeParallelBranch   OperationSubType = "ParallelBranch"
	OperationSubTypeWaitForCallback  OperationSubType = "WaitForCallback"
	OperationSubTypeWaitForCondition OperationSubType = "WaitForCondition"
	OperationSubTypeChainedInvoke    OperationSubType = "ChainedInvoke"
)

type OperationLifecycleState string

const (
	OperationLifecycleExecuting      OperationLifecycleState = "EXECUTING"
	OperationLifecycleRetryWaiting   OperationLifecycleState = "RETRY_WAITING"
	OperationLifecycleIdleNotAwaited OperationLifecycleState = "IDLE_NOT_AWAITED"
	OperationLifecycleIdleAwaited    OperationLifecycleState = "IDLE_AWAITED"
	OperationLifecycleCompleted      OperationLifecycleState = "COMPLETED"
)

type TerminationReason string

const (
	TerminationReasonOperationTerminated  TerminationReason = "OPERATION_TERMINATED"
	TerminationReasonRetryScheduled       TerminationReason = "RETRY_SCHEDULED"
	TerminationReasonRetryInterruptedStep TerminationReason = "RETRY_INTERRUPTED_STEP"
	TerminationReasonWaitScheduled        TerminationReason = "WAIT_SCHEDULED"
	TerminationReasonCallbackPending      TerminationReason = "CALLBACK_PENDING"
	TerminationReasonCheckpointFailed     TerminationReason = "CHECKPOINT_FAILED"
	TerminationReasonSerdesFailed         TerminationReason = "SERDES_FAILED"
	TerminationReasonContextValidation    TerminationReason = "CONTEXT_VALIDATION_ERROR"
	TerminationReasonCustom               TerminationReason = "CUSTOM"
)

type ErrorObject struct {
	ErrorType    string   `json:"ErrorType,omitempty"`
	ErrorMessage string   `json:"ErrorMessage,omitempty"`
	ErrorData    string   `json:"ErrorData,omitempty"`
	StackTrace   []string `json:"StackTrace,omitempty"`
}

type InvocationOutput struct {
	Status InvocationStatus `json:"Status"`
	Result string           `json:"Result,omitempty"`
	Error  *ErrorObject     `json:"Error,omitempty"`
}

type ExecutionState struct {
	Operations []Operation `json:"Operations"`
	NextMarker string      `json:"NextMarker,omitempty"`
}

type InvocationInput struct {
	DurableExecutionArn   string         `json:"DurableExecutionArn"`
	CheckpointToken       string         `json:"CheckpointToken"`
	InitialExecutionState ExecutionState `json:"InitialExecutionState"`
}

type ExecutionDetails struct {
	InputPayload string `json:"InputPayload,omitempty"`
}

type StepDetails struct {
	Result               string       `json:"Result,omitempty"`
	Error                *ErrorObject `json:"Error,omitempty"`
	Attempt              int          `json:"Attempt,omitempty"`
	NextAttemptTimestamp *time.Time   `json:"NextAttemptTimestamp,omitempty"`
}

type WaitDetails struct {
	ScheduledEndTimestamp *time.Time `json:"ScheduledEndTimestamp,omitempty"`
}

type CallbackDetails struct {
	CallbackID                string       `json:"CallbackId,omitempty"`
	Result                    string       `json:"Result,omitempty"`
	Error                     *ErrorObject `json:"Error,omitempty"`
	ScheduledTimeoutTimestamp *time.Time   `json:"ScheduledTimeoutTimestamp,omitempty"`
}

type ChainedInvokeDetails struct {
	Result string       `json:"Result,omitempty"`
	Error  *ErrorObject `json:"Error,omitempty"`
}

type ContextDetails struct {
	Result         string       `json:"Result,omitempty"`
	ReplayChildren bool         `json:"ReplayChildren,omitempty"`
	Error          *ErrorObject `json:"Error,omitempty"`
}

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

type StepOptions struct {
	NextAttemptDelaySeconds int `json:"NextAttemptDelaySeconds,omitempty"`
}

type WaitOptions struct {
	WaitSeconds int `json:"WaitSeconds,omitempty"`
}

type CallbackOptions struct {
	TimeoutSeconds          int `json:"TimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds int `json:"HeartbeatTimeoutSeconds,omitempty"`
}

type ChainedInvokeOptions struct {
	FunctionName string `json:"FunctionName,omitempty"`
}

type ContextOptions struct {
	ReplayChildren bool `json:"ReplayChildren,omitempty"`
}

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

type GetExecutionStateRequest struct {
	DurableExecutionArn string
	CheckpointToken     string
	Marker              string
	MaxItems            int
}

type GetExecutionStateResponse struct {
	Operations []Operation
	NextMarker string
}

type CheckpointRequest struct {
	DurableExecutionArn string
	CheckpointToken     string
	Updates             []OperationUpdate
}

type CheckpointResponse struct {
	CheckpointToken   string
	NewExecutionState *ExecutionState
}

type DurableExecutionClient interface {
	GetExecutionState(input GetExecutionStateRequest) (GetExecutionStateResponse, error)
	Checkpoint(input CheckpointRequest) (CheckpointResponse, error)
}

type Duration struct {
	Days    int `json:"days,omitempty"`
	Hours   int `json:"hours,omitempty"`
	Minutes int `json:"minutes,omitempty"`
	Seconds int `json:"seconds,omitempty"`
}

func (d Duration) ToSeconds() int {
	return d.Days*24*60*60 + d.Hours*60*60 + d.Minutes*60 + d.Seconds
}

func (d Duration) ToDuration() time.Duration {
	return time.Duration(d.ToSeconds()) * time.Second
}

type RetryDecision struct {
	ShouldRetry bool
	Delay       Duration
}

type StepSemantics string

const (
	StepSemanticsAtLeastOncePerRetry StepSemantics = "AT_LEAST_ONCE_PER_RETRY"
	StepSemanticsAtMostOncePerRetry  StepSemantics = "AT_MOST_ONCE_PER_RETRY"
)

type OperationMetadata struct {
	StepID   string
	Name     string
	Type     OperationType
	SubType  OperationSubType
	ParentID string
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type NopLogger struct{}

func (NopLogger) Debugf(string, ...any) {}
func (NopLogger) Infof(string, ...any)  {}
func (NopLogger) Warnf(string, ...any)  {}
func (NopLogger) Errorf(string, ...any) {}

type CreateCallbackConfig struct {
	Timeout          *Duration
	HeartbeatTimeout *Duration
	Serdes           Serdes
}

type CreateCallbackResult struct {
	Promise    *Future[any]
	CallbackID string
}

type WaitForCallbackContext struct {
	Logger Logger
}

type WaitForCallbackSubmitterFunc func(callbackID string, ctx WaitForCallbackContext) error

type WaitForCallbackConfig struct {
	Timeout          *Duration
	HeartbeatTimeout *Duration
	RetryStrategy    RetryStrategy
	Serdes           Serdes
}

type WaitForConditionContext struct {
	Logger Logger
}

type WaitForConditionCheckFunc func(state any, ctx WaitForConditionContext) (any, error)

type WaitForConditionWaitStrategyFunc func(state any, attempt int) WaitForConditionDecision

type WaitForConditionDecision struct {
	ShouldContinue bool
	Delay          Duration
}

type WaitForConditionConfig struct {
	WaitStrategy WaitForConditionWaitStrategyFunc
	InitialState any
	Serdes       Serdes
}

type BatchItemStatus string

const (
	BatchItemStatusSucceeded BatchItemStatus = "SUCCEEDED"
	BatchItemStatusFailed    BatchItemStatus = "FAILED"
	BatchItemStatusStarted   BatchItemStatus = "STARTED"
)

type BatchCompletionReason string

const (
	BatchCompletionReasonAllCompleted             BatchCompletionReason = "ALL_COMPLETED"
	BatchCompletionReasonMinSuccessfulReached     BatchCompletionReason = "MIN_SUCCESSFUL_REACHED"
	BatchCompletionReasonFailureToleranceExceeded BatchCompletionReason = "FAILURE_TOLERANCE_EXCEEDED"
)

type BatchItem struct {
	Result any             `json:"result,omitempty"`
	Error  *ErrorObject    `json:"error,omitempty"`
	Index  int             `json:"index"`
	Status BatchItemStatus `json:"status"`
}

type BatchResult struct {
	All              []BatchItem           `json:"all"`
	CompletionReason BatchCompletionReason `json:"completionReason"`
}

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

func (r *BatchResult) HasFailure() bool {
	return r.Status() == BatchItemStatusFailed
}

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

func (r *BatchResult) TotalCount() int {
	if r == nil {
		return 0
	}
	return len(r.All)
}

type CompletionConfig struct {
	MinSuccessful              *int
	ToleratedFailureCount      *int
	ToleratedFailurePercentage *float64
}

type MapFunc func(context *DurableContext, item any, index int, array []any) (any, error)

type MapConfig struct {
	MaxConcurrency   int
	ItemNamer        func(item any, index int) string
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}

type ParallelFunc func(context *DurableContext) (any, error)

type NamedParallelBranch struct {
	Name string
	Func ParallelFunc
}

type ParallelConfig struct {
	MaxConcurrency   int
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}

type ConcurrentExecutionItem struct {
	ID    string `json:"id"`
	Data  any    `json:"data,omitempty"`
	Index int    `json:"index"`
	Name  string `json:"name,omitempty"`
}

type ConcurrentExecutor func(item ConcurrentExecutionItem, childContext *DurableContext) (any, error)

type ConcurrencyConfig struct {
	MaxConcurrency   int
	TopLevelSubType  OperationSubType
	IterationSubType OperationSubType
	SummaryGenerator func(result *BatchResult) string
	Serdes           Serdes
	ItemSerdes       Serdes
	CompletionConfig *CompletionConfig
}
