package durable

import (
	"errors"
	"fmt"
)

// DurableOperationError is the common error type used for durable operation
// failures that can be serialized into ErrorObject.
type DurableOperationError struct {
	Type    string
	Message string
	Cause   error
	Data    string
}

// Error returns the durable operation error message.
func (e *DurableOperationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// Unwrap returns the underlying cause.
func (e *DurableOperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ToErrorObject converts e into a serializable ErrorObject.
func (e *DurableOperationError) ToErrorObject() *ErrorObject {
	if e == nil {
		return nil
	}
	eo := &ErrorObject{
		ErrorType:    e.Type,
		ErrorMessage: e.Message,
		ErrorData:    e.Data,
	}
	if e.Cause != nil {
		eo.StackTrace = []string{e.Cause.Error()}
	}
	return eo
}

// DurableOperationErrorFromErrorObject converts a checkpointed ErrorObject into
// a Go error.
func DurableOperationErrorFromErrorObject(eo *ErrorObject) error {
	if eo == nil {
		return errors.New("unknown durable operation error")
	}
	msg := eo.ErrorMessage
	if msg == "" {
		msg = "unknown durable operation error"
	}
	return &DurableOperationError{
		Type:    eo.ErrorType,
		Message: msg,
		Data:    eo.ErrorData,
	}
}

// NewStepError creates an error for a failed Step operation.
func NewStepError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "step failed"
	}
	return &DurableOperationError{Type: "StepError", Message: msg, Cause: cause, Data: data}
}

// NewCallbackError creates an error for a failed callback operation.
func NewCallbackError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback failed"
	}
	return &DurableOperationError{Type: "CallbackError", Message: msg, Cause: cause, Data: data}
}

// NewCallbackTimeoutError creates an error for a timed-out callback.
func NewCallbackTimeoutError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback timed out"
	}
	return &DurableOperationError{Type: "CallbackTimeoutError", Message: msg, Cause: cause, Data: data}
}

// NewCallbackSubmitterError creates an error for a failed WaitForCallback
// submitter step.
func NewCallbackSubmitterError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback submitter failed"
	}
	return &DurableOperationError{Type: "CallbackSubmitterError", Message: msg, Cause: cause, Data: data}
}

// NewInvokeError creates an error for a failed chained invocation.
func NewInvokeError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "invoke failed"
	}
	return &DurableOperationError{Type: "InvokeError", Message: msg, Cause: cause, Data: data}
}

// NewChildContextError creates an error for a failed child context.
func NewChildContextError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "child context failed"
	}
	return &DurableOperationError{Type: "ChildContextError", Message: msg, Cause: cause, Data: data}
}

// NewWaitForConditionError creates an error for a failed WaitForCondition.
func NewWaitForConditionError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "wait for condition failed"
	}
	return &DurableOperationError{Type: "WaitForConditionError", Message: msg, Cause: cause, Data: data}
}

// UnrecoverableError marks an error that should stop the current invocation or
// execution instead of being converted into InvocationStatusFailed.
type UnrecoverableError interface {
	error
	TerminationReason() TerminationReason
	InvocationLevel() bool
	ExecutionLevel() bool
}

type unrecoverable struct {
	message           string
	terminationReason TerminationReason
	cause             error
	invocation        bool
	execution         bool
}

func (u *unrecoverable) Error() string {
	if u == nil {
		return "<nil>"
	}
	return u.message
}

func (u *unrecoverable) Unwrap() error { return u.cause }

func (u *unrecoverable) TerminationReason() TerminationReason { return u.terminationReason }
func (u *unrecoverable) InvocationLevel() bool                { return u.invocation }
func (u *unrecoverable) ExecutionLevel() bool                 { return u.execution }

// NewUnrecoverableInvocationError creates an unrecoverable error scoped to the
// current invocation.
func NewUnrecoverableInvocationError(reason TerminationReason, msg string, cause error) error {
	return &unrecoverable{message: fmt.Sprintf("[Unrecoverable Invocation] %s", msg), terminationReason: reason, cause: cause, invocation: true}
}

// NewUnrecoverableExecutionError creates an unrecoverable error scoped to the
// whole durable execution.
func NewUnrecoverableExecutionError(reason TerminationReason, msg string, cause error) error {
	return &unrecoverable{message: fmt.Sprintf("[Unrecoverable Execution] %s", msg), terminationReason: reason, cause: cause, execution: true}
}

// IsUnrecoverableInvocationError reports whether err is unrecoverable at the
// invocation level.
func IsUnrecoverableInvocationError(err error) bool {
	var u UnrecoverableError
	if errors.As(err, &u) {
		return u.InvocationLevel()
	}
	return false
}

// IsUnrecoverableError reports whether err implements UnrecoverableError.
func IsUnrecoverableError(err error) bool {
	var u UnrecoverableError
	return errors.As(err, &u)
}

// ClassifyCheckpointError classifies backend checkpoint errors into
// unrecoverable invocation or execution errors.
func ClassifyCheckpointError(err error) error {
	var ae *APIError
	if errors.As(err, &ae) {
		if ae.StatusCode >= 400 && ae.StatusCode < 500 && ae.Code == "InvalidParameterValueException" && startsWith(ae.Message, "Invalid Checkpoint Token") {
			return NewUnrecoverableInvocationError(TerminationReasonCheckpointFailed, fmt.Sprintf("Checkpoint failed: %s", ae.Message), err)
		}
		if ae.StatusCode >= 400 && ae.StatusCode < 500 && ae.StatusCode != 429 {
			return NewUnrecoverableExecutionError(TerminationReasonCheckpointFailed, fmt.Sprintf("Checkpoint failed: %s", ae.Message), err)
		}
		return NewUnrecoverableInvocationError(TerminationReasonCheckpointFailed, fmt.Sprintf("Checkpoint failed: %s", ae.Message), err)
	}
	return NewUnrecoverableInvocationError(TerminationReasonCheckpointFailed, fmt.Sprintf("Checkpoint failed: %v", err), err)
}

// APIError is a normalized backend API error used for checkpoint error
// classification.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

// Error returns a formatted backend API error message.
func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("api error: status=%d code=%s message=%s", e.StatusCode, e.Code, e.Message)
}

func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// CreateErrorObjectFromError converts err into the serialized ErrorObject
// representation used in invocation outputs and checkpoints.
func CreateErrorObjectFromError(err error, data string) *ErrorObject {
	if err == nil {
		return &ErrorObject{ErrorMessage: "unknown error", ErrorData: data}
	}
	var de *DurableOperationError
	if errors.As(err, &de) {
		eo := de.ToErrorObject()
		if eo.ErrorData == "" {
			eo.ErrorData = data
		}
		return eo
	}
	return &ErrorObject{
		ErrorType:    fmt.Sprintf("%T", err),
		ErrorMessage: err.Error(),
		ErrorData:    data,
	}
}
