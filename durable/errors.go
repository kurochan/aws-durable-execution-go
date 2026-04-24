package durable

import (
	"errors"
	"fmt"
)

type DurableOperationError struct {
	Type    string
	Message string
	Cause   error
	Data    string
}

func (e *DurableOperationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

func (e *DurableOperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

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

func NewStepError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "step failed"
	}
	return &DurableOperationError{Type: "StepError", Message: msg, Cause: cause, Data: data}
}

func NewCallbackError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback failed"
	}
	return &DurableOperationError{Type: "CallbackError", Message: msg, Cause: cause, Data: data}
}

func NewCallbackTimeoutError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback timed out"
	}
	return &DurableOperationError{Type: "CallbackTimeoutError", Message: msg, Cause: cause, Data: data}
}

func NewCallbackSubmitterError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "callback submitter failed"
	}
	return &DurableOperationError{Type: "CallbackSubmitterError", Message: msg, Cause: cause, Data: data}
}

func NewInvokeError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "invoke failed"
	}
	return &DurableOperationError{Type: "InvokeError", Message: msg, Cause: cause, Data: data}
}

func NewChildContextError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "child context failed"
	}
	return &DurableOperationError{Type: "ChildContextError", Message: msg, Cause: cause, Data: data}
}

func NewWaitForConditionError(msg string, cause error, data string) error {
	if msg == "" {
		msg = "wait for condition failed"
	}
	return &DurableOperationError{Type: "WaitForConditionError", Message: msg, Cause: cause, Data: data}
}

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

func NewUnrecoverableInvocationError(reason TerminationReason, msg string, cause error) error {
	return &unrecoverable{message: fmt.Sprintf("[Unrecoverable Invocation] %s", msg), terminationReason: reason, cause: cause, invocation: true}
}

func NewUnrecoverableExecutionError(reason TerminationReason, msg string, cause error) error {
	return &unrecoverable{message: fmt.Sprintf("[Unrecoverable Execution] %s", msg), terminationReason: reason, cause: cause, execution: true}
}

func IsUnrecoverableInvocationError(err error) bool {
	var u UnrecoverableError
	if errors.As(err, &u) {
		return u.InvocationLevel()
	}
	return false
}

func IsUnrecoverableError(err error) bool {
	var u UnrecoverableError
	return errors.As(err, &u)
}

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

type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

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
