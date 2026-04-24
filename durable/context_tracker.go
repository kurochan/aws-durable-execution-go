package durable

import (
	"context"
	"errors"
	"fmt"
)

type activeOperationKey struct{}

// ActiveOperation identifies the durable context currently associated with a
// context.Context.
type ActiveOperation struct {
	ContextID string
	ParentID  string
	Attempt   int
	Mode      DurableExecutionMode
}

// WithActiveOperation stores active durable operation metadata in ctx.
func WithActiveOperation(ctx context.Context, op ActiveOperation) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, activeOperationKey{}, op)
}

// GetActiveOperation returns active durable operation metadata from ctx.
func GetActiveOperation(ctx context.Context) (ActiveOperation, bool) {
	if ctx == nil {
		return ActiveOperation{}, false
	}
	op, ok := ctx.Value(activeOperationKey{}).(ActiveOperation)
	return op, ok
}

// ValidateContextUsage terminates the invocation when a durable operation is
// called with a context from a different DurableContext.
func ValidateContextUsage(ctx context.Context, operationContextID, operationName string, tm *TerminationManager) {
	active, ok := GetActiveOperation(ctx)
	if !ok {
		return
	}
	expected := active.ContextID
	actual := operationContextID
	if actual == "" {
		actual = "root"
	}
	if expected == "" {
		expected = "root"
	}
	if expected == actual {
		return
	}
	msg := fmt.Sprintf("Context usage error in %q: expected context ID %q, got %q", operationName, expected, actual)
	tm.Terminate(TerminationDetails{Reason: TerminationReasonContextValidation, Message: msg, Error: errors.New(msg)})
}
