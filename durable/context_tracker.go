package durable

import (
	"context"
	"errors"
	"fmt"
)

type activeOperationKey struct{}

type ActiveOperation struct {
	ContextID string
	ParentID  string
	Attempt   int
	Mode      DurableExecutionMode
}

func WithActiveOperation(ctx context.Context, op ActiveOperation) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, activeOperationKey{}, op)
}

func GetActiveOperation(ctx context.Context) (ActiveOperation, bool) {
	if ctx == nil {
		return ActiveOperation{}, false
	}
	op, ok := ctx.Value(activeOperationKey{}).(ActiveOperation)
	return op, ok
}

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
