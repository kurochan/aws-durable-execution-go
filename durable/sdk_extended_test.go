package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func newInvocationInput(arn, token, payload string, extra ...Operation) InvocationInput {
	ops := []Operation{{
		ID:               "execution-root",
		Type:             OperationTypeExecution,
		Status:           OperationStatusStarted,
		ExecutionDetails: &ExecutionDetails{InputPayload: payload},
	}}
	ops = append(ops, extra...)
	return InvocationInput{
		DurableExecutionArn: arn,
		CheckpointToken:     token,
		InitialExecutionState: ExecutionState{
			Operations: ops,
		},
	}
}

func TestWaitForCallback_CompletesInSameInvocation(t *testing.T) {
	client := NewInMemoryClient()
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		v, err := dctx.WaitForCallback(ctx, "approval", func(callbackID string, _ WaitForCallbackContext) error {
			client.CompleteCallback(callbackID, "approved")
			return nil
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"approval": v}, nil
	}, DurableExecutionConfig{Client: client})

	out, err := wrapped(context.Background(), newInvocationInput("arn:test:callback", "tok-callback", `{"kind":"approval"}`))
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", out.Status)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out.Result), &decoded); err != nil {
		t.Fatalf("failed to decode output: %v", err)
	}
	if decoded["approval"] != "approved" {
		t.Fatalf("unexpected callback value: %#v", decoded["approval"])
	}
}

func TestCreateCallback_ReplayTimeoutFailure(t *testing.T) {
	client := NewInMemoryClient()
	stepID := HashID("1")
	now := time.Now()
	timeoutOp := Operation{
		ID:             stepID,
		Name:           "approval-callback",
		Type:           OperationTypeCallback,
		SubType:        OperationSubTypeCallback,
		Status:         OperationStatusTimedOut,
		StartTimestamp: &now,
		CallbackDetails: &CallbackDetails{
			CallbackID: stepID,
			Error: (&DurableOperationError{
				Type:    "CallbackTimeoutError",
				Message: "callback timed out",
			}).ToErrorObject(),
		},
	}
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		cb, err := dctx.CreateCallback(ctx, "approval-callback", nil).Await(ctx)
		if err != nil {
			return nil, err
		}
		_, err = cb.Promise.Await(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, nil
	}, DurableExecutionConfig{Client: client})

	out, err := wrapped(context.Background(), newInvocationInput(
		"arn:test:callback-timeout",
		"tok-timeout",
		`{}`,
		timeoutOp,
	))
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusFailed {
		t.Fatalf("expected FAILED, got %s", out.Status)
	}
	if out.Error == nil || out.Error.ErrorType != "CallbackTimeoutError" {
		t.Fatalf("expected CallbackTimeoutError, got %#v", out.Error)
	}
}

func TestWaitForCondition_ReplayUsesCachedResult(t *testing.T) {
	client := NewInMemoryClient()
	stepID := HashID("1")
	cached := Operation{
		ID:      stepID,
		Name:    "condition-check",
		Type:    OperationTypeStep,
		SubType: OperationSubTypeWaitForCondition,
		Status:  OperationStatusSucceeded,
		StepDetails: &StepDetails{
			Result: `{"ready":true,"attempts":2}`,
		},
	}

	called := 0
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		v, err := dctx.WaitForCondition(ctx, "condition-check", func(state any, _ WaitForConditionContext) (any, error) {
			called++
			return state, nil
		}, &WaitForConditionConfig{
			InitialState: map[string]any{"ready": false},
			WaitStrategy: func(state any, attempt int) WaitForConditionDecision {
				return WaitForConditionDecision{ShouldContinue: attempt < 3, Delay: Duration{Seconds: 1}}
			},
		}).Await(ctx)
		if err != nil {
			return nil, err
		}
		return v, nil
	}, DurableExecutionConfig{Client: client})

	out, err := wrapped(context.Background(), newInvocationInput("arn:test:wfc-replay", "tok-wfc", `{}`, cached))
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", out.Status)
	}
	if called != 0 {
		t.Fatalf("check function should not run in replay, called=%d", called)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out.Result), &decoded); err != nil {
		t.Fatalf("failed to decode output: %v", err)
	}
	if decoded["ready"] != true {
		t.Fatalf("unexpected waitForCondition output: %#v", decoded)
	}
}

func TestMapAndParallel(t *testing.T) {
	client := NewInMemoryClient()
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		items := []any{1, 2, 3}
		mapResult, err := dctx.Map(ctx, "map-double", items, func(child *DurableContext, item any, index int, _ []any) (any, error) {
			n, ok := item.(int)
			if !ok {
				return nil, fmt.Errorf("unexpected item type: %T", item)
			}
			return child.Step(context.Background(), fmt.Sprintf("map-%d", index), func(_ context.Context, _ StepContext) (any, error) {
				return n * 2, nil
			}, nil).Await(context.Background())
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}

		parallelResult, err := dctx.Parallel(ctx, "parallel-branches", []NamedParallelBranch{
			{
				Name: "a",
				Func: func(child *DurableContext) (any, error) {
					return child.Step(context.Background(), "parallel-a", func(_ context.Context, _ StepContext) (any, error) {
						return "A", nil
					}, nil).Await(context.Background())
				},
			},
			{
				Name: "b",
				Func: func(child *DurableContext) (any, error) {
					return child.Step(context.Background(), "parallel-b", func(_ context.Context, _ StepContext) (any, error) {
						return "B", nil
					}, nil).Await(context.Background())
				},
			},
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"map_total":         mapResult.TotalCount(),
			"map_success":       mapResult.SuccessCount(),
			"parallel_total":    parallelResult.TotalCount(),
			"parallel_success":  parallelResult.SuccessCount(),
			"parallel_failures": parallelResult.FailureCount(),
		}, nil
	}, DurableExecutionConfig{Client: client})

	out, err := wrapped(context.Background(), newInvocationInput("arn:test:map-parallel", "tok-map-parallel", `{}`))
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", out.Status)
	}
	var decoded map[string]float64
	if err := json.Unmarshal([]byte(out.Result), &decoded); err != nil {
		t.Fatalf("failed to decode output: %v", err)
	}
	if decoded["map_total"] != 3 || decoded["map_success"] != 3 {
		t.Fatalf("unexpected map result summary: %#v", decoded)
	}
	if decoded["parallel_total"] != 2 || decoded["parallel_success"] != 2 {
		t.Fatalf("unexpected parallel result summary: %#v", decoded)
	}
}
