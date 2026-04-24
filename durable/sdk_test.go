package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestWithDurableExecution_StepInvoke(t *testing.T) {
	client := NewInMemoryClient()
	wrapped := WithDurableExecution(func(ctx context.Context, event any, dctx *DurableContext) (any, error) {
		stepV, err := dctx.Step(ctx, "build-message", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"ok": true, "event": event}, nil
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}

		invokeV, err := dctx.Invoke(ctx, "echo-invoke", "echo-fn", map[string]any{"from": "invoke"}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"step":   stepV,
			"invoke": invokeV,
		}, nil
	}, DurableExecutionConfig{Client: client})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := wrapped(ctx, InvocationInput{
		DurableExecutionArn: "arn:test:execution:1",
		CheckpointToken:     "tok-1",
		InitialExecutionState: ExecutionState{Operations: []Operation{{
			ID:               "execution-1",
			Type:             OperationTypeExecution,
			Status:           OperationStatusStarted,
			ExecutionDetails: &ExecutionDetails{InputPayload: `{"name":"alice"}`},
		}}},
	})
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusSucceeded {
		t.Fatalf("unexpected status: %s", out.Status)
	}
	if out.Result == "" {
		t.Fatalf("result should not be empty")
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out.Result), &decoded); err != nil {
		t.Fatalf("result is not json: %v", err)
	}
	if decoded["step"] == nil || decoded["invoke"] == nil {
		t.Fatalf("result should include step and invoke fields: %#v", decoded)
	}
}

func TestWithDurableExecution_WaitReturnsPending(t *testing.T) {
	client := NewInMemoryClient()
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		if _, err := dctx.Wait(ctx, "tiny-wait", Duration{Seconds: 30}).Await(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"done": true}, nil
	}, DurableExecutionConfig{Client: client})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := wrapped(ctx, InvocationInput{
		DurableExecutionArn: "arn:test:execution:wait",
		CheckpointToken:     "tok-wait",
		InitialExecutionState: ExecutionState{Operations: []Operation{{
			ID:               "execution-wait",
			Type:             OperationTypeExecution,
			Status:           OperationStatusStarted,
			ExecutionDetails: &ExecutionDetails{InputPayload: `{"name":"alice"}`},
		}}},
	})
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusPending {
		t.Fatalf("expected PENDING for wait suspension, got: %s", out.Status)
	}
}

func TestReplay_StepUsesCachedResult(t *testing.T) {
	client := NewInMemoryClient()
	cachedStepID := HashID("1")
	now := time.Now()
	client.SetOperation(Operation{
		ID:             cachedStepID,
		Type:           OperationTypeStep,
		SubType:        OperationSubTypeStep,
		Name:           "cached-step",
		Status:         OperationStatusSucceeded,
		StartTimestamp: &now,
		StepDetails:    &StepDetails{Result: `{"value":42}`},
	})

	called := 0
	wrapped := WithDurableExecution(func(ctx context.Context, _ any, dctx *DurableContext) (any, error) {
		v, err := dctx.Step(ctx, "cached-step", func(_ context.Context, _ StepContext) (any, error) {
			called++
			return map[string]any{"value": 99}, nil
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}
		return v, nil
	}, DurableExecutionConfig{Client: client})

	out, err := wrapped(context.Background(), InvocationInput{
		DurableExecutionArn: "arn:test:execution:2",
		CheckpointToken:     "tok-2",
		InitialExecutionState: ExecutionState{Operations: []Operation{
			{
				ID:               "execution-2",
				Type:             OperationTypeExecution,
				Status:           OperationStatusStarted,
				ExecutionDetails: &ExecutionDetails{InputPayload: `{"x":1}`},
			},
			{
				ID:          cachedStepID,
				Type:        OperationTypeStep,
				SubType:     OperationSubTypeStep,
				Name:        "cached-step",
				Status:      OperationStatusSucceeded,
				StepDetails: &StepDetails{Result: `{"value":42}`},
			},
		}},
	})
	if err != nil {
		t.Fatalf("wrapped returned error: %v", err)
	}
	if out.Status != InvocationStatusSucceeded {
		t.Fatalf("unexpected status: %s", out.Status)
	}
	if called != 0 {
		t.Fatalf("step function should not be called in replay; called=%d", called)
	}
	if out.Result == "" {
		t.Fatalf("expected non-empty replay result")
	}
}
