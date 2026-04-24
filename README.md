# aws-durable-execution-go

An unofficial and experimental Go implementation of the AWS Durable Execution SDK.
Use it as the `durable` package.

This repository is an independent experimental implementation and is not an official AWS SDK.

## Reference Implementation

This implementation is based on behavior and API concepts from the official
[AWS Durable Execution SDK for JavaScript](https://github.com/aws/aws-durable-execution-sdk-js).
It aims to provide similar durable workflow primitives for Go, while remaining an unofficial experimental project.

## Setup

This module supports Go 1.25 or later.

```bash
go mod tidy
go test ./...
```

Import path:

```go
import "github.com/kurochan/aws-durable-execution-go/durable"
```

## Minimal Example

Wrap your handler with `WithDurableExecution`.

```go
client := durable.NewInMemoryClient() // Replace this with a DurableExecutionClient implementation in production.

wrapped := durable.WithDurableExecution(
	func(ctx context.Context, event any, dctx *durable.DurableContext) (any, error) {
		v, err := dctx.Step(ctx, "hello-step", func(_ context.Context, _ durable.StepContext) (any, error) {
			return map[string]any{"ok": true, "event": event}, nil
		}, nil).Await(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"step": v}, nil
	},
	durable.DurableExecutionConfig{Client: client},
)

out, err := wrapped(context.Background(), durable.InvocationInput{
	DurableExecutionArn: "arn:test:execution:1",
	CheckpointToken:     "token-1",
	InitialExecutionState: durable.ExecutionState{
		Operations: []durable.Operation{{
			ID:     "execution-root",
			Type:   durable.OperationTypeExecution,
			Status: durable.OperationStatusStarted,
			ExecutionDetails: &durable.ExecutionDetails{
				InputPayload: `{"name":"alice"}`,
			},
		}},
	},
})
```

## Implemented APIs

- `Step`
- `Wait`
- `Invoke`
- `RunInChildContext`
- `CreateCallback`
- `WaitForCallback`
- `WaitForCondition`
- `Map`
- `Parallel`
- `ExecuteConcurrently`

All APIs return a `Future`; call `Await(ctx)` to wait for completion.

## API Usage

### Step

```go
res, err := dctx.Step(ctx, "fetch-user", func(_ context.Context, _ durable.StepContext) (any, error) {
	return map[string]any{"id": "u-1"}, nil
}, &durable.StepConfig{
	Semantics: durable.StepSemanticsAtLeastOncePerRetry,
}).Await(ctx)
```

### Wait

```go
_, err := dctx.Wait(ctx, "cooldown", durable.Duration{Seconds: 30}).Await(ctx)
```

### Invoke

```go
out, err := dctx.Invoke(ctx, "invoke-worker", "worker-func", map[string]any{"job": "a"}, nil).Await(ctx)
```

### RunInChildContext

```go
out, err := dctx.RunInChildContext(ctx, "child", func(childCtx context.Context, child *durable.DurableContext) (any, error) {
	return child.Step(childCtx, "child-step", func(_ context.Context, _ durable.StepContext) (any, error) {
		return "ok", nil
	}, nil).Await(childCtx)
}, nil).Await(ctx)
```

### CreateCallback / WaitForCallback

```go
cb, err := dctx.CreateCallback(ctx, "approval", &durable.CreateCallbackConfig{
	Timeout: &durable.Duration{Minutes: 5},
}).Await(ctx)
if err != nil {
	return nil, err
}

go func() {
	// An external system is expected to use callbackId to complete the callback.
	_ = cb.CallbackID
}()

approval, err := cb.Promise.Await(ctx)
```

```go
approval, err := dctx.WaitForCallback(ctx, "approval-flow",
	func(callbackID string, cbCtx durable.WaitForCallbackContext) error {
		cbCtx.Logger.Infof("submit callback id=%s", callbackID)
		return nil
	},
	nil,
).Await(ctx)
```

### WaitForCondition

```go
state, err := dctx.WaitForCondition(ctx, "poll-status",
	func(state any, _ durable.WaitForConditionContext) (any, error) {
		m := state.(map[string]any)
		m["attempt"] = m["attempt"].(int) + 1
		if m["attempt"].(int) >= 3 {
			m["done"] = true
		}
		return m, nil
	},
	&durable.WaitForConditionConfig{
		InitialState: map[string]any{"attempt": 0, "done": false},
		WaitStrategy: func(state any, attempt int) durable.WaitForConditionDecision {
			m := state.(map[string]any)
			if m["done"] == true {
				return durable.WaitForConditionDecision{ShouldContinue: false}
			}
			return durable.WaitForConditionDecision{
				ShouldContinue: true,
				Delay:          durable.Duration{Seconds: 10},
			}
		},
	},
).Await(ctx)
```

### Map / Parallel

```go
items := []any{1, 2, 3}
mapped, err := dctx.Map(ctx, "double", items,
	func(child *durable.DurableContext, item any, index int, _ []any) (any, error) {
		n := item.(int)
		return n * 2, nil
	},
	nil,
).Await(ctx)
```

```go
parallel, err := dctx.Parallel(ctx, "branches", []durable.NamedParallelBranch{
	{Name: "a", Func: func(_ *durable.DurableContext) (any, error) { return "A", nil }},
	{Name: "b", Func: func(_ *durable.DurableContext) (any, error) { return "B", nil }},
}, nil).Await(ctx)
```

`BatchResult` exposes the following fields and helpers.

- `All`
- `Succeeded()`
- `Failed()`
- `Started()`
- `SuccessCount()`
- `FailureCount()`
- `TotalCount()`
- `CompletionReason`

## Custom Client Implementation

Implement `DurableExecutionClient` to plug in a backend client.

```go
type DurableExecutionClient interface {
	GetExecutionState(ctx context.Context, input GetExecutionStateRequest) (GetExecutionStateResponse, error)
	Checkpoint(ctx context.Context, input CheckpointRequest) (CheckpointResponse, error)
}
```

## Important Notes

- This implementation is experimental. Public APIs and checkpoint payload compatibility may change.
- The durable operation call order inside a handler must be identical during replay.
- If an existing checkpoint does not match `type/name/subtype`, execution stops with a non-deterministic error.
- `Wait`, `Callback`, and `Retry` waits return `InvocationStatusPending`.
- `RunInChildContext` enters `ReplayChildren` reconstruction mode when the result exceeds 256 KB.
- If the final handler return value exceeds 6 MB, the execution result is checkpointed and the returned body is empty.

## License

Apache License 2.0

## Current Status

- `go test ./...` passes.
- Additional compatibility tests can be added to verify stricter behavior parity with the JS SDK.

## Sample Lambda

The Lambda sample is available here.

- [examples/lambda-sample/main.go](examples/lambda-sample/main.go)
- [examples/lambda-sample/README.md](examples/lambda-sample/README.md)

## Related Blog

- [CyberAgent Developers Blog article about Durable Execution and this project (Japanese)](https://developers.cyberagent.co.jp/blog/archives/63635/)
