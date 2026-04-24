package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/kurochan/aws-durable-execution-go/durable"
)

var (
	once      sync.Once
	wrapped   durable.WrappedHandler
	setupErr  error
	setupInfo string
)

func isDebugEnabled() bool {
	return strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug")
}

func debugf(format string, args ...any) {
	if isDebugEnabled() {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func newWrapped(ctx context.Context) (durable.WrappedHandler, error) {
	once.Do(func() {
		debugf("initializing durable execution client")
		awsClient, err := durable.NewDefaultAWSDurableExecutionClient(ctx)
		if err != nil {
			setupErr = fmt.Errorf("failed to initialize AWS durable client: %w", err)
			return
		}
		setupInfo = "using AWS Lambda durable execution client"
		client := durable.DurableExecutionClient(awsClient)
		wrapped = durable.WithDurableExecution(handlerCore, durable.DurableExecutionConfig{Client: client})
	})
	return wrapped, setupErr
}

func handlerCore(
	ctx context.Context,
	event any,
	dctx *durable.DurableContext,
) (any, error) {
	payload, _ := event.(map[string]any)
	debugf("handlerCore start, payload keys=%d", len(payload))
	name, _ := payload["name"].(string)
	if name == "" {
		name = "durable-user"
	}
	debugf("resolved name=%q", name)

	greeting, err := dctx.Step(ctx, "build-greeting", func(_ context.Context, _ durable.StepContext) (any, error) {
		return fmt.Sprintf("hello %s", name), nil
	}, nil).Await(ctx)
	if err != nil {
		return nil, err
	}
	debugf("step build-greeting completed")

	childResult, err := dctx.RunInChildContext(ctx, "child-flow", func(childCtx context.Context, child *durable.DurableContext) (any, error) {
		return child.Step(childCtx, "child-step", func(_ context.Context, _ durable.StepContext) (any, error) {
			return map[string]any{"child": true}, nil
		}, nil).Await(childCtx)
	}, nil).Await(ctx)
	if err != nil {
		return nil, err
	}
	debugf("runInChildContext completed")

	if _, err := dctx.Wait(ctx, "wait-demo", durable.Duration{Seconds: 1}).Await(ctx); err != nil {
		return nil, err
	}
	debugf("wait completed")

	parallelResult, err := dctx.Parallel(ctx, "parallel-demo", []durable.NamedParallelBranch{
		{
			Name: "branch-a",
			Func: func(child *durable.DurableContext) (any, error) {
				return child.Step(context.Background(), "parallel-a-step", func(_ context.Context, _ durable.StepContext) (any, error) {
					return "A", nil
				}, nil).Await(context.Background())
			},
		},
		{
			Name: "branch-b",
			Func: func(child *durable.DurableContext) (any, error) {
				return child.Step(context.Background(), "parallel-b-step", func(_ context.Context, _ durable.StepContext) (any, error) {
					return "B", nil
				}, nil).Await(context.Background())
			},
		},
	}, nil).Await(ctx)
	if err != nil {
		return nil, err
	}
	debugf("parallel completed status=%s total=%d", parallelResult.Status(), parallelResult.TotalCount())

	mapResult, err := dctx.Map(ctx, "map-demo", []any{1, 2, 3}, func(_ *durable.DurableContext, item any, _ int, _ []any) (any, error) {
		n, ok := item.(int)
		if !ok {
			return nil, fmt.Errorf("unexpected item type %T", item)
		}
		return n * n, nil
	}, nil).Await(ctx)
	if err != nil {
		return nil, err
	}
	debugf("map completed count=%d", len(mapResult.Results()))

	return map[string]any{
		"setup":           setupInfo,
		"greeting":        greeting,
		"child_result":    childResult,
		"parallel_status": parallelResult.Status(),
		"parallel_total":  parallelResult.TotalCount(),
		"map_results":     mapResult.Results(),
	}, nil
}

func handler(ctx context.Context, raw json.RawMessage) (durable.InvocationOutput, error) {
	debugf("lambda handler invoked, raw payload bytes=%d", len(raw))
	if isDebugEnabled() {
		const max = 2048
		if len(raw) <= max {
			debugf("raw payload=%s", string(raw))
		} else {
			debugf("raw payload (truncated %d/%d bytes)=%s", max, len(raw), string(raw[:max]))
		}
	}
	inv, err := durable.ParseInvocationInput(raw)
	if err != nil {
		return durable.InvocationOutput{}, fmt.Errorf("invalid invocation payload: %w", err)
	}
	debugf(
		"invocation fields arn_set=%t token_set=%t operations=%d",
		inv.DurableExecutionArn != "",
		inv.CheckpointToken != "",
		len(inv.InitialExecutionState.Operations),
	)
	if inv.DurableExecutionArn == "" || inv.CheckpointToken == "" {
		return durable.InvocationOutput{}, fmt.Errorf(
			"unexpected payload provided to start durable execution: DurableExecutionArn and CheckpointToken are required",
		)
	}
	if len(inv.InitialExecutionState.Operations) == 0 {
		return durable.InvocationOutput{}, fmt.Errorf(
			"invalid invocation payload: InitialExecutionState.Operations must include execution operation",
		)
	}
	w, err := newWrapped(ctx)
	if err != nil {
		return durable.InvocationOutput{}, err
	}
	return w(ctx, inv)
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	debugf("debug logging enabled")
	lambda.Start(handler)
}
