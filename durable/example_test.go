package durable_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kurochan/aws-durable-execution-go/durable"
)

func exampleInput(arn, token, payload string) durable.InvocationInput {
	return durable.InvocationInput{
		DurableExecutionArn: arn,
		CheckpointToken:     token,
		InitialExecutionState: durable.ExecutionState{
			Operations: []durable.Operation{{
				ID:     "execution-root",
				Type:   durable.OperationTypeExecution,
				Status: durable.OperationStatusStarted,
				ExecutionDetails: &durable.ExecutionDetails{
					InputPayload: payload,
				},
			}},
		},
	}
}

func ExampleWithDurableExecution() {
	client := durable.NewInMemoryClient()
	wrapped := durable.WithDurableExecution(
		func(ctx context.Context, event any, dctx *durable.DurableContext) (any, error) {
			result, err := dctx.Step(ctx, "build-greeting", func(context.Context, durable.StepContext) (any, error) {
				input := event.(map[string]any)
				return "hello " + input["name"].(string), nil
			}, nil).Await(ctx)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		durable.DurableExecutionConfig{Client: client},
	)

	out, err := wrapped(context.Background(), exampleInput(
		"arn:test:execution:example",
		"token-example",
		`{"name":"alice"}`,
	))
	if err != nil {
		panic(err)
	}

	var result string
	if err := json.Unmarshal([]byte(out.Result), &result); err != nil {
		panic(err)
	}
	fmt.Println(out.Status)
	fmt.Println(result)

	// Output:
	// SUCCEEDED
	// hello alice
}

func ExampleDurableContext_Wait() {
	client := durable.NewInMemoryClient()
	wrapped := durable.WithDurableExecution(
		func(ctx context.Context, _ any, dctx *durable.DurableContext) (any, error) {
			if _, err := dctx.Wait(ctx, "cooldown", durable.Duration{Seconds: 30}).Await(ctx); err != nil {
				return nil, err
			}
			return "done", nil
		},
		durable.DurableExecutionConfig{Client: client},
	)

	out, err := wrapped(context.Background(), exampleInput(
		"arn:test:execution:wait-example",
		"token-wait-example",
		`{}`,
	))
	if err != nil {
		panic(err)
	}
	fmt.Println(out.Status)

	// Output:
	// PENDING
}

func ExampleDurableContext_Map() {
	client := durable.NewInMemoryClient()
	wrapped := durable.WithDurableExecution(
		func(ctx context.Context, _ any, dctx *durable.DurableContext) (any, error) {
			result, err := dctx.Map(ctx, "double", []any{1, 2, 3},
				func(_ *durable.DurableContext, item any, _ int, _ []any) (any, error) {
					return item.(int) * 2, nil
				},
				&durable.MapConfig{MaxConcurrency: 2},
			).Await(ctx)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"total":   result.TotalCount(),
				"success": result.SuccessCount(),
				"values":  result.Results(),
			}, nil
		},
		durable.DurableExecutionConfig{Client: client},
	)

	out, err := wrapped(context.Background(), exampleInput(
		"arn:test:execution:map-example",
		"token-map-example",
		`{}`,
	))
	if err != nil {
		panic(err)
	}
	fmt.Println(out.Status)
	fmt.Println(out.Result)

	// Output:
	// SUCCEEDED
	// {"success":3,"total":3,"values":[2,4,6]}
}
