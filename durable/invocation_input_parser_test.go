package durable

import (
	"testing"
	"time"
)

func TestParseInvocationInput_AcceptsStringTimestamp(t *testing.T) {
	raw := []byte(`{
		"DurableExecutionArn":"arn:test",
		"CheckpointToken":"token-1",
		"InitialExecutionState":{
			"Operations":[
				{
					"Id":"execution-root",
					"Type":"EXECUTION",
					"Status":"STARTED",
					"StartTimestamp":"2026-02-27T23:00:00.000Z",
					"ExecutionDetails":{"InputPayload":"{\"name\":\"alice\"}"}
				}
			]
		}
	}`)

	inv, err := ParseInvocationInput(raw)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if inv.DurableExecutionArn != "arn:test" || inv.CheckpointToken != "token-1" {
		t.Fatalf("unexpected identity fields: %+v", inv)
	}
	if len(inv.InitialExecutionState.Operations) != 1 {
		t.Fatalf("unexpected operations length: %d", len(inv.InitialExecutionState.Operations))
	}
	ts := inv.InitialExecutionState.Operations[0].StartTimestamp
	if ts == nil || ts.UTC().Format(time.RFC3339Nano) != "2026-02-27T23:00:00Z" {
		t.Fatalf("unexpected timestamp: %v", ts)
	}
}

func TestParseInvocationInput_AcceptsEpochNumberTimestamp(t *testing.T) {
	raw := []byte(`{
		"DurableExecutionArn":"arn:test",
		"CheckpointToken":"token-1",
		"InitialExecutionState":{
			"Operations":[
				{
					"Id":"execution-root",
					"Type":"EXECUTION",
					"Status":"STARTED",
					"StartTimestamp":1772233200.5,
					"ExecutionDetails":{"InputPayload":"{\"name\":\"alice\"}"}
				}
			]
		}
	}`)

	inv, err := ParseInvocationInput(raw)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	got := inv.InitialExecutionState.Operations[0].StartTimestamp
	if got == nil {
		t.Fatalf("missing StartTimestamp")
	}
	if got.UTC().Format(time.RFC3339Nano) != "2026-02-27T23:00:00.5Z" {
		t.Fatalf("unexpected timestamp: %s", got.UTC().Format(time.RFC3339Nano))
	}
}
