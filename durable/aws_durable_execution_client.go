package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
)

type AWSDurableExecutionClient struct {
	client *lambdasdk.Client
}

func NewAWSDurableExecutionClient(client *lambdasdk.Client) *AWSDurableExecutionClient {
	return &AWSDurableExecutionClient{client: client}
}

func NewDefaultAWSDurableExecutionClient(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (*AWSDurableExecutionClient, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &AWSDurableExecutionClient{
		client: lambdasdk.NewFromConfig(cfg),
	}, nil
}

func (c *AWSDurableExecutionClient) GetExecutionState(input GetExecutionStateRequest) (GetExecutionStateResponse, error) {
	if c == nil || c.client == nil {
		return GetExecutionStateResponse{}, fmt.Errorf("aws durable execution client is not initialized")
	}
	out, err := c.client.GetDurableExecutionState(context.Background(), &lambdasdk.GetDurableExecutionStateInput{
		DurableExecutionArn: &input.DurableExecutionArn,
		CheckpointToken:     &input.CheckpointToken,
		Marker:              optionalString(input.Marker),
		MaxItems:            int32(input.MaxItems),
	})
	if err != nil {
		return GetExecutionStateResponse{}, wrapAWSClientError(err)
	}
	ops, err := convertOpsFromAWS(out.Operations)
	if err != nil {
		return GetExecutionStateResponse{}, err
	}
	return GetExecutionStateResponse{
		Operations: ops,
		NextMarker: stringValue(out.NextMarker),
	}, nil
}

func (c *AWSDurableExecutionClient) Checkpoint(input CheckpointRequest) (CheckpointResponse, error) {
	if c == nil || c.client == nil {
		return CheckpointResponse{}, fmt.Errorf("aws durable execution client is not initialized")
	}
	updates, err := convertUpdatesToAWS(input.Updates)
	if err != nil {
		return CheckpointResponse{}, err
	}
	out, err := c.client.CheckpointDurableExecution(context.Background(), &lambdasdk.CheckpointDurableExecutionInput{
		DurableExecutionArn: &input.DurableExecutionArn,
		CheckpointToken:     &input.CheckpointToken,
		Updates:             updates,
	})
	if err != nil {
		return CheckpointResponse{}, wrapAWSClientError(err)
	}
	resp := CheckpointResponse{
		CheckpointToken: stringValue(out.CheckpointToken),
	}
	if out.NewExecutionState != nil {
		ops, err := convertOpsFromAWS(out.NewExecutionState.Operations)
		if err != nil {
			return CheckpointResponse{}, err
		}
		resp.NewExecutionState = &ExecutionState{
			Operations: ops,
			NextMarker: stringValue(out.NewExecutionState.NextMarker),
		}
	}
	return resp, nil
}

func convertOpsFromAWS(in []lambdatypes.Operation) ([]Operation, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal aws operations: %w", err)
	}
	var out []Operation
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal aws operations: %w", err)
	}
	return out, nil
}

func convertUpdatesToAWS(in []OperationUpdate) ([]lambdatypes.OperationUpdate, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal operation updates: %w", err)
	}
	var out []lambdatypes.OperationUpdate
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal operation updates: %w", err)
	}
	return out, nil
}

func wrapAWSClientError(err error) error {
	statusCode := 0
	code := ""
	message := err.Error()

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code = apiErr.ErrorCode()
		if apiErr.ErrorMessage() != "" {
			message = apiErr.ErrorMessage()
		}
	}
	var responseErr *awshttp.ResponseError
	if errors.As(err, &responseErr) {
		statusCode = responseErr.HTTPStatusCode()
	}

	if code != "" || statusCode != 0 {
		return &APIError{
			StatusCode: statusCode,
			Code:       code,
			Message:    message,
		}
	}
	return err
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func optionalString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
