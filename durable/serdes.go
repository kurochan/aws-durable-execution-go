package durable

import (
	"encoding/json"
	"fmt"
)

// SerdesContext provides operation metadata to serializers.
type SerdesContext struct {
	// EntityID is the durable operation ID before backend hashing.
	EntityID string
	// DurableExecutionArn identifies the current durable execution.
	DurableExecutionArn string
}

// Serdes serializes values into checkpoint payload strings and deserializes
// them back into Go values.
type Serdes interface {
	Serialize(value any, ctx SerdesContext) (string, error)
	Deserialize(data string, ctx SerdesContext) (any, error)
}

// JSONSerdes serializes values with encoding/json.
type JSONSerdes struct{}

// PassThroughSerdes passes string values through unchanged and JSON-serializes
// non-string values.
type PassThroughSerdes struct{}

// Serialize converts value to a JSON string.
func (JSONSerdes) Serialize(value any, _ SerdesContext) (string, error) {
	if value == nil {
		return "", nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Deserialize converts a JSON string to a Go value.
func (JSONSerdes) Deserialize(data string, _ SerdesContext) (any, error) {
	if data == "" {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil, err
	}
	return v, nil
}

// Serialize returns string values unchanged and JSON-serializes other values.
func (PassThroughSerdes) Serialize(value any, _ SerdesContext) (string, error) {
	if value == nil {
		return "", nil
	}
	if s, ok := value.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Deserialize returns data unchanged.
func (PassThroughSerdes) Deserialize(data string, _ SerdesContext) (any, error) {
	return data, nil
}

// SafeSerialize serializes value and terminates the invocation on serialization
// failure.
func SafeSerialize(serdes Serdes, value any, stepID, stepName string, tm *TerminationManager, durableExecutionArn string) (string, error) {
	if serdes == nil {
		serdes = JSONSerdes{}
	}
	payload, err := serdes.Serialize(value, SerdesContext{EntityID: stepID, DurableExecutionArn: durableExecutionArn})
	if err != nil {
		msg := fmt.Sprintf("serialization failed for step %q (%s): %v", stepName, stepID, err)
		tm.Terminate(TerminationDetails{Reason: TerminationReasonSerdesFailed, Message: msg})
		return "", NewUnrecoverableInvocationError(TerminationReasonSerdesFailed, msg, err)
	}
	return payload, nil
}

// SafeDeserialize deserializes data and terminates the invocation on
// deserialization failure.
func SafeDeserialize(serdes Serdes, data string, stepID, stepName string, tm *TerminationManager, durableExecutionArn string) (any, error) {
	if serdes == nil {
		serdes = JSONSerdes{}
	}
	result, err := serdes.Deserialize(data, SerdesContext{EntityID: stepID, DurableExecutionArn: durableExecutionArn})
	if err != nil {
		msg := fmt.Sprintf("deserialization failed for step %q (%s): %v", stepName, stepID, err)
		tm.Terminate(TerminationDetails{Reason: TerminationReasonSerdesFailed, Message: msg})
		return nil, NewUnrecoverableInvocationError(TerminationReasonSerdesFailed, msg, err)
	}
	return result, nil
}
