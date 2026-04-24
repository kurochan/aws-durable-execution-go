package durable

import (
	"encoding/json"
	"fmt"
)

type SerdesContext struct {
	EntityID            string
	DurableExecutionArn string
}

type Serdes interface {
	Serialize(value any, ctx SerdesContext) (string, error)
	Deserialize(data string, ctx SerdesContext) (any, error)
}

type JSONSerdes struct{}

type PassThroughSerdes struct{}

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

func (PassThroughSerdes) Deserialize(data string, _ SerdesContext) (any, error) {
	return data, nil
}

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
