package durable

import "encoding/json"

// BatchResultSerdes serializes BatchResult values for Map, Parallel, and
// ExecuteConcurrently.
type BatchResultSerdes struct{}

// Serialize converts a BatchResult or compatible value to JSON.
func (BatchResultSerdes) Serialize(value any, _ SerdesContext) (string, error) {
	if value == nil {
		return "", nil
	}
	switch v := value.(type) {
	case *BatchResult:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case BatchResult:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

// Deserialize converts JSON into a *BatchResult.
func (BatchResultSerdes) Deserialize(data string, _ SerdesContext) (any, error) {
	if data == "" {
		return &BatchResult{
			All:              []BatchItem{},
			CompletionReason: BatchCompletionReasonAllCompleted,
		}, nil
	}
	var out BatchResult
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return nil, err
	}
	if out.All == nil {
		out.All = []BatchItem{}
	}
	if out.CompletionReason == "" {
		out.CompletionReason = BatchCompletionReasonAllCompleted
	}
	return &out, nil
}
