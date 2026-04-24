package durable

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// ParseInvocationInput parses invocation payload from durable backend.
// The backend may encode timestamp fields as either ISO-8601 strings or epoch numbers.
func ParseInvocationInput(raw []byte) (InvocationInput, error) {
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return InvocationInput{}, err
	}
	normalizeInvocationTimestamps(normalized)

	b, err := json.Marshal(normalized)
	if err != nil {
		return InvocationInput{}, err
	}
	var inv InvocationInput
	if err := json.Unmarshal(b, &inv); err != nil {
		return InvocationInput{}, err
	}
	return inv, nil
}

func normalizeInvocationTimestamps(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if isTimestampKey(k) {
				if s, ok := convertTimestampToRFC3339(child); ok {
					t[k] = s
					continue
				}
				// Do not fail invocation parsing on unknown timestamp encoding.
				// Keep behavior resilient and allow runtime to continue.
				t[k] = nil
				continue
			}
			normalizeInvocationTimestamps(child)
		}
	case []any:
		for _, child := range t {
			normalizeInvocationTimestamps(child)
		}
	}
}

func isTimestampKey(k string) bool {
	switch k {
	case "StartTimestamp", "EndTimestamp", "NextAttemptTimestamp", "ScheduledEndTimestamp", "ScheduledTimeoutTimestamp":
		return true
	default:
		return false
	}
}

func convertTimestampToRFC3339(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		if t == "" {
			return "", false
		}
		return t, true
	case float64:
		return epochNumberToRFC3339(t)
	case map[string]any:
		if sec, ok := numberFromMap(t, "Seconds", "seconds", "EpochSeconds", "epochSeconds"); ok {
			ns := 0.0
			if n, ok := numberFromMap(t, "Nanos", "nanos", "EpochNanos", "epochNanos"); ok {
				ns = n
			}
			ts := time.Unix(int64(sec), int64(ns)).UTC()
			return ts.Format(time.RFC3339Nano), true
		}
		if rawDate, ok := t["$date"]; ok {
			return convertTimestampToRFC3339(rawDate)
		}
		if millis, ok := numberFromMap(t, "$numberLong", "millis", "milliseconds", "epochMillis", "EpochMillis"); ok {
			return epochNumberToRFC3339(millis)
		}
		if ts, ok := numberFromMap(t, "timestamp", "Timestamp", "time", "Time"); ok {
			return epochNumberToRFC3339(ts)
		}
	}
	return "", false
}

func epochNumberToRFC3339(n float64) (string, bool) {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "", false
	}
	abs := math.Abs(n)
	sec := n
	if abs > 1e12 {
		sec = n / 1000 // assume milliseconds
	}
	i, frac := math.Modf(sec)
	ts := time.Unix(int64(i), int64(frac*1e9)).UTC()
	return ts.Format(time.RFC3339Nano), true
}

func numberFromMap(m map[string]any, keys ...string) (float64, bool) {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		switch n := v.(type) {
		case float64:
			return n, true
		case string:
			if parsed, err := strconv.ParseFloat(n, 64); err == nil {
				return parsed, true
			}
		case json.Number:
			if parsed, err := n.Float64(); err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func mustParseRFC3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(fmt.Sprintf("unexpected timestamp %q: %v", s, err))
	}
	return t
}
