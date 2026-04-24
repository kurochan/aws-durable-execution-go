// Package durable provides an experimental Go implementation of AWS Durable
// Execution style workflow helpers.
//
// The package is centered around WithDurableExecution, which wraps a Lambda
// style handler and provides a DurableContext. A handler uses DurableContext to
// record durable operations such as Step, Wait, Invoke, callbacks, child
// contexts, Map, and Parallel. Each operation returns a Future; call Await to
// either receive a replayed result from the checkpoint state or wait for the
// operation to complete.
//
// Durable handlers must be deterministic in their durable operation order. On a
// replay, the SDK validates that the next operation has the same type, subtype,
// and name as the checkpointed operation. Non-deterministic changes fail the
// invocation rather than silently binding results to a different operation.
//
// This repository is an independent experimental implementation and is not an
// official AWS SDK. Public API and checkpoint payload compatibility may change.
package durable
