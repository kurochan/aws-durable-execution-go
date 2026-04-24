package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

type concurrencyController struct {
	operationName     string
	skipNextOperation func()
}

func (cc *concurrencyController) isChildEntityCompleted(executionContext *ExecutionContext, parentEntityID string, completedCount int) bool {
	childEntityID := fmt.Sprintf("%s-%d", parentEntityID, completedCount+1)
	childStepData := executionContext.GetStepData(childEntityID)
	return childStepData != nil && isTerminalStatus(childStepData.Status)
}

func (cc *concurrencyController) getCompletionReason(
	failureCount int,
	successCount int,
	completedCount int,
	items []ConcurrentExecutionItem,
	config ConcurrencyConfig,
) BatchCompletionReason {
	completion := config.CompletionConfig
	if completion == nil || (!hasCompletionCriteria(completion)) {
		if failureCount > 0 {
			return BatchCompletionReasonFailureToleranceExceeded
		}
	} else {
		if completion.ToleratedFailureCount != nil && failureCount > *completion.ToleratedFailureCount {
			return BatchCompletionReasonFailureToleranceExceeded
		}
		if completion.ToleratedFailurePercentage != nil && len(items) > 0 {
			failurePct := float64(failureCount) / float64(len(items)) * 100
			if failurePct > *completion.ToleratedFailurePercentage {
				return BatchCompletionReasonFailureToleranceExceeded
			}
		}
	}
	if completedCount == len(items) {
		return BatchCompletionReasonAllCompleted
	}
	if completion != nil && completion.MinSuccessful != nil && successCount >= *completion.MinSuccessful {
		return BatchCompletionReasonMinSuccessfulReached
	}
	return BatchCompletionReasonAllCompleted
}

func hasCompletionCriteria(c *CompletionConfig) bool {
	if c == nil {
		return false
	}
	return c.MinSuccessful != nil || c.ToleratedFailureCount != nil || c.ToleratedFailurePercentage != nil
}

func shouldContinueScheduling(failureCount int, totalCount int, completion *CompletionConfig) bool {
	if completion == nil || !hasCompletionCriteria(completion) {
		return failureCount == 0
	}
	if completion.ToleratedFailureCount != nil && failureCount > *completion.ToleratedFailureCount {
		return false
	}
	if completion.ToleratedFailurePercentage != nil && totalCount > 0 {
		failurePct := float64(failureCount) / float64(totalCount) * 100
		if failurePct > *completion.ToleratedFailurePercentage {
			return false
		}
	}
	return true
}

func (cc *concurrencyController) executeItems(
	ctx context.Context,
	items []ConcurrentExecutionItem,
	executor ConcurrentExecutor,
	parentContext *DurableContext,
	config ConcurrencyConfig,
	durableExecutionMode DurableExecutionMode,
	entityID string,
	executionContext *ExecutionContext,
) (*BatchResult, error) {
	if durableExecutionMode == ReplaySucceededContext && entityID != "" && executionContext != nil {
		targetCount := 0
		if stepData := executionContext.GetStepData(entityID); stepData != nil && stepData.ContextDetails != nil {
			if stepData.ContextDetails.Result != "" {
				var summary struct {
					TotalCount int `json:"totalCount"`
				}
				if err := json.Unmarshal([]byte(stepData.ContextDetails.Result), &summary); err == nil && summary.TotalCount > 0 {
					targetCount = summary.TotalCount
				}
			}
		}
		if targetCount == 0 {
			targetCount = len(items)
		}
		return cc.replayItems(ctx, items, executor, parentContext, config, targetCount, executionContext, entityID)
	}
	return cc.executeItemsConcurrently(ctx, items, executor, parentContext, config)
}

func (cc *concurrencyController) replayItems(
	ctx context.Context,
	items []ConcurrentExecutionItem,
	executor ConcurrentExecutor,
	parentContext *DurableContext,
	config ConcurrencyConfig,
	targetTotalCount int,
	executionContext *ExecutionContext,
	parentEntityID string,
) (*BatchResult, error) {
	resultItems := make([]BatchItem, 0, len(items))
	completedCount := 0
	successCount := 0
	failureCount := 0
	stepCounter := 0

	for _, item := range items {
		if completedCount >= targetTotalCount {
			break
		}

		if !cc.isChildEntityCompleted(executionContext, parentEntityID, stepCounter) {
			cc.skipNextOperation()
			stepCounter++
			continue
		}
		stepCounter++

		result, err := parentContext.RunInChildContext(ctx, itemDisplayName(item), func(childCtx context.Context, child *DurableContext) (any, error) {
			return executor(item, child)
		}, &ChildConfig{
			SubType: config.IterationSubType,
			Serdes:  config.ItemSerdes,
		}).Await(ctx)
		if err != nil {
			failureCount++
			resultItems = append(resultItems, BatchItem{
				Error:  CreateErrorObjectFromError(err, ""),
				Index:  item.Index,
				Status: BatchItemStatusFailed,
			})
		} else {
			successCount++
			resultItems = append(resultItems, BatchItem{
				Result: result,
				Index:  item.Index,
				Status: BatchItemStatusSucceeded,
			})
		}
		completedCount++
	}

	return &BatchResult{
		All:              resultItems,
		CompletionReason: cc.getCompletionReason(failureCount, successCount, completedCount, items, config),
	}, nil
}

func (cc *concurrencyController) executeItemsConcurrently(
	ctx context.Context,
	items []ConcurrentExecutionItem,
	executor ConcurrentExecutor,
	parentContext *DurableContext,
	config ConcurrencyConfig,
) (*BatchResult, error) {
	if len(items) == 0 {
		return &BatchResult{
			All:              []BatchItem{},
			CompletionReason: BatchCompletionReasonAllCompleted,
		}, nil
	}

	maxConcurrency := config.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = len(items)
	}
	if maxConcurrency > len(items) {
		maxConcurrency = len(items)
	}

	results := make([]BatchItem, len(items))
	started := make([]bool, len(items))

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	completedCount := 0
	successCount := 0
	failureCount := 0

	for index, item := range items {
		mu.Lock()
		shouldStart := shouldContinueScheduling(failureCount, len(items), config.CompletionConfig)
		mu.Unlock()
		if !shouldStart {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case sem <- struct{}{}:
		}

		mu.Lock()
		started[index] = true
		results[index] = BatchItem{
			Index:  item.Index,
			Status: BatchItemStatusStarted,
		}
		mu.Unlock()

		wg.Add(1)
		go func(itemIndex int, it ConcurrentExecutionItem) {
			defer wg.Done()
			defer func() { <-sem }()

			res, err := parentContext.RunInChildContext(ctx, itemDisplayName(it), func(childCtx context.Context, child *DurableContext) (any, error) {
				return executor(it, child)
			}, &ChildConfig{
				SubType: config.IterationSubType,
				Serdes:  config.ItemSerdes,
			}).Await(ctx)

			mu.Lock()
			defer mu.Unlock()
			completedCount++
			if err != nil {
				failureCount++
				results[itemIndex] = BatchItem{
					Error:  CreateErrorObjectFromError(err, ""),
					Index:  it.Index,
					Status: BatchItemStatusFailed,
				}
				return
			}
			successCount++
			results[itemIndex] = BatchItem{
				Result: res,
				Index:  it.Index,
				Status: BatchItemStatusSucceeded,
			}
		}(index, item)
	}

	wg.Wait()

	finalItems := make([]BatchItem, 0, len(items))
	for i, item := range results {
		if started[i] {
			finalItems = append(finalItems, item)
		}
	}

	return &BatchResult{
		All:              finalItems,
		CompletionReason: cc.getCompletionReason(failureCount, successCount, completedCount, items, config),
	}, nil
}

func itemDisplayName(item ConcurrentExecutionItem) string {
	if item.Name != "" {
		return item.Name
	}
	if item.ID != "" {
		return item.ID
	}
	return fmt.Sprintf("item-%d", item.Index)
}

func (c *DurableContext) skipNextOperation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepCounter++
}

func normalizeBatchResult(value any) (*BatchResult, error) {
	if value == nil {
		return &BatchResult{
			All:              []BatchItem{},
			CompletionReason: BatchCompletionReasonAllCompleted,
		}, nil
	}
	switch v := value.(type) {
	case *BatchResult:
		if v.All == nil {
			v.All = []BatchItem{}
		}
		if v.CompletionReason == "" {
			v.CompletionReason = BatchCompletionReasonAllCompleted
		}
		return v, nil
	case BatchResult:
		out := v
		if out.All == nil {
			out.All = []BatchItem{}
		}
		if out.CompletionReason == "" {
			out.CompletionReason = BatchCompletionReasonAllCompleted
		}
		return &out, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var out BatchResult
		if err := json.Unmarshal(b, &out); err != nil {
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
}

func (c *DurableContext) executeConcurrentlyInternal(
	ctx context.Context,
	name string,
	items []ConcurrentExecutionItem,
	executor ConcurrentExecutor,
	cfg *ConcurrencyConfig,
) *Future[*BatchResult] {
	phaseDone := make(chan struct{})
	var phaseErr error
	var phaseResult *BatchResult

	go func() {
		defer close(phaseDone)
		if items == nil {
			phaseErr = errors.New("concurrent execution requires an array of items")
			return
		}
		if executor == nil {
			phaseErr = errors.New("concurrent execution requires an executor function")
			return
		}

		conf := ConcurrencyConfig{}
		if cfg != nil {
			conf = *cfg
		}
		if conf.MaxConcurrency < 0 {
			phaseErr = fmt.Errorf("invalid maxConcurrency: %d", conf.MaxConcurrency)
			return
		}
		if conf.Serdes == nil {
			conf.Serdes = BatchResultSerdes{}
		}

		var summaryGenerator func(value any) string
		if conf.SummaryGenerator != nil {
			summaryGenerator = func(value any) string {
				batch, err := normalizeBatchResult(value)
				if err != nil {
					return ""
				}
				return conf.SummaryGenerator(batch)
			}
		}

		raw, err := c.RunInChildContext(ctx, name, func(childCtx context.Context, child *DurableContext) (any, error) {
			controller := &concurrencyController{
				operationName:     "concurrent-execution",
				skipNextOperation: child.skipNextOperation,
			}
			return controller.executeItems(
				childCtx,
				items,
				executor,
				child,
				conf,
				child.mode,
				child.stepPrefix,
				c.execCtx,
			)
		}, &ChildConfig{
			SubType:          conf.TopLevelSubType,
			SummaryGenerator: summaryGenerator,
			Serdes:           conf.Serdes,
		}).Await(ctx)
		if err != nil {
			phaseErr = err
			return
		}
		phaseResult, phaseErr = normalizeBatchResult(raw)
	}()

	return NewFuture(func(awaitCtx context.Context) (*BatchResult, error) {
		select {
		case <-awaitCtx.Done():
			return nil, awaitCtx.Err()
		case <-phaseDone:
			return phaseResult, phaseErr
		}
	})
}

func (c *DurableContext) ExecuteConcurrently(
	ctx context.Context,
	name string,
	items []ConcurrentExecutionItem,
	executor ConcurrentExecutor,
	cfg *ConcurrencyConfig,
) *Future[*BatchResult] {
	ValidateContextUsage(ctx, c.stepPrefix, "_executeConcurrently", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[*BatchResult] {
		return c.executeConcurrentlyInternal(ctx, name, items, executor, cfg)
	})
}

func mapSummaryGenerator(result *BatchResult) string {
	if result == nil {
		return `{"type":"MapResult","totalCount":0,"successCount":0,"failureCount":0,"completionReason":"ALL_COMPLETED","status":"SUCCEEDED"}`
	}
	summary := map[string]any{
		"type":             "MapResult",
		"totalCount":       result.TotalCount(),
		"successCount":     result.SuccessCount(),
		"failureCount":     result.FailureCount(),
		"completionReason": result.CompletionReason,
		"status":           result.Status(),
	}
	b, _ := json.Marshal(summary)
	return string(b)
}

func parallelSummaryGenerator(result *BatchResult) string {
	if result == nil {
		return `{"type":"ParallelResult","totalCount":0,"successCount":0,"failureCount":0,"startedCount":0,"completionReason":"ALL_COMPLETED","status":"SUCCEEDED"}`
	}
	summary := map[string]any{
		"type":             "ParallelResult",
		"totalCount":       result.TotalCount(),
		"successCount":     result.SuccessCount(),
		"failureCount":     result.FailureCount(),
		"startedCount":     result.StartedCount(),
		"completionReason": result.CompletionReason,
		"status":           result.Status(),
	}
	b, _ := json.Marshal(summary)
	return string(b)
}

func (c *DurableContext) Map(
	ctx context.Context,
	name string,
	items []any,
	mapFunc MapFunc,
	cfg *MapConfig,
) *Future[*BatchResult] {
	ValidateContextUsage(ctx, c.stepPrefix, "map", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[*BatchResult] {
		if items == nil {
			return NewRejectedFuture[*BatchResult](errors.New("map operation requires an array of items"))
		}
		if mapFunc == nil {
			return NewRejectedFuture[*BatchResult](errors.New("map operation requires a function to process items"))
		}

		executionItems := make([]ConcurrentExecutionItem, 0, len(items))
		for i, item := range items {
			nameForItem := ""
			if cfg != nil && cfg.ItemNamer != nil {
				nameForItem = cfg.ItemNamer(item, i)
			}
			executionItems = append(executionItems, ConcurrentExecutionItem{
				ID:    fmt.Sprintf("map-item-%d", i),
				Data:  item,
				Index: i,
				Name:  nameForItem,
			})
		}

		conf := &ConcurrencyConfig{
			TopLevelSubType:  OperationSubTypeMap,
			IterationSubType: OperationSubTypeMapIteration,
			SummaryGenerator: mapSummaryGenerator,
			Serdes:           BatchResultSerdes{},
		}
		if cfg != nil {
			conf.MaxConcurrency = cfg.MaxConcurrency
			conf.ItemSerdes = cfg.ItemSerdes
			conf.CompletionConfig = cfg.CompletionConfig
			if cfg.Serdes != nil {
				conf.Serdes = cfg.Serdes
			}
		}

		executor := func(item ConcurrentExecutionItem, childContext *DurableContext) (any, error) {
			return mapFunc(childContext, item.Data, item.Index, items)
		}
		return c.executeConcurrentlyInternal(ctx, name, executionItems, executor, conf)
	})
}

func (c *DurableContext) Parallel(
	ctx context.Context,
	name string,
	branches []NamedParallelBranch,
	cfg *ParallelConfig,
) *Future[*BatchResult] {
	ValidateContextUsage(ctx, c.stepPrefix, "parallel", c.execCtx.TerminationManager())
	return withModeManagement(c, func() *Future[*BatchResult] {
		if branches == nil {
			return NewRejectedFuture[*BatchResult](errors.New("parallel operation requires an array of branch functions"))
		}
		executionItems := make([]ConcurrentExecutionItem, 0, len(branches))
		for i, branch := range branches {
			if branch.Func == nil {
				return NewRejectedFuture[*BatchResult](errors.New("all branches must define a function"))
			}
			executionItems = append(executionItems, ConcurrentExecutionItem{
				ID:    fmt.Sprintf("parallel-branch-%d", i),
				Data:  branch.Func,
				Index: i,
				Name:  branch.Name,
			})
		}

		conf := &ConcurrencyConfig{
			TopLevelSubType:  OperationSubTypeParallel,
			IterationSubType: OperationSubTypeParallelBranch,
			SummaryGenerator: parallelSummaryGenerator,
			Serdes:           BatchResultSerdes{},
		}
		if cfg != nil {
			conf.MaxConcurrency = cfg.MaxConcurrency
			conf.ItemSerdes = cfg.ItemSerdes
			conf.CompletionConfig = cfg.CompletionConfig
			if cfg.Serdes != nil {
				conf.Serdes = cfg.Serdes
			}
		}

		executor := func(item ConcurrentExecutionItem, childContext *DurableContext) (any, error) {
			fn, ok := item.Data.(ParallelFunc)
			if !ok || fn == nil {
				return nil, errors.New("parallel branch is not executable")
			}
			return fn(childContext)
		}
		return c.executeConcurrentlyInternal(ctx, name, executionItems, executor, conf)
	})
}
