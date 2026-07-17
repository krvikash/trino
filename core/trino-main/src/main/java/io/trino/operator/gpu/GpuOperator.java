/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.gpu;

import ai.rapids.cudf.Cuda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.annotation.NotThreadSafe;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuTaskMemoryContext;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Operator that executes a pipeline of GPU operations.
 * <p>
 * The operator maintains a chain of GpuOperations and uses a pull-based
 * execution model, however the operations are separated with a {@link PullCircuitBreaker}.
 * The driver runs each operation directly from {@link #getOutput()},
 * so call stacks stay flat (one operation per frame, not nested).
 */
public abstract class GpuOperator
        implements Operator
{
    public static final String GPU_OPERATOR_METRIC = "GPU Operator";
    // Plan-node IDs of operations fused into this GpuOperator are published as metric keys
    // under this prefix, so they show up in EXPLAIN ANALYZE alongside the primary plan node.
    public static final String FUSED_PLAN_NODE_METRIC_PREFIX = "GPU fused plan node: ";

    public abstract static class BaseFactory
            implements OperatorFactory
    {
        protected final int operatorId;
        protected final PlanNodeId planNodeId;
        // Plan nodes whose operations have been fused into this operator above the primary.
        protected final List<PlanNodeId> fusedPlanNodeIds;
        protected final Function<GpuOperation.Context, GpuOperatorSource> sourceFactory;
        protected final List<GpuOperation.Factory> operations;
        protected final List<Type> outputTypes;

        protected boolean closed;

        private BaseFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<PlanNodeId> fusedPlanNodeIds,
                Function<GpuOperation.Context, GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.fusedPlanNodeIds = ImmutableList.copyOf(requireNonNull(fusedPlanNodeIds, "fusedPlanNodeIds is null"));
            this.sourceFactory = requireNonNull(sourceFactory, "sourceFactory is null");
            this.operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        public abstract BaseFactory withAdditionalOperations(List<PlanNodeId> additionalFusedPlanNodeIds, List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes);

        @Override
        public void noMoreOperators()
        {
            closed = true;
            for (GpuOperation.Factory factory : operations) {
                factory.noMoreOperators();
            }
        }

        protected Metrics initialMetrics()
        {
            ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.builder();
            metrics.put(GPU_OPERATOR_METRIC, new LongCount(1));
            // Deduplicate defensively
            for (PlanNodeId fused : ImmutableSet.copyOf(fusedPlanNodeIds)) {
                metrics.put(FUSED_PLAN_NODE_METRIC_PREFIX + fused, new LongCount(1));
            }
            return new Metrics(metrics.buildOrThrow());
        }
    }

    public static class SourceFactory
            extends BaseFactory
            implements SourceOperatorFactory
    {
        public SourceFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                Session session,
                TableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                List<Type> columnTypes)
        {
            this(operatorId,
                    planNodeId,
                    ImmutableList.of(),
                    context -> {
                        GpuTableScan tableScan = new GpuTableScan(
                                context,
                                pageSourceProvider,
                                session,
                                table,
                                tableCredentials,
                                columns,
                                columnTypes,
                                dynamicFilter);
                        return new GpuOperatorSource(tableScan, tableScan);
                    },
                    ImmutableList.of(),
                    columnTypes);
        }

        private SourceFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<PlanNodeId> fusedPlanNodeIds,
                Function<GpuOperation.Context, GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            super(operatorId, planNodeId, fusedPlanNodeIds, sourceFactory, operations, outputTypes);
        }

        @Override
        public BaseFactory withAdditionalOperations(List<PlanNodeId> additionalFusedPlanNodeIds, List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes)
        {
            // TODO: The new operations may require additional columns on GPU that were not needed by existing operations.
            //  When we add support for selective column copying (copying only columns needed by GPU operations),
            //  we'll need to update the set of columns copied to GPU here.
            //  https://starburstdata.atlassian.net/browse/ENG-9808
            List<GpuOperation.Factory> newOperations = ImmutableList.<GpuOperation.Factory>builder()
                    .addAll(operations)
                    .addAll(additionalOperations)
                    .build();
            List<PlanNodeId> newFusedPlanNodeIds = ImmutableList.<PlanNodeId>builder()
                    .addAll(fusedPlanNodeIds)
                    .addAll(additionalFusedPlanNodeIds)
                    .build();
            return new SourceFactory(operatorId, planNodeId, newFusedPlanNodeIds, sourceFactory, newOperations, newOutputTypes);
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, GpuOperator.class.getSimpleName());
            GpuOperation.Context context = new OperationContext(operatorContext.getDriverContext().getPipelineContext().getTaskContext().getGpuTaskMemoryContext(), operatorContext);
            GpuOperatorSource operatorSource = sourceFactory.apply(context);
            GpuSourceOperation source = operatorSource.sourceOperation();
            GpuOperation head = operatorSource.sourceOutput();
            RefillSignal refillSignal = new RefillSignal();
            for (GpuOperation.Factory factory : this.operations) {
                head = new PullCircuitBreaker(head, refillSignal);
                head = factory.create(context, head);
            }
            head = new CopyToBlocks(context, head, outputTypes);
            operatorContext.setLatestMetrics(initialMetrics());
            return new GpuSourceOperator(planNodeId, operatorContext, head, source, refillSignal);
        }
    }

    public static class Factory
            extends BaseFactory
    {
        public Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, List<GpuOperation.Factory> operations, List<Type> outputTypes)
        {
            this(operatorId,
                    planNodeId,
                    ImmutableList.of(),
                    context -> {
                        BufferPages sourceOperation = new BufferPages(context.taskMemoryContext());
                        if (inputTypes.isEmpty()) {
                            // Skip CopyToDevice when there are no columns (e.g., projection with only COUNT(*))
                            return new GpuOperatorSource(sourceOperation, sourceOperation);
                        }
                        CopyToDevice copyToDevice = new CopyToDevice(
                                context,
                                sourceOperation,
                                inputTypes,
                                // TODO (https://starburstdata.atlassian.net/browse/ENG-9808) copy to device only necessary columns
                                IntStream.range(0, inputTypes.size()).boxed().collect(toImmutableSet()));
                        return new GpuOperatorSource(sourceOperation, copyToDevice);
                    },
                    operations,
                    outputTypes);
        }

        public Factory(int operatorId, PlanNodeId planNodeId, GpuSourceOperation.Factory sourceFactory, List<Type> outputTypes)
        {
            this(operatorId,
                    planNodeId,
                    ImmutableList.of(),
                    _ -> {
                        GpuSourceOperation source = sourceFactory.create();
                        return new GpuOperatorSource(source, source);
                    },
                    ImmutableList.of(),
                    outputTypes);
        }

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                List<PlanNodeId> fusedPlanNodeIds,
                Function<GpuOperation.Context, GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            super(operatorId, planNodeId, fusedPlanNodeIds, sourceFactory, operations, outputTypes);
        }

        @Override
        public BaseFactory withAdditionalOperations(List<PlanNodeId> additionalFusedPlanNodeIds, List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes)
        {
            // TODO: The new operations may require additional columns on GPU that were not needed by existing operations.
            //  When we add support for selective column copying (copying only columns needed by GPU operations),
            //  we'll need to update the set of columns copied to GPU here.
            //  https://starburstdata.atlassian.net/browse/ENG-9808
            List<GpuOperation.Factory> newOperations = ImmutableList.<GpuOperation.Factory>builder()
                    .addAll(operations)
                    .addAll(additionalOperations)
                    .build();
            List<PlanNodeId> newFusedPlanNodeIds = ImmutableList.<PlanNodeId>builder()
                    .addAll(fusedPlanNodeIds)
                    .addAll(additionalFusedPlanNodeIds)
                    .build();
            return new Factory(operatorId, planNodeId, newFusedPlanNodeIds, sourceFactory, newOperations, newOutputTypes);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, GpuOperator.class.getSimpleName());
            GpuOperation.Context context = new OperationContext(operatorContext.getDriverContext().getPipelineContext().getTaskContext().getGpuTaskMemoryContext(), operatorContext);
            GpuOperatorSource operatorSource = sourceFactory.apply(context);
            GpuSourceOperation source = operatorSource.sourceOperation();
            GpuOperation head = operatorSource.sourceOutput();
            RefillSignal refillSignal = new RefillSignal();
            for (GpuOperation.Factory factory : this.operations) {
                head = new PullCircuitBreaker(head, refillSignal);
                head = factory.create(context, head);
            }
            head = new CopyToBlocks(context, head, outputTypes);
            operatorContext.setLatestMetrics(initialMetrics());
            return new GpuIntermediateOperator(operatorContext, head, source, refillSignal);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(
                    operatorId,
                    planNodeId,
                    fusedPlanNodeIds,
                    sourceFactory,
                    operations.stream()
                            .map(GpuOperation.Factory::duplicate)
                            .collect(toImmutableList()),
                    outputTypes);
        }
    }

    private record GpuOperatorSource(@Borrow GpuSourceOperation sourceOperation, @Own GpuOperation sourceOutput)
    {
        GpuOperatorSource
        {
            requireNonNull(sourceOperation, "sourceOperation is null");
            requireNonNull(sourceOutput, "sourceOutput is null");
        }
    }

    private final OperatorContext operatorContext;
    private final @Own GpuOperation topOperation;
    private final RefillSignal refillSignal;

    private boolean finished;
    private ListenableFuture<Void> blocked = NOT_BLOCKED;
    private final GpuPageToPages gpuPageToPages = new GpuPageToPages();

    private GpuOperator(
            OperatorContext operatorContext,
            @Move GpuOperation topOperation,
            RefillSignal refillSignal)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.topOperation = requireNonNull(topOperation, "topOperation is null");
        this.refillSignal = requireNonNull(refillSignal, "refillSignal is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        throw new UnsupportedOperationException("Unsupported in " + getClass());
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException("Unsupported in " + getClass());
    }

    @Override
    public void finish()
    {
        throw new UnsupportedOperationException("Unsupported in " + getClass());
    }

    @Override
    public @Nullable Page getOutput()
    {
        if (finished) {
            return null;
        }

        checkState(blocked.isDone(), "Blocked");

        Optional<Page> ready = gpuPageToPages.poll();
        if (ready.isPresent()) {
            return ready.get();
        }

        refillSignal.clear();
        List<@Borrow PullCircuitBreaker> pending = new ArrayList<>(); // stack
        @Own GpuOperation.Result topGpuOperationResult;
        GpuOomHandler.setContext(operatorContext.getDriverContext().getTaskId().queryId());
        try {
            while (true) {
                topGpuOperationResult = topOperation.execute();
                if (!(topGpuOperationResult instanceof Yielded()) || !refillSignal.isSet()) {
                    break;
                }
                verify(pending.isEmpty(), "pending not empty: %s", pending);
                pending.addLast(refillSignal.clear());
                while (!pending.isEmpty()) {
                    PullCircuitBreaker next = pending.removeLast();
                    // Intentionally calling next.source.execute() directly, so the call stack stays flat: every operation is pulled directly from getOutput
                    GpuOperation.Result refilled = next.source.execute();
                    next.set(refilled);
                    if (refillSignal.isSet()) {
                        if (refilled instanceof Yielded()) {
                            pending.addLast(next);
                        }
                        pending.addLast(refillSignal.clear());
                    }
                }
            }
        }
        catch (OutOfMemoryError e) {
            Optional<GpuOomHandler.FailureSnapshot> snapshot = GpuOomHandler.getLastFailureSnapshot();
            if (snapshot.isPresent()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, snapshot.get().toErrorMessage(), e);
            }
            throw e;
        }
        finally {
            GpuOomHandler.clearContext();
        }
        Page operatorResult = switch (topGpuOperationResult) {
            case Data(AllocatedMemory memory, GpuPage gpuPage) -> {
                try (memory; gpuPage) {
                    gpuPageToPages.add(gpuPage);
                }
                yield gpuPageToPages.poll().orElse(null);
            }
            case Blocked(ListenableFuture<Void> future) -> {
                blocked = future;
                yield null;
            }
            case Yielded() -> null;
            case Finished() -> {
                finished = true;
                yield null;
            }
        };
        // Next time this operator is called, it may be scheduled on a different Driver thread, unless
        // experimental.thread-per-driver-scheduler-enabled is set.
        // Therefore, returuning/yielding constitutes an implicit cross-thread communication boundary
        // for internal state stored within operations or in GpuPageToPages buffers.
        Cuda.DEFAULT_STREAM.sync();
        return operatorResult;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
    {
        topOperation.close();
    }

    public static class GpuSourceOperator
            extends GpuOperator
            implements SourceOperator
    {
        private final PlanNodeId planNodeId;
        private final GpuSourceOperation sourceOperation;
        private boolean splitSet;

        private GpuSourceOperator(PlanNodeId planNodeId, OperatorContext operatorContext, GpuOperation topOperation, GpuSourceOperation sourceOperation, RefillSignal refillSignal)
        {
            super(operatorContext, topOperation, refillSignal);
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceOperation = requireNonNull(sourceOperation, "sourceOperation is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public void addSplit(Split split)
        {
            checkState(!splitSet, "split already set, table scan source operators are expected to get exactly one split");
            splitSet = true;
            sourceOperation.setSplit(split);
        }

        @Override
        public void noMoreSplits()
        {
            checkState(splitSet, "split not set yet");
        }
    }

    private static class GpuIntermediateOperator
            extends GpuOperator
    {
        private final GpuSourceOperation sourceOperation;

        private GpuIntermediateOperator(OperatorContext operatorContext, @Move GpuOperation topOperation, @Borrow GpuSourceOperation sourceOperation, RefillSignal refillSignal)
        {
            super(operatorContext, topOperation, refillSignal);
            this.sourceOperation = requireNonNull(sourceOperation, "sourceOperation is null");
        }

        @Override
        public boolean needsInput()
        {
            return !isFinished() && sourceOperation.needsInput();
        }

        @Override
        public void addInput(Page page)
        {
            sourceOperation.addInput(page);
        }

        @Override
        public void finish()
        {
            sourceOperation.noMoreInput();
        }
    }

    @NotThreadSafe
    private static final class RefillSignal
    {
        private @Borrow @Nullable PullCircuitBreaker pending;

        void request(@Borrow PullCircuitBreaker pullCircuitBreaker)
        {
            pending = requireNonNull(pullCircuitBreaker, "pullBreaker is null");
        }

        boolean isSet()
        {
            return pending != null;
        }

        @Borrow
        @Nullable
        PullCircuitBreaker clear()
        {
            @Borrow PullCircuitBreaker pending = this.pending;
            this.pending = null;
            return pending;
        }
    }

    @NotThreadSafe
    private static final class PullCircuitBreaker
            implements GpuOperation
    {
        private final @Own GpuOperation source;
        private final RefillSignal refillSignal;
        private @Nullable Result bufferedResult;

        PullCircuitBreaker(GpuOperation source, RefillSignal refillSignal)
        {
            this.source = requireNonNull(source, "source is null");
            this.refillSignal = requireNonNull(refillSignal, "refillSignal is null");
        }

        @Override
        public @Move Result execute()
        {
            return switch (bufferedResult) {
                case null -> {
                    refillSignal.request(this);
                    yield new Yielded();
                }
                // Terminal
                case Finished() -> bufferedResult;
                default -> {
                    Result result = bufferedResult;
                    bufferedResult = null;
                    yield result;
                }
            };
        }

        public void set(@Move Result result)
        {
            requireNonNull(result, "result is null");
            checkState(
                    bufferedResult == null || bufferedResult instanceof Yielded(),
                    "bufferedResult already set to %s when setting to %s",
                    bufferedResult,
                    result);
            bufferedResult = result;
        }

        @Override
        public void close()
        {
            try (var closer = UncheckedCloser.create()) {
                closer.register(source);
                switch (bufferedResult) {
                    case null -> {
                        // nothing to close
                    }
                    case Blocked _, Yielded _, Finished _ -> {
                        // nothing to close
                    }
                    case Data(AllocatedMemory memory, GpuPage page) -> {
                        closer.register(memory);
                        closer.register(page);
                    }
                }
                bufferedResult = null;
            }
        }
    }

    private record OperationContext(GpuTaskMemoryContext taskMemoryContext, OperatorContext operatorContext)
            implements GpuOperation.Context
    {
        OperationContext
        {
            requireNonNull(taskMemoryContext, "taskMemoryContext is null");
            requireNonNull(operatorContext, "operatorContext is null");
        }
    }
}
