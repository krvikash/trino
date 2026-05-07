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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
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
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Operator that executes a pipeline of GPU operations.
 * <p>
 * The operator maintains a chain of GpuOperations and uses a pull-based
 * execution model. When getOutput() is called, it pulls from the top
 * operation, which recursively pulls from its source, eventually reaching
 * the source operation that batches input Pages.
 */
public abstract class GpuOperator
        implements Operator
{
    public abstract static class BaseFactory
            implements OperatorFactory
    {
        protected final int operatorId;
        protected final PlanNodeId planNodeId;
        protected final Supplier<GpuOperatorSource> sourceFactory;
        protected final List<GpuOperation.Factory> operations;
        protected final List<Type> outputTypes;

        protected boolean closed;

        private BaseFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceFactory = requireNonNull(sourceFactory, "sourceFactory is null");
            this.operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        public abstract BaseFactory withAdditionalOperations(List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes);

        @Override
        public void noMoreOperators()
        {
            closed = true;
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
            this(
                    operatorId,
                    planNodeId,
                    () -> {
                        GpuTableScan tableScan = new GpuTableScan(
                                pageSourceProvider,
                                session,
                                table,
                                tableCredentials,
                                columns,
                                dynamicFilter);
                        return new GpuOperatorSource(tableScan, tableScan);
                    },
                    ImmutableList.of(),
                    columnTypes);
        }

        private SourceFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            super(operatorId, planNodeId, sourceFactory, operations, outputTypes);
        }

        @Override
        public BaseFactory withAdditionalOperations(List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes)
        {
            // TODO: The new operations may require additional columns on GPU that were not needed by existing operations.
            //  When we add support for selective column copying (copying only columns needed by GPU operations),
            //  we'll need to update the set of columns copied to GPU here.
            //  https://starburstdata.atlassian.net/browse/ENG-9808
            List<GpuOperation.Factory> newOperations = ImmutableList.<GpuOperation.Factory>builder()
                    .addAll(operations)
                    .addAll(additionalOperations)
                    .build();
            return new SourceFactory(operatorId, planNodeId, sourceFactory, newOperations, newOutputTypes);
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
            GpuOperatorSource operatorSource = sourceFactory.get();
            GpuSourceOperation source = operatorSource.sourceOperation();
            GpuOperation head = operatorSource.sourceOutput();
            for (GpuOperation.Factory factory : this.operations) {
                head = factory.create(head);
            }
            head = new CopyToBlocks(head, outputTypes);
            OperatorContext operatorContext1 = driverContext.addOperatorContext(operatorId, planNodeId, GpuOperator.class.getSimpleName());
            return new GpuSourceOperator(planNodeId, operatorContext1, head, source);
        }
    }

    public static class Factory
            extends BaseFactory
    {
        public Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, List<GpuOperation.Factory> operations, List<Type> outputTypes)
        {
            this(
                    operatorId,
                    planNodeId,
                    () -> {
                        BufferPages sourceOperation = new BufferPages();
                        if (inputTypes.isEmpty()) {
                            // Skip CopyToDevice when there are no columns (e.g., projection with only COUNT(*))
                            return new GpuOperatorSource(sourceOperation, sourceOperation);
                        }
                        CopyToDevice copyToDevice = new CopyToDevice(
                                sourceOperation,
                                inputTypes,
                                // TODO (https://starburstdata.atlassian.net/browse/ENG-9808) copy to device only necessary columns
                                IntStream.range(0, inputTypes.size()).boxed().collect(toImmutableSet()));
                        return new GpuOperatorSource(sourceOperation, copyToDevice);
                    },
                    operations,
                    outputTypes);
        }

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<GpuOperatorSource> sourceFactory,
                List<GpuOperation.Factory> operations,
                List<Type> outputTypes)
        {
            super(operatorId, planNodeId, sourceFactory, operations, outputTypes);
        }

        @Override
        public BaseFactory withAdditionalOperations(List<GpuOperation.Factory> additionalOperations, List<Type> newOutputTypes)
        {
            // TODO: The new operations may require additional columns on GPU that were not needed by existing operations.
            //  When we add support for selective column copying (copying only columns needed by GPU operations),
            //  we'll need to update the set of columns copied to GPU here.
            //  https://starburstdata.atlassian.net/browse/ENG-9808
            List<GpuOperation.Factory> newOperations = ImmutableList.<GpuOperation.Factory>builder()
                    .addAll(operations)
                    .addAll(additionalOperations)
                    .build();
            return new Factory(operatorId, planNodeId, sourceFactory, newOperations, newOutputTypes);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Already closed");
            GpuOperatorSource operatorSource = sourceFactory.get();
            GpuSourceOperation source = operatorSource.sourceOperation();
            GpuOperation head = operatorSource.sourceOutput();
            for (GpuOperation.Factory factory : this.operations) {
                head = factory.create(head);
            }
            head = new CopyToBlocks(head, outputTypes);
            OperatorContext operatorContext1 = driverContext.addOperatorContext(operatorId, planNodeId, GpuOperator.class.getSimpleName());
            return new GpuIntermediateOperator(operatorContext1, head, source);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(
                    operatorId,
                    planNodeId,
                    sourceFactory, // TODO duplicate?
                    operations, // TODO duplicate?
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

    private boolean finished;
    private ListenableFuture<Void> blocked = NOT_BLOCKED;
    private final GpuPageToPages gpuPageToPages = new GpuPageToPages();

    public GpuOperator(
            OperatorContext operatorContext,
            @Move GpuOperation topOperation)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.topOperation = requireNonNull(topOperation, "topOperation is null");
        operatorContext.setLatestMetrics(new Metrics(ImmutableMap.of("GPU Operator", new LongCount(1))));
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

        @Own GpuOperation.Result result = topOperation.execute();
        return switch (result) {
            case Data(GpuPage gpuPage) -> {
                try (gpuPage) {
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

    static class GpuSourceOperator
            extends GpuOperator
            implements SourceOperator
    {
        private final PlanNodeId planNodeId;
        private final GpuSourceOperation sourceOperation;
        private boolean splitSet;

        public GpuSourceOperator(PlanNodeId planNodeId, OperatorContext operatorContext, GpuOperation topOperation, GpuSourceOperation sourceOperation)
        {
            super(operatorContext, topOperation);
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

    static class GpuIntermediateOperator
            extends GpuOperator
    {
        private final GpuSourceOperation sourceOperation;

        public GpuIntermediateOperator(OperatorContext operatorContext, @Move GpuOperation topOperation, @Borrow GpuSourceOperation sourceOperation)
        {
            super(operatorContext, topOperation);
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
}
