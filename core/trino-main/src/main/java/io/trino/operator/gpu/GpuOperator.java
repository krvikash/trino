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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.borrow.Own;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
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
public class GpuOperator
        implements Operator
{
    public static class Factory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> inputTypes;
        private final List<GpuOperation.Factory> operations;
        private final List<Type> outputTypes;
        private boolean closed;

        public Factory(int operatorId, PlanNodeId planNodeId, List<Type> inputTypes, List<GpuOperation.Factory> operations, List<Type> outputTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes, "inputTypes is null"));
            this.operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Already closed");
            BufferPages sourceOperation = new BufferPages();
            GpuOperation operation = sourceOperation;
            operation = new CopyToDevice(
                    operation,
                    inputTypes,
                    // TODO (https://starburstdata.atlassian.net/browse/ENG-9808) copy to device only necessary columns
                    IntStream.range(0, inputTypes.size()).boxed().collect(toImmutableSet()));
            for (GpuOperation.Factory factory : this.operations) {
                operation = factory.create(operation);
            }
            operation = new CopyToBlocks(operation, outputTypes);
            return new GpuOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId, GpuOperator.class.getSimpleName()),
                    operation,
                    sourceOperation);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(
                    operatorId,
                    planNodeId,
                    inputTypes,
                    operations, // TODO duplicate?
                    outputTypes);
        }
    }

    private final OperatorContext operatorContext;
    private final GpuOperation topOperation;
    private final GpuSourceOperation sourceOperation;

    private boolean finished;
    private ListenableFuture<Void> blocked = NOT_BLOCKED;
    private final GpuPageToPages gpuPageToPages = new GpuPageToPages();

    public GpuOperator(
            OperatorContext operatorContext,
            GpuOperation topOperation,
            GpuSourceOperation sourceOperation)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.topOperation = requireNonNull(topOperation, "topOperation is null");
        this.sourceOperation = requireNonNull(sourceOperation, "sourceOperation is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && sourceOperation.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        sourceOperation.addInput(page);
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
    public void finish()
    {
        sourceOperation.noMoreInput();
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
        // should not be needed
        sourceOperation.close();
    }
}
