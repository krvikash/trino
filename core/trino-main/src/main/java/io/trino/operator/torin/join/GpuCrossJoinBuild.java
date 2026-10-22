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
package io.trino.operator.torin.join;

import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.join.GpuCrossJoinBuildSideSupplier.GpuCrossJoinBuildSide;
import io.trino.operator.torin.memory.AllocatedMemory;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.plugin.base.torin.TablesList;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.MemoryAmount;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.torin.GpuUtils.toTable;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/// Build side of a cross join: buffers the output columns of every build row and publishes them
/// through a [GpuCrossJoinBuildSideSupplier].
public final class GpuCrossJoinBuild
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuCrossJoinBuildSideSupplier buildSideSupplier;
        private final int[] buildOutputChannels;

        public Factory(GpuCrossJoinBuildSideSupplier buildSideSupplier, int[] buildOutputChannels)
        {
            this.buildSideSupplier = requireNonNull(buildSideSupplier, "buildSideSupplier is null");
            this.buildOutputChannels = requireNonNull(buildOutputChannels, "buildOutputChannels is null").clone();
        }

        @Override
        public Factory duplicate()
        {
            throw new UnsupportedOperationException("Build side factory cannot be duplicated");
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuCrossJoinBuild(context, source, buildSideSupplier, buildOutputChannels);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final GpuCrossJoinBuildSideSupplier buildSideSupplier;
    private final int[] buildOutputChannels;

    // Never observed by the probe side
    private final @Own TablesList bufferedTables = TablesList.create();

    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<Table> buildOutputTable = ClosingRef.empty();

    private final AllocatedMemory allocated;

    private long buildRowCount;
    private long bufferedDeviceBytes;
    private boolean published;
    private final SettableFuture<Void> probesAllFinishedFuture = SettableFuture.create();

    private GpuCrossJoinBuild(Context context, GpuOperation source, GpuCrossJoinBuildSideSupplier buildSideSupplier, int[] buildOutputChannels)
    {
        this.source = requireNonNull(source, "source is null");
        this.buildSideSupplier = requireNonNull(buildSideSupplier, "buildSideSupplier is null");
        this.buildOutputChannels = buildOutputChannels;
        this.allocated = context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO);
    }

    @Override
    public @Move Result execute()
    {
        if (published) {
            if (!probesAllFinishedFuture.isDone()) {
                return new Blocked(probesAllFinishedFuture);
            }
            return new Finished();
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory memory, GpuPage page) -> {
                try (memory; page) {
                    bufferPage(memory, page);
                }
                yield new Yielded();
            }
            case Finished() -> {
                publishBuildSide();
                yield new Yielded();
            }
        };
    }

    private void bufferPage(@Borrow AllocatedMemory pageAllocation, @Borrow GpuPage page)
    {
        if (page.positionCount() == 0) {
            return;
        }
        buildRowCount += page.positionCount();
        if (buildOutputChannels.length == 0) {
            return;
        }
        // Take over only what is retained. The page keeps the reservation for the channels outside
        // buildOutputChannels and releases it with itself, which it does only after this returns.
        long selectedBytes = 0;
        for (int channel : buildOutputChannels) {
            selectedBytes += page.column(channel).retainedDeviceMemoryBytes();
        }
        allocated.transferFrom(pageAllocation, MemoryAmount.gpuDevice(min(selectedBytes, pageAllocation.amount().gpuDeviceBytes())));
        bufferedTables.add(toTable(page, buildOutputChannels));
        bufferedDeviceBytes += selectedBytes;
        allocated.update(MemoryAmount.gpuDevice(bufferedDeviceBytes));
    }

    private void publishBuildSide()
    {
        checkState(!published, "already published");

        Optional<Table> outputTable = Optional.empty();
        if (buildRowCount > 0 && buildOutputChannels.length > 0) {
            allocated.update(allocated.amount().add(bufferedTables.concatenateMemoryRequirements()));
            buildOutputTable.set(bufferedTables.concatenateAndClear());
            allocated.update(MemoryAmount.gpuDevice(buildOutputTable.borrow().getDeviceMemorySize()));
            outputTable = Optional.of(buildOutputTable.borrow());
        }

        checkState(!probesAllFinishedFuture.isDone(), "probesAllFinishedFuture is already marked as done");
        published = true;
        // Note: if publish throws, it's unclear who owns the memory and it may leak.
        buildSideSupplier.publishBuildSide(new GpuCrossJoinBuildSide(buildRowCount, outputTable), this::allProbesFinished);
    }

    private void allProbesFinished()
    {
        checkState(published, "probes could not finish without publishing");
        try {
            // From the moment of publishing, these resources are owned by the probe side.
            releaseSharedResources();
        }
        finally {
            // Let the operator complete after the memory is released.
            probesAllFinishedFuture.set(null);
        }
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            if (!published) {
                probesAllFinishedFuture.setException(new IllegalStateException("Closed before publishing"));
                // Registered first, so the reservation is given up only after the buffers it covers are freed
                closer.register(this::releaseSharedResources);
            }
            closer.register(source);
            // if there is anything in bufferedTables, it hasn't been exposed to probe side yet
            closer.register(bufferedTables);
        }
    }

    private void releaseSharedResources()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(allocated); // release last
            closer.register(buildOutputTable);
        }
    }
}
