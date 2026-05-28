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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.ast.AstExpression;
import ai.rapids.cudf.ast.CompiledExpression;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuJoinBridge.EmptyBuildSide;
import io.trino.operator.gpu.join.GpuJoinBridge.FilteredHashJoinBridge;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.TablesList;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class GpuJoinBuild
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuJoinBridgeManager bridgeManager;
        private final int[] buildKeyChannels;
        private final int[] buildOutputChannels;
        private final Optional<AstExpression> filter;
        private final Optional<GpuDynamicFilterCollector> dynamicFilter;

        public Factory(
                GpuJoinBridgeManager bridgeManager,
                int[] buildKeyChannels,
                int[] buildOutputChannels,
                Optional<AstExpression> filter,
                Optional<GpuDynamicFilterCollector> dynamicFilter)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            this.buildKeyChannels = requireNonNull(buildKeyChannels, "buildKeyChannels is null").clone();
            checkArgument(this.buildKeyChannels.length > 0, "buildKeyChannels must not be empty");
            this.buildOutputChannels = requireNonNull(buildOutputChannels, "buildOutputChannels is null").clone();
            this.filter = requireNonNull(filter, "filter is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        }

        @Override
        public Factory duplicate()
        {
            throw new UnsupportedOperationException("Build side factory cannot be duplicated");
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuJoinBuild(source, bridgeManager, buildKeyChannels, buildOutputChannels, filter, dynamicFilter);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final GpuJoinBridgeManager bridgeManager;
    private final int[] buildKeyChannels;
    private final int[] buildOutputChannels;
    private final Optional<AstExpression> filter;
    private final Optional<GpuDynamicFilterCollector> dynamicFilter;

    // Never observed by the probe side
    private final @Own TablesList bufferedTables = TablesList.create();

    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<Table> buildSourceTable = ClosingRef.empty();
    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<Table> buildKeyTable = ClosingRef.empty();
    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<HashJoin> hashJoin = ClosingRef.empty();
    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<CompiledExpression> compiledFilter = ClosingRef.empty();
    // From the moment of publish, this is owned by the probe side
    private final ClosingRef<Table> buildOutputTable = ClosingRef.empty();

    private boolean published;
    private final SettableFuture<Void> probesAllFinishedFuture = SettableFuture.create();

    private GpuJoinBuild(
            GpuOperation source,
            GpuJoinBridgeManager bridgeManager,
            int[] buildKeyChannels,
            int[] buildOutputChannels,
            Optional<AstExpression> filter,
            Optional<GpuDynamicFilterCollector> dynamicFilter)
    {
        this.source = requireNonNull(source, "source is null");
        this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
        this.buildKeyChannels = buildKeyChannels;
        this.buildOutputChannels = buildOutputChannels;
        this.filter = requireNonNull(filter, "filter is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
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
            case Data(GpuPage page) -> {
                try (page) {
                    bufferPage(page);
                }
                yield new Yielded();
            }
            case Finished() -> {
                publishBuild();
                yield new Yielded();
            }
        };
    }

    private void bufferPage(@Borrow GpuPage page)
    {
        int columnCount = page.columnCount();
        if (page.positionCount() == 0) {
            return;
        }
        @Borrow ColumnVector[] tableColumns = new ColumnVector[columnCount];
        for (int channel = 0; channel < columnCount; channel++) {
            // Table constructor calls incRefCount(), so columns outlive the page being closed.
            tableColumns[channel] = ((DeviceMemory) page.column(channel)).columnVector();
        }
        bufferedTables.add(new Table(tableColumns));
    }

    private void publishBuild()
    {
        checkState(!published, "already published");

        GpuJoinBridge bridge;
        if (bufferedTables.isEmpty()) {
            // build side empty
            dynamicFilter.ifPresent(GpuDynamicFilterCollector::collectEmpty);
            bridge = new EmptyBuildSide();
        }
        else {
            buildSourceTable.set(bufferedTables.concatenateAndClear());
            dynamicFilter.ifPresent(filter -> filter.collect(buildSourceTable.borrow()));

            buildKeyTable.set(selectColumns(buildSourceTable.borrow(), buildKeyChannels));

            // Join output data from the build side
            @Nullable @Borrow Table buildOutputTable;
            if (buildOutputChannels.length > 0) {
                this.buildOutputTable.set(selectColumns(buildSourceTable.borrow(), buildOutputChannels));
                buildOutputTable = this.buildOutputTable.borrow();
            }
            else {
                buildOutputTable = null;
            }

            if (filter.isEmpty()) {
                buildSourceTable.close();
                hashJoin.set(new HashJoin(buildKeyTable.borrow(), /*compareNullsEqual=*/ false));
                buildKeyTable.close();
                bridge = new GpuJoinBridge.HashJoinBridge(
                        hashJoin.borrow(),
                        buildOutputTable);
            }
            else {
                compiledFilter.set(filter.get().compile());
                bridge = new FilteredHashJoinBridge(
                        buildSourceTable.borrow(),
                        buildKeyTable.borrow(),
                        compiledFilter.borrow(),
                        buildOutputTable);
            }
        }

        checkState(!probesAllFinishedFuture.isDone(), "probesAllFinishedFuture is already marked as done");
        published = true;
        // Note: if publish throws, it's unclear who owns the memory and it may leak.
        bridgeManager.publishBridge(bridge, this::allProbesFinished);
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
            closer.register(source);
            // if there is anything in bufferedTables, it hasn't been exposed to probe side yet
            closer.register(bufferedTables);

            // If publish wasn't reached, we still own the resources and need to close them.
            if (!published) {
                probesAllFinishedFuture.setException(new Exception("Closed before publishing"));
                releaseSharedResources();
            }
        }
    }

    private void releaseSharedResources()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(buildSourceTable);
            closer.register(buildKeyTable);
            closer.register(hashJoin);
            closer.register(compiledFilter);
            closer.register(buildOutputTable);
        }
    }

    private static @Own Table selectColumns(@Borrow Table source, int[] channels)
    {
        ColumnVector[] selected = new ColumnVector[channels.length];
        for (int i = 0; i < channels.length; i++) {
            selected[i] = source.getColumn(channels[i]);
        }
        return new Table(selected);
    }
}
