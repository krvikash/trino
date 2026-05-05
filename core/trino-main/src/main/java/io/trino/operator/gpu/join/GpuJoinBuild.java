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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;
import static java.util.Objects.requireNonNull;

/**
 * Sink GPU operation for the build side of a hash join. Buffers the cuDF {@link Table}s
 * produced from build-side pages and, when the upstream signals {@link Finished},
 * concatenates them, builds the cuDF {@link HashJoin}, and publishes the resulting
 * {@link GpuJoinBridge} via the shared {@link GpuJoinBridgeManager}.
 * <p>
 * After publishing, the operation stays {@link Blocked} on the bridge's free future and only
 * reports {@link Finished} once every probe operator has closed and the probe operator factory
 * has been closed. This mirrors the CPU {@code HashBuilderOperator} lifecycle, where the build
 * driver lives until the lookup source is no longer needed.
 * <p>
 * This operation never emits a {@link Data} result.
 */
public final class GpuJoinBuild
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuJoinBridgeManager bridgeManager;
        private final int[] buildKeyChannels;
        private final int[] buildOutputChannels;

        public Factory(GpuJoinBridgeManager bridgeManager, int[] buildKeyChannels, int[] buildOutputChannels)
        {
            this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
            this.buildKeyChannels = requireNonNull(buildKeyChannels, "buildKeyChannels is null").clone();
            checkArgument(this.buildKeyChannels.length > 0, "buildKeyChannels must not be empty");
            this.buildOutputChannels = requireNonNull(buildOutputChannels, "buildOutputChannels is null").clone();
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuJoinBuild(source, bridgeManager, buildKeyChannels, buildOutputChannels);
        }
    }

    private final GpuOperation source;
    private final GpuJoinBridgeManager bridgeManager;
    private final int[] buildKeyChannels;
    private final int[] buildOutputChannels;
    private final List<@Own Table> bufferedTables = new ArrayList<>();

    private boolean published;
    private @Nullable ListenableFuture<Void> probesAllFinishedFuture;

    private GpuJoinBuild(GpuOperation source, GpuJoinBridgeManager bridgeManager, int[] buildKeyChannels, int[] buildOutputChannels)
    {
        this.source = requireNonNull(source, "source is null");
        this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
        this.buildKeyChannels = buildKeyChannels;
        this.buildOutputChannels = buildOutputChannels;
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
        published = true;

        @Own Table buildTable = null;
        @Own Table buildKeyTable = null;
        @Own Table buildPayloadTable = null;
        @Own HashJoin hashJoin = null;
        try {
            if (!bufferedTables.isEmpty()) {
                buildTable = concatenateAndClose(bufferedTables);
                bufferedTables.clear();
                buildKeyTable = selectColumns(buildTable, buildKeyChannels);
                if (buildOutputChannels.length > 0) {
                    buildPayloadTable = selectColumns(buildTable, buildOutputChannels);
                }
                hashJoin = new HashJoin(buildKeyTable, /*compareNullsEqual=*/false);
                buildKeyTable.close();
                buildKeyTable = null;
            }
            // else: build received zero rows; bridge holds null HashJoin and probe handles it.

            // Bridge is created with refCount=1 (the seed held by the manager). publishBridge()
            // will acquire additional refs for registered probe operators before completing the future.
            GpuJoinBridge bridge = new GpuJoinBridge(buildPayloadTable, hashJoin, 1);
            hashJoin = null;
            buildPayloadTable = null;
            // Capture before publishing: once published, this build operator no longer owns the bridge,
            // but the free future is safe to observe — it fires when refCount reaches zero.
            // TODO (https://starburstdata.atlassian.net/browse/ENG-9840) memory accounting for the build side until probe side is finished
            probesAllFinishedFuture = bridge.getFreeFuture();
            bridgeManager.publishBridge(bridge);
        }
        catch (Throwable t) {
            closeQuietly(t, hashJoin);
            closeQuietly(t, buildPayloadTable);
            closeQuietly(t, buildKeyTable);
            throw t;
        }
        finally {
            if (buildTable != null) {
                buildTable.close();
            }
            for (Table table : bufferedTables) {
                if (table != null) {
                    table.close();
                }
            }
            bufferedTables.clear();
        }
    }

    @Override
    public void close()
    {
        try {
            source.close();
        }
        finally {
            // If publishBuild() was not called (cancellation or upstream failure), free buffered
            // tables. The bridge future stays pending; Trino's task-failure mechanism unblocks
            // probe operators.
            for (Table table : bufferedTables) {
                table.close();
            }
            bufferedTables.clear();
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

    private static void closeQuietly(Throwable cause, AutoCloseable resource)
    {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        }
        catch (Throwable closeEx) {
            if (closeEx != cause) {
                cause.addSuppressed(closeEx);
            }
        }
    }
}
