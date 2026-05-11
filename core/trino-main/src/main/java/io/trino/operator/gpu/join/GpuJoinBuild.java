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
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.util.AutoCloseableCloser;
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
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;
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
    private final ClosingRef<Table> buildOutputTable = ClosingRef.empty();
    private final ClosingRef<HashJoin> hashJoin = ClosingRef.empty();
    private boolean published;
    private final SettableFuture<Void> probesAllFinishedFuture = SettableFuture.create();

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

        if (bufferedTables.isEmpty()) {
            // build side empty
            bridgeManager.publishBridge(null, null, this::allProbesFinished);
        }
        else {
            try (ClosingRef<Table> buildTable = ClosingRef.empty();
                    ClosingRef<Table> buildKeyTable = ClosingRef.empty()) {
                buildTable.set(concatenateAndClose(bufferedTables));
                bufferedTables.clear();
                buildKeyTable.set(selectColumns(buildTable.borrow(), buildKeyChannels));
                @Nullable @Borrow Table buildOutputTable;
                if (buildOutputChannels.length > 0) {
                    this.buildOutputTable.set(selectColumns(buildTable.borrow(), buildOutputChannels));
                    buildOutputTable = this.buildOutputTable.borrow();
                }
                else {
                    buildOutputTable = null;
                }
                hashJoin.set(new HashJoin(buildKeyTable.borrow(), /*compareNullsEqual=*/false));
                buildKeyTable.close();

                bridgeManager.publishBridge(hashJoin.borrow(), buildOutputTable, this::allProbesFinished);
            }
        }
    }

    void allProbesFinished()
    {
        probesAllFinishedFuture.set(null);
        close();
    }

    @Override
    public synchronized void close()
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            closer.register(source);
            bufferedTables.forEach(closer::register);
            bufferedTables.clear();
            closer.register(buildOutputTable);
            closer.register(hashJoin);
            probesAllFinishedFuture.setException(new Exception("Closed"));
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
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
