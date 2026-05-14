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
import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuSemiJoinSetSupplier.GpuSemiJoinSet;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.operator.gpu.GpuUtils.concatenateAndClose;
import static java.util.Objects.requireNonNull;

public final class GpuSemiJoinBuild
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuSemiJoinSetSupplier setSupplier;
        private final int buildKeyChannel;

        public Factory(GpuSemiJoinSetSupplier setSupplier, int buildKeyChannel)
        {
            this.setSupplier = requireNonNull(setSupplier, "setSupplier is null");
            this.buildKeyChannel = buildKeyChannel;
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuSemiJoinBuild(source, setSupplier, buildKeyChannel);
        }
    }

    private final GpuOperation source;
    private final GpuSemiJoinSetSupplier setSupplier;
    private final int buildKeyChannel;

    private final List<@Own Table> bufferedTables = new ArrayList<>();
    private final ClosingRef<Table> buildKeyTable = ClosingRef.empty();
    private boolean published;
    private final SettableFuture<Void> probesAllFinishedFuture = SettableFuture.create();

    private GpuSemiJoinBuild(GpuOperation source, GpuSemiJoinSetSupplier setSupplier, int buildKeyChannel)
    {
        this.source = requireNonNull(source, "source is null");
        this.setSupplier = requireNonNull(setSupplier, "setSupplier is null");
        this.buildKeyChannel = buildKeyChannel;
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
        if (page.positionCount() == 0) {
            return;
        }
        @Borrow ColumnVector buildKeyColumn = ((DeviceMemory) page.column(buildKeyChannel)).columnVector();
        bufferedTables.add(new Table(buildKeyColumn));
    }

    private void publishBuild()
    {
        checkState(!published, "already published");
        published = true;

        if (bufferedTables.isEmpty()) {
            setSupplier.publishSet(new GpuSemiJoinSet(Optional.empty(), false), this::allProbesFinished);
            return;
        }

        buildKeyTable.set(concatenateAndClose(bufferedTables));
        bufferedTables.clear();

        boolean buildHasNull = buildKeyTable.borrow().getColumn(0).hasNulls();
        setSupplier.publishSet(
                new GpuSemiJoinSet(Optional.of(buildKeyTable.borrow()), buildHasNull),
                this::allProbesFinished);
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
            closer.register(buildKeyTable);
            probesAllFinishedFuture.setException(new Exception("Closed"));
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
