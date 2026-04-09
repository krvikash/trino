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
import com.google.common.util.concurrent.SettableFuture;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.split.EmptySplit;
import io.trino.split.PageSourceProvider;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class GpuTableScan
        implements GpuSourceOperation
{
    private final PageSourceProvider pageSourceProvider;
    private final Session session;
    private final TableHandle table;
    private final Optional<ConnectorTableCredentials> tableCredentials;
    private final List<ColumnHandle> columns;

    private @Nullable Split split;
    private final SettableFuture<Void> splitSet = SettableFuture.create();
    private @Nullable ConnectorGpuPageSource pageSource;

    public GpuTableScan(
            PageSourceProvider pageSourceProvider,
            Session session,
            TableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.session = requireNonNull(session, "session is null");
        this.table = requireNonNull(table, "table is null");
        this.tableCredentials = requireNonNull(tableCredentials, "tableCredentials is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @Override
    public boolean needsInput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreInput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSplit(Split split)
    {
        this.split = requireNonNull(split, "split is null");
        splitSet.set(null);
    }

    @Override
    public @Move Result execute()
    {
        if (split == null) {
            return new Blocked(splitSet);
        }

        if (split.getConnectorSplit() instanceof EmptySplit) {
            return new Finished();
        }

        if (pageSource == null) {
            pageSource = pageSourceProvider.createGpuPageSource(
                    session,
                    split,
                    table,
                    tableCredentials,
                    columns,
                    DynamicFilter.EMPTY);
        }

        @Own ConnectorGpuPageSource.Result result = pageSource.readNext();
        return switch (result) {
            case ConnectorGpuPageSource.Blocked(CompletableFuture<Void> future) -> new Blocked(future.isDone() ? NOT_BLOCKED : toListenableFuture(future));
            case ConnectorGpuPageSource.Data(GpuPage page) -> new Data(page);
            case ConnectorGpuPageSource.Finished() -> new Finished();
            case ConnectorGpuPageSource.Yielded() -> new Yielded();
        };
    }

    @Override
    public void close()
    {
        if (pageSource != null) {
            pageSource.close();
            pageSource = null;
        }
    }
}
