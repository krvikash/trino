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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class CompositeIcebergPageSource
        implements ConnectorPageSource
{
    private final Iterator<IcebergSplit> subSplits;
    private final Function<IcebergSplit, ConnectorPageSource> pageSourceFactory;

    @Nullable
    private ConnectorPageSource currentPageSource;
    private boolean finished;
    private long completedBytes;
    private long readTimeNanos;
    private Metrics metrics = Metrics.EMPTY;

    public CompositeIcebergPageSource(List<IcebergSplit> subSplits, Function<IcebergSplit, ConnectorPageSource> pageSourceFactory)
    {
        this.subSplits = subSplits.iterator();
        this.pageSourceFactory = requireNonNull(pageSourceFactory, "pageSourceFactory is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (currentPageSource != null ? currentPageSource.getCompletedBytes() : 0);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + (currentPageSource != null ? currentPageSource.getReadTimeNanos() : 0);
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        while (true) {
            if (finished) {
                return null;
            }
            if (currentPageSource == null) {
                if (!subSplits.hasNext()) {
                    finished = true;
                    return null;
                }
                currentPageSource = pageSourceFactory.apply(subSplits.next());
            }
            SourcePage page = currentPageSource.getNextSourcePage();
            if (page != null) {
                return page;
            }
            if (!currentPageSource.isFinished()) {
                return null;
            }
            closeCurrentPageSource();
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return currentPageSource != null ? currentPageSource.getMemoryUsage() : 0;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (currentPageSource != null) {
            return currentPageSource.isBlocked();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void close()
            throws IOException
    {
        if (finished) {
            return;
        }
        finished = true;
        closeCurrentPageSource();
    }

    @Override
    public Metrics getMetrics()
    {
        return metrics.mergeWith(currentPageSource != null ? currentPageSource.getMetrics() : Metrics.EMPTY);
    }

    private void closeCurrentPageSource()
    {
        if (currentPageSource != null) {
            completedBytes += currentPageSource.getCompletedBytes();
            readTimeNanos += currentPageSource.getReadTimeNanos();
            metrics = metrics.mergeWith(currentPageSource.getMetrics());
            try {
                currentPageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            currentPageSource = null;
        }
    }
}
