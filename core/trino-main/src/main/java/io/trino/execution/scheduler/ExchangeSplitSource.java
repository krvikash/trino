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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.connector.CatalogHandle;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.metadata.Split;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;
import io.trino.spi.metrics.Metrics;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.util.concurrent.Futures.getDone;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class ExchangeSplitSource
        implements SplitSource
{
    private final List<ExchangeSourceHandleSource> handleSources;
    private final long targetSplitSizeInBytes;

    // null means idle; reads/writes serialized by the SplitSource single-caller contract
    private final List<ListenableFuture<ExchangeSourceHandleBatch>> pendingFutures;
    // one per source; forwards completion to the current anyCompleted future via a single listener
    private final List<SourceRelay> relays;
    private final boolean[] exhausted;

    public ExchangeSplitSource(ExchangeSourceHandleSource handleSource, long targetSplitSizeInBytes)
    {
        this(ImmutableList.of(handleSource), targetSplitSizeInBytes);
    }

    public ExchangeSplitSource(List<ExchangeSourceHandleSource> handleSources, long targetSplitSizeInBytes)
    {
        checkArgument(!requireNonNull(handleSources, "handleSources is null").isEmpty(), "handleSources is empty");
        this.handleSources = ImmutableList.copyOf(handleSources);
        this.targetSplitSizeInBytes = targetSplitSizeInBytes;
        this.pendingFutures = new ArrayList<>(nCopies(handleSources.size(), null));
        this.exhausted = new boolean[handleSources.size()];
        ImmutableList.Builder<SourceRelay> relaysBuilder = ImmutableList.builderWithExpectedSize(handleSources.size());
        for (int i = 0; i < handleSources.size(); i++) {
            relaysBuilder.add(new SourceRelay());
        }
        this.relays = relaysBuilder.build();
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return REMOTE_CATALOG_HANDLE;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        // maxSize is ignored; a call may drain all N completed sources into one batch.
        // Start all idle sources concurrently; return as soon as any one produces a batch.
        for (int i = 0; i < handleSources.size(); i++) {
            if (!exhausted[i] && pendingFutures.get(i) == null) {
                SourceRelay relay = relays.get(i);
                relay.reset();
                ListenableFuture<ExchangeSourceHandleBatch> future = toListenableFuture(handleSources.get(i).getNextBatch());
                pendingFutures.set(i, future);
                future.addListener(relay::onCompleted, directExecutor());
            }
        }

        boolean anyActive = false;
        for (int i = 0; i < handleSources.size(); i++) {
            if (!exhausted[i] && pendingFutures.get(i) != null) {
                anyActive = true;
                break;
            }
        }

        if (!anyActive) {
            return immediateFuture(new SplitBatch(ImmutableList.of(), true));
        }

        // setDelegate fires anyCompleted immediately if the relay already completed (no lost signals).
        SettableFuture<Void> anyCompleted = SettableFuture.create();
        for (int i = 0; i < handleSources.size(); i++) {
            if (!exhausted[i] && pendingFutures.get(i) != null) {
                relays.get(i).setDelegate(anyCompleted);
            }
        }

        return transformAsync(anyCompleted, _ -> collectAvailableSplits(), directExecutor());
    }

    private ListenableFuture<SplitBatch> collectAvailableSplits()
            throws ExecutionException
    {
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        boolean allDone = true;

        for (int i = 0; i < handleSources.size(); i++) {
            if (exhausted[i]) {
                continue;
            }
            ListenableFuture<ExchangeSourceHandleBatch> future = pendingFutures.get(i);
            if (future != null && future.isDone()) {
                ExchangeSourceHandleBatch batch = getDone(future); // throws ExecutionException on failure
                pendingFutures.set(i, null);
                if (batch.lastBatch()) {
                    exhausted[i] = true;
                }
                else {
                    allDone = false;
                }
                List<ExchangeSourceHandle> handles = batch.handles();
                ListMultimap<Integer, ExchangeSourceHandle> partitionToHandles = handles.stream()
                        .collect(toImmutableListMultimap(ExchangeSourceHandle::getPartitionId, identity()));
                for (int partition : partitionToHandles.keySet()) {
                    splits.addAll(createRemoteSplits(partitionToHandles.get(partition)));
                }
            }
            else {
                allDone = false;
            }
        }

        return immediateFuture(new SplitBatch(splits.build(), allDone));
    }

    private List<Split> createRemoteSplits(List<ExchangeSourceHandle> handles)
    {
        ImmutableList.Builder<Split> result = ImmutableList.builder();
        ImmutableList.Builder<ExchangeSourceHandle> currentSplitHandles = ImmutableList.builder();
        long currentSplitHandlesSize = 0;
        long currentSplitHandlesCount = 0;
        for (ExchangeSourceHandle handle : handles) {
            if (currentSplitHandlesCount > 0 && currentSplitHandlesSize + handle.getDataSizeInBytes() > targetSplitSizeInBytes) {
                result.add(createRemoteSplit(currentSplitHandles.build()));
                currentSplitHandles = ImmutableList.builder();
                currentSplitHandlesSize = 0;
                currentSplitHandlesCount = 0;
            }
            currentSplitHandles.add(handle);
            currentSplitHandlesSize += handle.getDataSizeInBytes();
            currentSplitHandlesCount++;
        }
        if (currentSplitHandlesCount > 0) {
            result.add(createRemoteSplit(currentSplitHandles.build()));
        }
        return result.build();
    }

    private static Split createRemoteSplit(List<ExchangeSourceHandle> handles)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(handles, Optional.empty())));
    }

    @Override
    public void close()
    {
        Closer closer = Closer.create();
        handleSources.forEach(closer::register);
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    @Override
    public Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("handleSources", handleSources)
                .add("targetSplitSizeInBytes", targetSplitSizeInBytes)
                .toString();
    }

    // MoreFutures.whenAnyComplete re-attaches a listener on every getNextBatch() call, accumulating
    // O(n) listeners on long-lived pending futures. SourceRelay attaches one listener per future
    // lifetime and redirects its signal to a fresh delegate on each call.
    // onCompleted() may be called from any thread; reset()/setDelegate() from the getNextBatch() caller.
    private static class SourceRelay
    {
        private SettableFuture<Void> delegate;
        private boolean completed;

        synchronized void onCompleted()
        {
            completed = true;
            if (delegate != null) {
                delegate.set(null);
            }
        }

        synchronized void reset()
        {
            completed = false;
            delegate = null;
        }

        synchronized void setDelegate(SettableFuture<Void> newDelegate)
        {
            delegate = newDelegate;
            if (completed) {
                newDelegate.set(null);
            }
        }
    }
}
