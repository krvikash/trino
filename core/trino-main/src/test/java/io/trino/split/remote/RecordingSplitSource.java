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
package io.trino.split.remote;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.testing.TestingSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class RecordingSplitSource
        implements ConnectorSplitSource
{
    public record Request(int maxSize, DynamicFilterSnapshot snapshot) {}

    private final List<Request> requests = new ArrayList<>();
    private final int totalBatches;
    private final int splitsPerBatch;
    private CompletableFuture<List<ConnectorSplit>> blockedBatch;
    private int failOnCall = -1;
    private RuntimeException failure = new RuntimeException("synchronous enumeration failure");
    private RuntimeException constructionFailure;
    private RuntimeException closeFailure;
    private boolean closed;
    private volatile long memoryUsage;

    public RecordingSplitSource(int totalBatches)
    {
        this(totalBatches, 0);
    }

    public RecordingSplitSource(int totalBatches, int splitsPerBatch)
    {
        this.totalBatches = totalBatches;
        this.splitsPerBatch = splitsPerBatch;
    }

    public void blockNextBatch()
    {
        blockedBatch = new CompletableFuture<>();
    }

    public CompletableFuture<List<ConnectorSplit>> blockedBatch()
    {
        return blockedBatch;
    }

    public void unblock()
    {
        blockedBatch.complete(ImmutableList.of());
    }

    public void failOnCall(int callNumber)
    {
        failOnCall = callNumber;
    }

    public void failOnCall(int callNumber, RuntimeException failure)
    {
        failOnCall = callNumber;
        this.failure = failure;
    }

    public void setMemoryUsage(long memoryUsage)
    {
        this.memoryUsage = memoryUsage;
    }

    /**
     * Fails the caller's {@link RemoteSplitsTask} construction (via
     * {@link #getRequestedDynamicFilterWaitTimeoutMillis}, read first per SPI contract).
     */
    public void failConstructionWith(RuntimeException failure)
    {
        this.constructionFailure = failure;
    }

    public void failCloseWith(RuntimeException failure)
    {
        this.closeFailure = failure;
    }

    public List<Request> requests()
    {
        return ImmutableList.copyOf(requests);
    }

    public List<DynamicFilterSnapshot> snapshots()
    {
        return requests.stream()
                .map(Request::snapshot)
                .collect(toImmutableList());
    }

    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        requests.add(new Request(maxSize, dynamicFilterSnapshot));
        if (requests.size() == failOnCall) {
            throw failure;
        }
        if (blockedBatch != null) {
            return blockedBatch;
        }
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builderWithExpectedSize(splitsPerBatch);
        for (int i = 0; i < splitsPerBatch; i++) {
            splits.add(TestingSplit.createRemoteSplit());
        }
        return completedFuture(splits.build());
    }

    @Override
    public boolean isFinished()
    {
        return requests.size() >= totalBatches;
    }

    @Override
    public void close()
    {
        closed = true;
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        if (constructionFailure != null) {
            throw constructionFailure;
        }
        return 123;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }
}
