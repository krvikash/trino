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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-side counters describing a {@link RemoteSplitsSource}'s activity, merged into the
 * worker-reported metrics the source exposes through
 * {@link io.trino.spi.connector.ConnectorSplitSource#getMetrics()}.
 */
final class RemoteSplitsSourceMetrics
{
    private final AtomicLong taskCreateAttempts = new AtomicLong();
    private final AtomicLong batchesFetched = new AtomicLong();
    private final AtomicLong splitsFetched = new AtomicLong();
    private final AtomicLong notReadyPolls = new AtomicLong();
    private final AtomicLong fetchRetries = new AtomicLong();
    private final AtomicLong responseBytes = new AtomicLong();
    private final AtomicLong heartbeatsSent = new AtomicLong();

    void taskCreateAttempt()
    {
        taskCreateAttempts.incrementAndGet();
    }

    void batchFetched(int splits)
    {
        batchesFetched.incrementAndGet();
        splitsFetched.addAndGet(splits);
    }

    void notReadyPoll()
    {
        notReadyPolls.incrementAndGet();
    }

    void fetchRetry()
    {
        fetchRetries.incrementAndGet();
    }

    void responseReceived(int bytes)
    {
        responseBytes.addAndGet(bytes);
    }

    void heartbeatSent()
    {
        heartbeatsSent.incrementAndGet();
    }

    Metrics snapshot()
    {
        return new Metrics(ImmutableMap.of(
                "remoteSplitsSource.taskCreateAttempts", new LongCount(taskCreateAttempts.get()),
                "remoteSplitsSource.batchesFetched", new LongCount(batchesFetched.get()),
                "remoteSplitsSource.splitsFetched", new LongCount(splitsFetched.get()),
                "remoteSplitsSource.notReadyPolls", new LongCount(notReadyPolls.get()),
                "remoteSplitsSource.fetchRetries", new LongCount(fetchRetries.get()),
                "remoteSplitsSource.responseBytes", new LongCount(responseBytes.get()),
                "remoteSplitsSource.heartbeatsSent", new LongCount(heartbeatsSent.get())));
    }
}
