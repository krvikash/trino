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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.ExceededMemoryLimitException.exceededLocalUserMemoryLimit;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRemoteSplitsTask
{
    private static final DynamicFilterSnapshot INCOMPLETE_ALL = new DynamicFilterSnapshot(TupleDomain.all(), false);
    private static final DynamicFilterSnapshot INCOMPLETE_NONE = new DynamicFilterSnapshot(TupleDomain.none(), false);
    private static final DynamicFilterSnapshot COMPLETE_ALL = new DynamicFilterSnapshot(TupleDomain.all(), true);
    private static final Duration GENEROUS_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(50);

    @Test
    void testDeliversBatchWithinDeadline()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        // one fetch per poll — nothing runs ahead, even under a complete filter
        assertThat(splitSource.snapshots()).containsExactly(COMPLETE_ALL);
    }

    @Test
    void testEachDeliveredPollFetchesOnDemand()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        Optional<List<ConnectorSplit>> firstBatch = task.poll(0, 10, INCOMPLETE_ALL, GENEROUS_TIMEOUT).join();
        // retrying the same token replays the batch without another connector call
        assertThat(task.poll(0, 10, INCOMPLETE_NONE, GENEROUS_TIMEOUT).join()).isEqualTo(firstBatch);
        // the next token acknowledges it and starts a fresh fetch with its own snapshot
        task.poll(1, 10, INCOMPLETE_NONE, GENEROUS_TIMEOUT).join();
        assertThat(splitSource.snapshots()).containsExactly(INCOMPLETE_ALL, INCOMPLETE_NONE);
    }

    @Test
    void testNotReadyWithinDeadlineReattachesInFlightBatch()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(2);
        splitSource.blockNextBatch();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        // the batch is not ready within the deadline, so the poll returns empty without finishing
        assertThat(task.poll(0, 10, INCOMPLETE_ALL, SHORT_TIMEOUT).join()).isEmpty();
        // a re-poll attaches to the same in-flight batch rather than starting a new fetch
        assertThat(task.poll(0, 10, INCOMPLETE_ALL, SHORT_TIMEOUT).join()).isEmpty();
        assertThat(splitSource.snapshots()).containsExactly(INCOMPLETE_ALL);

        // once the batch completes the next poll delivers it from the same single underlying fetch
        splitSource.unblock();
        assertThat(task.poll(0, 10, INCOMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        assertThat(splitSource.snapshots()).containsExactly(INCOMPLETE_ALL);
    }

    @Test
    void testExhaustedSourceReportsFinished()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(1);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        assertThat(task.isFinished()).isTrue();
        assertThat(splitSource.isClosed()).isTrue();
    }

    @Test
    void testExposesRequestedDynamicFilterWaitTimeout()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(1);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);
        assertThat(task.requestedDynamicFilterWaitTimeoutMillis()).isEqualTo(123);
    }

    @Test
    void testPollAfterCloseFailsInsteadOfReportingEndOfSplits()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        task.poll(0, 10, INCOMPLETE_ALL, GENEROUS_TIMEOUT).join();
        task.close();

        // a closed-but-not-exhausted source must fail the poll: reporting a normal end of splits
        // here would make the coordinator silently truncate the scan
        assertThat(task.isFinished()).isFalse();
        assertThat(task.poll(1, 10, INCOMPLETE_ALL, GENEROUS_TIMEOUT)).isCompletedExceptionally();
    }

    @Test
    void testCloseAfterExhaustionStillReportsEndOfSplits()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(1);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join();
        assertThat(task.isFinished()).isTrue();

        task.close();
        assertThat(task.isFinished()).isTrue();
        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
    }

    @Test
    void testSynchronousFailureBecomesFailedFuture()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.failOnCall(1);
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        // a synchronous throw from the delegate must not escape poll
        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT)).isCompletedExceptionally();
        assertThat(task.isFinished()).isFalse();
        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT)).isCompletedExceptionally();
        assertThat(splitSource.isClosed()).isTrue();
    }

    @Test
    void testConnectorErrorCodeIsPreserved()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.failOnCall(1, new TrinoException(PERMISSION_DENIED, "no access"));
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        // the connector's own error code must survive, not get masked as a generic internal error
        assertThatThrownBy(() -> task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(TrinoException.class)
                .extracting(cause -> ((TrinoException) cause).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testCloseCancelsInFlightPoll()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(2);
        splitSource.blockNextBatch();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        CompletableFuture<Optional<List<ConnectorSplit>>> poll = task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT);
        task.close();
        assertThat(task.isFinished()).isFalse();
        assertThat(splitSource.isClosed()).isTrue();
        // close cancels the in-flight generation, failing the pending poll
        assertThat(poll).isCompletedExceptionally();
    }

    @Test
    void testMemoryReservedAtCreationAndRefreshedPerBatch()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(2);
        splitSource.setMemoryUsage(1000);
        LocalMemoryContext memoryContext = newMemoryContext();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE);

        // the connector's estimate is reserved at creation, before any batch is fetched
        assertThat(memoryContext.getBytes()).isEqualTo(1000);

        splitSource.setMemoryUsage(500);
        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        // refreshed after each delivered batch
        assertThat(memoryContext.getBytes()).isEqualTo(500);

        assertThat(task.poll(1, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        // exhaustion releases the reservation
        assertThat(task.isFinished()).isTrue();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    void testCloseReleasesMemoryReservation()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.setMemoryUsage(1000);
        LocalMemoryContext memoryContext = newMemoryContext();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE);
        assertThat(memoryContext.getBytes()).isEqualTo(1000);

        task.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    @Timeout(10)
    void testSlowConnectorCloseDoesNotBlockPolls()
            throws Exception
    {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        RecordingSplitSource splitSource = new RecordingSplitSource(3)
        {
            @Override
            public void close()
            {
                closeStarted.countDown();
                await(releaseClose);
                super.close();
            }
        };
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, newMemoryContext(), Long.MAX_VALUE);

        Thread closer = new Thread(task::close);
        closer.start();
        try {
            assertThat(closeStarted.await(10, SECONDS)).isTrue();
            // the connector close is still in flight; a poll must fail fast on the closed flag
            // instead of queueing behind the close on the task monitor
            assertThatThrownBy(() -> task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("closed");
        }
        finally {
            releaseClose.countDown();
            closer.join();
        }
        assertThat(splitSource.isClosed()).isTrue();
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    void testFullMemoryPoolAtCreationIsRejectedAsRetryable()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.setMemoryUsage(1000);
        FullPoolMemoryContext memoryContext = new FullPoolMemoryContext();

        assertThatThrownBy(() -> new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE))
                .isInstanceOf(TrinoException.class)
                .extracting(failure -> ((TrinoException) failure).getErrorCode())
                .isEqualTo(REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE.toErrorCode());
        // the transient reservation must be released so the pool is not leaked into
        assertThat(memoryContext.closed).isTrue();
    }

    @Test
    void testEstimateOverPerNodeLimitFailsDeterministically()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.setMemoryUsage(1000);

        assertThatThrownBy(() -> new RemoteSplitsTask(splitSource, newMemoryContext(), 999))
                .isInstanceOf(TrinoException.class)
                .extracting(failure -> ((TrinoException) failure).getErrorCode())
                .isEqualTo(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode());
    }

    @Test
    void testQueryHeadroomTakenOnNodeAtCreationIsRejectedAsRetryable()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(3);
        splitSource.setMemoryUsage(1000);
        // the estimate fits under the per-node limit, but this query's other tasks on the node
        // already hold the headroom, so the reservation itself is rejected
        LimitedMemoryContext memoryContext = new LimitedMemoryContext();

        assertThatThrownBy(() -> new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE))
                .isInstanceOf(TrinoException.class)
                .extracting(failure -> ((TrinoException) failure).getErrorCode())
                .isEqualTo(REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE.toErrorCode());
        assertThat(memoryContext.closed).isTrue();
    }

    @Test
    void testMemoryLimitBreachMidEnumerationFailsWithItsOwnCode()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(2, 2);
        splitSource.setMemoryUsage(1000);
        // admits the creation reservation, then the query's other tasks take the node's headroom
        LimitedAfterCreationMemoryContext memoryContext = new LimitedAfterCreationMemoryContext();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE);

        // unlike at creation, splits were already handed out, so the coordinator cannot restart
        // enumeration on another worker: the failure must carry the real memory-limit code, not
        // the retryable creation code that promises a retry the fetch path never does
        assertThatThrownBy(() -> task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(TrinoException.class)
                .extracting(cause -> ((TrinoException) cause).getErrorCode())
                .isEqualTo(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode());
    }

    @Test
    void testPoolShortfallMidEnumerationPausesNextFetch()
    {
        RecordingSplitSource splitSource = new RecordingSplitSource(2, 2);
        splitSource.setMemoryUsage(1000);
        BlockedRefreshMemoryContext memoryContext = new BlockedRefreshMemoryContext();
        RemoteSplitsTask task = new RemoteSplitsTask(splitSource, memoryContext, Long.MAX_VALUE);

        assertThat(task.poll(0, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();

        // the pool cannot cover the refreshed reservation; the memory is already allocated by
        // the source, so the only backpressure is answering not-ready instead of fetching more
        assertThat(task.poll(1, 10, COMPLETE_ALL, SHORT_TIMEOUT).join()).isEmpty();
        assertThat(splitSource.requests()).hasSize(1);

        memoryContext.freePool();
        assertThat(task.poll(1, 10, COMPLETE_ALL, GENEROUS_TIMEOUT).join()).isPresent();
        assertThat(splitSource.requests()).hasSize(2);
    }

    private static LocalMemoryContext newMemoryContext()
    {
        return newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
    }

    /**
     * Accepts every reservation but always reports the pool as out of free memory, like
     * {@code MemoryPool} does when a reservation pushes free bytes below zero.
     */
    private static final class FullPoolMemoryContext
            implements LocalMemoryContext
    {
        public long bytes;
        public boolean closed;

        @Override
        public long getBytes()
        {
            return bytes;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            this.bytes = bytes;
            return SettableFuture.create();
        }

        @Override
        public ListenableFuture<Void> addBytes(long delta)
        {
            return setBytes(bytes + delta);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return false;
        }

        @Override
        public void close()
        {
            closed = true;
            bytes = 0;
        }
    }

    /**
     * Rejects every reservation the way {@code QueryContext.enforceUserMemoryLimit} does when the
     * query's tasks on this node already hold the per-node headroom.
     */
    private static final class LimitedMemoryContext
            implements LocalMemoryContext
    {
        public boolean closed;

        @Override
        public long getBytes()
        {
            return 0;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            throw exceededLocalUserMemoryLimit(DataSize.ofBytes(500), "other tasks hold the headroom");
        }

        @Override
        public ListenableFuture<Void> addBytes(long delta)
        {
            return setBytes(delta);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return false;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    /**
     * Admits the creation reservation, then rejects every refresh the way
     * {@code QueryContext.enforceUserMemoryLimit} does once the query's other tasks on this node
     * have taken the per-node headroom.
     */
    private static final class LimitedAfterCreationMemoryContext
            implements LocalMemoryContext
    {
        public int reservations;

        @Override
        public long getBytes()
        {
            return 0;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            reservations++;
            if (reservations > 1) {
                throw exceededLocalUserMemoryLimit(DataSize.ofBytes(500), "other tasks hold the headroom");
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> addBytes(long delta)
        {
            return setBytes(delta);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return false;
        }

        @Override
        public void close() {}
    }

    /**
     * Admits the creation reservation, then reports the pool as out of free memory on every
     * refresh until {@link #freePool()}, like {@code MemoryPool} does while free bytes stay
     * below zero.
     */
    private static final class BlockedRefreshMemoryContext
            implements LocalMemoryContext
    {
        public final SettableFuture<Void> poolFreed = SettableFuture.create();
        public int reservations;
        public long bytes;

        public void freePool()
        {
            poolFreed.set(null);
        }

        @Override
        public long getBytes()
        {
            return bytes;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            this.bytes = bytes;
            reservations++;
            if (reservations == 1) {
                return immediateVoidFuture();
            }
            return poolFreed;
        }

        @Override
        public ListenableFuture<Void> addBytes(long delta)
        {
            return setBytes(bytes + delta);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return false;
        }

        @Override
        public void close()
        {
            bytes = 0;
        }
    }
}
