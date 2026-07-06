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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.metadata.Split;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource.SplitBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestExchangeSplitSource
{
    // getNextBatch ignores maxSize; the value is arbitrary.
    private static final int MAX_SIZE = 1000;
    // Large enough that handles are never split apart by size unless a test opts in.
    private static final long UNLIMITED_SPLIT_SIZE = 1024 * 1024;

    @Test
    void testAllHandleSourcesAreConsumed()
    {
        ExchangeSourceHandle a1 = handle(1, 0, 10);
        ExchangeSourceHandle a2 = handle(2, 0, 10);
        ExchangeSourceHandle b1 = handle(3, 0, 10);

        TestingHandleSource sourceA = new TestingHandleSource();
        sourceA.enqueue(batch(false, a1));
        sourceA.enqueue(batch(true, a2));
        TestingHandleSource sourceB = new TestingHandleSource();
        sourceB.enqueue(batch(true, b1));

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(ImmutableList.of(sourceA, sourceB), UNLIMITED_SPLIT_SIZE)) {
            assertThat(handlesOf(drainAll(splitSource)))
                    .containsExactlyInAnyOrder(a1, a2, b1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void testSlowSourceDoesNotBlockFastSources(int slowIndex)
    {
        List<ExchangeSourceHandle> handles = ImmutableList.of(handle(0, 0, 10), handle(1, 1, 10), handle(2, 2, 10));
        List<TestingHandleSource> sources = ImmutableList.of(new TestingHandleSource(), new TestingHandleSource(), new TestingHandleSource());

        CompletableFuture<ExchangeSourceHandleBatch> slowBatch = sources.get(slowIndex).enqueuePending();
        for (int i = 0; i < sources.size(); i++) {
            if (i != slowIndex) {
                sources.get(i).enqueue(batch(true, handles.get(i)));
            }
        }

        List<ExchangeSourceHandle> fastSources = otherThan(handles, slowIndex);
        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(ImmutableList.copyOf(sources), UNLIMITED_SPLIT_SIZE)) {
            // Fast sources are served immediately even though the slow source is still in flight.
            ListenableFuture<SplitBatch> firstBatch = splitSource.getNextBatch(MAX_SIZE);
            assertThat(firstBatch).isDone();
            SplitBatch first = getFutureValue(firstBatch);
            assertThat(first.isLastBatch()).isFalse();
            assertThat(handlesOf(first.getSplits())).containsExactlyInAnyOrderElementsOf(fastSources);

            // Once the slow source completes, its handle is delivered and the source finishes.
            slowBatch.complete(batch(true, handles.get(slowIndex)));
            SplitBatch second = getFutureValue(splitSource.getNextBatch(MAX_SIZE));
            assertThat(second.isLastBatch()).isTrue();
            assertThat(handlesOf(second.getSplits())).containsExactly(handles.get(slowIndex));
        }
    }

    @Test
    void testPendingSourceCompletedFromAnotherThread()
            throws Exception
    {
        ExchangeSourceHandle handle = handle(1, 0, 10);
        TestingHandleSource source = new TestingHandleSource();
        CompletableFuture<ExchangeSourceHandleBatch> pending = source.enqueuePending();

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(source, UNLIMITED_SPLIT_SIZE)) {
            ListenableFuture<SplitBatch> nextBatch = splitSource.getNextBatch(MAX_SIZE);
            assertThat(nextBatch).isNotDone();

            Thread completer = new Thread(() -> pending.complete(batch(true, handle)));
            completer.start();
            completer.join();

            SplitBatch batch = getFutureValue(nextBatch);
            assertThat(batch.isLastBatch()).isTrue();
            assertThat(handlesOf(batch.getSplits())).containsExactly(handle);
        }
    }

    @Test
    void testPendingSourceIsNotRestartedAcrossCalls()
    {
        // A source that keeps producing (never-last) batches, one per getNextBatch call.
        TestingHandleSource fast = new TestingHandleSource();
        for (int i = 0; i < 5; i++) {
            fast.enqueue(batch(false, handle(i, 0, 10)));
        }
        // A source that stays in flight for the whole test.
        TestingHandleSource slow = new TestingHandleSource();
        slow.enqueuePending();

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(ImmutableList.of(fast, slow), UNLIMITED_SPLIT_SIZE)) {
            for (int i = 0; i < 5; i++) {
                SplitBatch batch = getFutureValue(splitSource.getNextBatch(MAX_SIZE));
                assertThat(batch.isLastBatch()).isFalse();
            }

            // The fast source is restarted on every call, the in-flight slow source is polled exactly once.
            // This is the guarantee behind SourceRelay: a single getNextBatch() per in-flight future means a
            // single completion listener per future lifetime, rather than one accumulating on every call.
            assertThat(fast.getNextBatchCallCount()).isEqualTo(5);
            assertThat(slow.getNextBatchCallCount()).isEqualTo(1);
        }
    }

    @Test
    void testHandlesAreGroupedIntoSplitsByTargetSize()
    {
        ExchangeSourceHandle h1 = handle(1, 0, 30);
        ExchangeSourceHandle h2 = handle(2, 0, 30);
        ExchangeSourceHandle h3 = handle(3, 0, 60);

        TestingHandleSource source = new TestingHandleSource();
        source.enqueue(batch(true, h1, h2, h3));

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(source, 100)) {
            List<Split> splits = drainAll(splitSource);
            assertThat(splits).hasSize(2);
            assertThat(handlesOf(splits.get(0))).containsExactly(h1, h2);
            assertThat(handlesOf(splits.get(1))).containsExactly(h3);
        }
    }

    @Test
    void testHandlesAreGroupedIntoSplitsByPartition()
    {
        ExchangeSourceHandle partition0First = handle(1, 0, 10);
        ExchangeSourceHandle partition1 = handle(2, 1, 10);
        ExchangeSourceHandle partition0Second = handle(3, 0, 10);

        TestingHandleSource source = new TestingHandleSource();
        source.enqueue(batch(true, partition0First, partition1, partition0Second));

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(source, UNLIMITED_SPLIT_SIZE)) {
            List<Split> splits = drainAll(splitSource);
            assertThat(splits).hasSize(2);
            assertThat(handlesOf(splits.get(0))).containsExactly(partition0First, partition0Second);
            assertThat(handlesOf(splits.get(1))).containsExactly(partition1);
        }
    }

    @Test
    void testEmptyLastBatchProducesEmptyFinishedBatch()
    {
        TestingHandleSource source = new TestingHandleSource();
        source.enqueue(batch(true));

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(source, UNLIMITED_SPLIT_SIZE)) {
            SplitBatch batch = getFutureValue(splitSource.getNextBatch(MAX_SIZE));
            assertThat(batch.getSplits()).isEmpty();
            assertThat(batch.isLastBatch()).isTrue();

            // With every source exhausted a further call returns an empty finished batch without polling again.
            SplitBatch afterExhausted = getFutureValue(splitSource.getNextBatch(MAX_SIZE));
            assertThat(afterExhausted.getSplits()).isEmpty();
            assertThat(afterExhausted.isLastBatch()).isTrue();
            assertThat(source.getNextBatchCallCount()).isEqualTo(1);
        }
    }

    @Test
    void testFailureIsPropagated()
    {
        TestingHandleSource source = new TestingHandleSource();
        CompletableFuture<ExchangeSourceHandleBatch> pending = source.enqueuePending();
        pending.completeExceptionally(new RuntimeException("boom"));

        try (ExchangeSplitSource splitSource = new ExchangeSplitSource(source, UNLIMITED_SPLIT_SIZE)) {
            ListenableFuture<SplitBatch> nextBatch = splitSource.getNextBatch(MAX_SIZE);
            assertThatThrownBy(() -> getFutureValue(nextBatch)).hasRootCauseMessage("boom");
        }
    }

    @Test
    void testCloseClosesAllSources()
    {
        TestingHandleSource sourceA = new TestingHandleSource();
        TestingHandleSource sourceB = new TestingHandleSource();
        TestingHandleSource sourceC = new TestingHandleSource();

        ExchangeSplitSource splitSource = new ExchangeSplitSource(ImmutableList.of(sourceA, sourceB, sourceC), UNLIMITED_SPLIT_SIZE);
        splitSource.close();

        assertThat(sourceA.getCloseCallCount()).isEqualTo(1);
        assertThat(sourceB.getCloseCallCount()).isEqualTo(1);
        assertThat(sourceC.getCloseCallCount()).isEqualTo(1);
    }

    private static List<Split> drainAll(ExchangeSplitSource splitSource)
    {
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        while (true) {
            SplitBatch batch = getFutureValue(splitSource.getNextBatch(MAX_SIZE));
            splits.addAll(batch.getSplits());
            if (batch.isLastBatch()) {
                return splits.build();
            }
        }
    }

    private static List<ExchangeSourceHandle> handlesOf(List<Split> splits)
    {
        return splits.stream()
                .flatMap(split -> handlesOf(split).stream())
                .collect(toImmutableList());
    }

    private static List<ExchangeSourceHandle> handlesOf(Split split)
    {
        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        return ((SpoolingExchangeInput) remoteSplit.getExchangeInput()).getExchangeSourceHandles();
    }

    private static List<ExchangeSourceHandle> otherThan(List<ExchangeSourceHandle> handles, int excludedIndex)
    {
        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (int i = 0; i < handles.size(); i++) {
            if (i != excludedIndex) {
                result.add(handles.get(i));
            }
        }
        return result.build();
    }

    private static ExchangeSourceHandleBatch batch(boolean lastBatch, ExchangeSourceHandle... handles)
    {
        return new ExchangeSourceHandleBatch(ImmutableList.copyOf(handles), lastBatch);
    }

    private static ExchangeSourceHandle handle(int id, int partitionId, long sizeInBytes)
    {
        return new TestingExchangeSourceHandle(id, partitionId, sizeInBytes);
    }

    private static final class TestingHandleSource
            implements ExchangeSourceHandleSource
    {
        private final Queue<CompletableFuture<ExchangeSourceHandleBatch>> batches = new ArrayDeque<>();
        private int nextBatchCallCount;
        private int closeCallCount;

        void enqueue(ExchangeSourceHandleBatch batch)
        {
            batches.add(CompletableFuture.completedFuture(batch));
        }

        CompletableFuture<ExchangeSourceHandleBatch> enqueuePending()
        {
            CompletableFuture<ExchangeSourceHandleBatch> future = new CompletableFuture<>();
            batches.add(future);
            return future;
        }

        int getNextBatchCallCount()
        {
            return nextBatchCallCount;
        }

        int getCloseCallCount()
        {
            return closeCallCount;
        }

        @Override
        public CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
        {
            nextBatchCallCount++;
            CompletableFuture<ExchangeSourceHandleBatch> future = batches.poll();
            checkState(future != null, "no batch queued");
            return future;
        }

        @Override
        public void close()
        {
            closeCallCount++;
        }
    }
}
