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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.operator.output.PositionsAppenderFactory;
import io.trino.operator.output.PositionsAppenderPageBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/**
 * Local partitioning exchange divides the input page into multiple partitions, producing small pages as a result.
 * Small pages hurt the performance of subsequent operators, as many operators have significant per-page overhead.
 * This class improves that by merging the output pages using `PositionsAppenderPageBuilder` to produce big pages again.
 * This improves the CPU performance of subsequent operators, at a cost of increased memory usage as the blocks produced
 * by the `PositionsAppenderPageBuilder` retain more memory than necessary, plus the `PositionsAppenderPageBuilder` consumes
 * around the max page size (1MB) memory per partition.
 * We still compact the page in the `PartitioningExchanger` before sending it
 * to the `MergingLocalExchangePageBuffer` wasting some CPU in a process.
 * The alternative would be to send the input page with a list of positions to copy, but that approach makes the local exchange
 * retain the entire page even if most of it was already processed, which both uses more memory and blocks upstream stages
 * from processing due to back pressure via the limited local exchange buffer memory. This, in turn, makes the upstream operators
 * buffer more pages, increasing the peak memory even further.
 * <p>
 * Thread safety works if only "writing" methods (addPage, finish) are called from another thread
 * than "reading" methods (removePage, waitForReading, isFinished, close).
 */
public class MergingLocalExchangePageBuffer
        implements LocalExchangePageBuffer
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final LocalExchangeMemoryManager memoryManager;
    private final Consumer<LocalExchangePageBuffer> onFinish;

    @GuardedBy("this")
    private final Queue<Page> buffer = new ArrayDeque<>();
    private final Queue<Page> outputBuffer = new ArrayDeque<>();
    private final PositionsAppenderPageBuilder outputBuilder;

    private final AtomicLong bufferedBytes = new AtomicLong();
    private final AtomicInteger bufferedPages = new AtomicInteger();
    private LocalMemoryContext memoryContext;

    @Nullable
    @GuardedBy("this")
    private SettableFuture<Void> notEmptyFuture; // null indicates no callback has been registered

    private volatile boolean finishing;

    public MergingLocalExchangePageBuffer(
            LocalExchangeMemoryManager memoryManager,
            Consumer<LocalExchangePageBuffer> onFinish,
            List<Type> sourceTypes,
            PositionsAppenderFactory positionsAppenderFactory)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
        this.outputBuilder = PositionsAppenderPageBuilder.withMaxPageSize(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, requireNonNull(sourceTypes, "sourceTypes is null"), positionsAppenderFactory);
    }

    public void init(OperatorContext operatorContext)
    {
        verify(this.memoryContext == null);
        this.memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext(MergingLocalExchangePageBuffer.class.getSimpleName());
        updateMemoryUsage();
    }

    @Override
    public LocalExchangeBufferInfo getBufferInfo()
    {
        // This must be lock free to assure task info creation is fast
        // Note: the stats my be internally inconsistent
        return new LocalExchangeBufferInfo(bufferedBytes.get(), bufferedPages.get());
    }

    @Override
    public void addPage(Page page)
    {
        assertNotHoldsLock();

        boolean added = false;
        SettableFuture<Void> notEmptyFuture = null;
        long retainedSizeInBytes = page.getRetainedSizeInBytes();
        synchronized (this) {
            // ignore pages after finish
            if (!finishing) {
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferedBytes.addAndGet(retainedSizeInBytes);
                bufferedPages.incrementAndGet();
                buffer.add(page);
                added = true;
            }

            // we just added a page (or we are finishing) so we are not empty
            if (this.notEmptyFuture != null) {
                notEmptyFuture = this.notEmptyFuture;
                this.notEmptyFuture = null;
            }
        }

        if (!added) {
            memoryManager.updateMemoryUsage(-retainedSizeInBytes);
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }
    }

    @Override
    public WorkProcessor<Page> pages()
    {
        return WorkProcessor.create(() -> {
            Page page = removePage();
            if (page == null) {
                if (isFinished()) {
                    return ProcessState.finished();
                }

                ListenableFuture<Void> blocked = waitForReading();
                if (!blocked.isDone()) {
                    return ProcessState.blocked(blocked);
                }

                return ProcessState.yielded();
            }

            return ProcessState.ofResult(page);
        });
    }

    @Override
    public Page removePage()
    {
        assertNotHoldsLock();

        Queue<Page> tempBuffer = new ArrayDeque<>();
        synchronized (this) {
            tempBuffer.addAll(buffer);
            buffer.clear();
        }

        while (!tempBuffer.isEmpty()) {
            Page inputPage = tempBuffer.poll();
            long retainedSizeInBytes = inputPage.getRetainedSizeInBytes();

            memoryManager.updateMemoryUsage(-retainedSizeInBytes);
            bufferedBytes.addAndGet(-retainedSizeInBytes);
            bufferedPages.decrementAndGet();

            outputBuilder.appendRangeToOutputPartition(inputPage, 0, inputPage.getPositionCount());
            if (outputBuilder.isFull()) {
                addToOutputBuffer(outputBuilder.build());
            }
        }
        if (finishing && !outputBuilder.isEmpty()) {
            addToOutputBuffer(outputBuilder.build());
        }
        updateMemoryUsage();

        Page outputPage = outputBuffer.poll();

        if (outputPage != null) {
            long retainedSizeInBytes = outputPage.getRetainedSizeInBytes();
            memoryManager.updateMemoryUsage(-retainedSizeInBytes);
            bufferedBytes.addAndGet(-retainedSizeInBytes);
            bufferedPages.decrementAndGet();
        }

        checkFinished();
        return outputPage;
    }

    private synchronized boolean isInputBufferEmpty()
    {
        return buffer.isEmpty();
    }

    private void addToOutputBuffer(Page outPage)
    {
        outputBuffer.add(outPage);
        long retainedSizeInBytes = outPage.getRetainedSizeInBytes();
        memoryManager.updateMemoryUsage(retainedSizeInBytes);
        bufferedBytes.addAndGet(retainedSizeInBytes);
        bufferedPages.incrementAndGet();
    }

    @Override
    public ListenableFuture<Void> waitForReading()
    {
        assertNotHoldsLock();
        // Fast path, definitely not blocked
        if (finishing || bufferedPages.get() > 0) {
            return NOT_BLOCKED;
        }

        synchronized (this) {
            // re-check after synchronizing
            if (finishing || bufferedPages.get() > 0) {
                return NOT_BLOCKED;
            }
            verify(outputBuffer.isEmpty(), "outputBuffer not empty");
            verify(buffer.isEmpty(), "inputBuffer not empty");
            // if we need to block readers, and the current future is complete, create a new one
            if (notEmptyFuture == null) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    @Override
    public boolean isFinished()
    {
        // Common case fast-path without synchronizing
        if (!finishing) {
            return false;
        }
        synchronized (this) {
            // Synchronize to ensure effects of an in-flight close() or finish() are observed
            return finishing && bufferedPages.get() == 0 && outputBuilder.isEmpty();
        }
    }

    @Override
    public void finish()
    {
        assertNotHoldsLock();
        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            if (finishing) {
                return;
            }
            finishing = true;

            // Unblock any waiters
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }

        checkFinished();
    }

    @Override
    public void close()
    {
        assertNotHoldsLock();
        int remainingPagesCount = 0;
        long remainingPagesRetainedSizeInBytes = 0;
        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            finishing = true;

            for (Page page : buffer) {
                remainingPagesCount++;
                remainingPagesRetainedSizeInBytes += page.getRetainedSizeInBytes();
            }
            buffer.clear();
            for (Page page : outputBuffer) {
                remainingPagesCount++;
                remainingPagesRetainedSizeInBytes += page.getRetainedSizeInBytes();
            }
            outputBuffer.clear();
            outputBuilder.release();
            bufferedBytes.addAndGet(-remainingPagesRetainedSizeInBytes);
            bufferedPages.addAndGet(-remainingPagesCount);

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // free all the remaining pages
        memoryManager.updateMemoryUsage(-remainingPagesRetainedSizeInBytes);
        memoryContext.close();
        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }

        // this will always fire the finished event
        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();
    }

    private void checkFinished()
    {
        assertNotHoldsLock();

        if (isFinished()) {
            // notify finish listener outside of lock, since it may make a callback
            // NOTE: due the race in this method, the onFinish may be called multiple times
            // it is expected that the implementer handles this (which is why this source
            // is passed to the function)
            onFinish.accept(this);
        }
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private void assertNotHoldsLock()
    {
        assert !Thread.holdsLock(this) : "Cannot execute this method while holding the lock";
    }

    private void updateMemoryUsage()
    {
        memoryContext.setBytes(outputBuilder.getRetainedSizeInBytes());
    }
}
