/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.lang.Math.toIntExact;

@ThreadSafe
public class BufferExchangeSourceHandleSource
        implements ExchangeSourceHandleSource
{
    private static final Logger log = Logger.get(ExchangeSourceHandleSource.class);

    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(BufferExchangeSourceHandleSource.class).instanceSize());

    @GuardedBy("this")
    private List<ExchangeSourceHandle> nextBatchHandles = new ArrayList<>();
    @GuardedBy("this")
    private boolean lastBatchReceived;
    @GuardedBy("this")
    private CompletableFuture<ExchangeSourceHandleBatch> nextBatchFuture;
    @GuardedBy("this")
    private Throwable failure;
    @GuardedBy("this")
    private boolean closed;

    public synchronized void addSourceHandles(List<ExchangeSourceHandle> sourceHandles, boolean lastBatch)
    {
        if (closed) {
            // BufferExchangeSourceHandleSource may be closed asynchronously by engine any time
            return;
        }

        if (lastBatchReceived) {
            if (sourceHandles.isEmpty() && lastBatch) {
                // extra empty call is ok
                verify(nextBatchFuture == null, "batch future should have been completed already");
                return;
            }
            throw new IllegalStateException("already received last batch");
        }

        if (sourceHandles.isEmpty() && !lastBatch) {
            // nothing to do
            return;
        }

        if (lastBatch) {
            lastBatchReceived = true;
        }

        if (failure != null) {
            verify(nextBatchFuture == null, "batch future should have been complete with failure");
            // ignore chunks
            return;
        }

        if (nextBatchFuture != null) {
            verify(nextBatchHandles.isEmpty(), "ready handles should be empty if nextBatchFuture is set");
            completeNextBatchFuture(new ExchangeSourceHandleBatch(sourceHandles, lastBatch));
            return;
        }

        nextBatchHandles.addAll(sourceHandles);
    }

    public synchronized void markFailed(Throwable failure)
    {
        this.failure = failure;
        completeNextBatchFutureExceptionally(failure);
    }

    @Override
    public synchronized CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
    {
        checkState(!closed, "already closed");
        checkState(nextBatchFuture == null, "previous batch future not returned yet");

        if (failure != null) {
            return CompletableFuture.failedFuture(failure);
        }

        if (!nextBatchHandles.isEmpty() || lastBatchReceived) {
            // we can return completed future immediately
            ExchangeSourceHandleBatch batch = new ExchangeSourceHandleBatch(nextBatchHandles, lastBatchReceived);
            nextBatchHandles = new ArrayList<>();
            return CompletableFuture.completedFuture(batch);
        }

        // create future to be completed later
        nextBatchFuture = new CompletableFuture<>();
        return nextBatchFuture;
    }

    @Override
    public void close()
    {
        synchronized (this) {
            nextBatchHandles.clear();
            closed = true;
            completeNextBatchFutureExceptionally(new RuntimeException("ExchangeSourceHandleSource closed"));
        }
    }

    @GuardedBy("this")
    private void completeNextBatchFuture(ExchangeSourceHandleBatch batch)
    {
        CompletableFuture<ExchangeSourceHandleBatch> futureToComplete = nextBatchFuture;
        nextBatchFuture = null; // null before completing future - may impact registered listeners
        futureToComplete.complete(batch);
    }

    @GuardedBy("this")
    private void completeNextBatchFutureExceptionally(Throwable failure)
    {
        if (nextBatchFuture != null) {
            CompletableFuture<ExchangeSourceHandleBatch> futureToComplete = nextBatchFuture;
            nextBatchFuture = null; // null before completing future - may impact registered listeners
            futureToComplete.completeExceptionally(failure);
        }
    }

    public synchronized long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(nextBatchHandles, ExchangeSourceHandle::getRetainedSizeInBytes);
    }
}
