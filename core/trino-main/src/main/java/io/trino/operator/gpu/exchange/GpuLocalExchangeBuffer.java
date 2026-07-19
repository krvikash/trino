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
package io.trino.operator.gpu.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * Per-partition queue of {@link GpuPage}s owned by a {@link GpuLocalExchange}. Concurrency
 * contract mirrors {@link io.trino.operator.exchange.LocalExchangeSource}: lock-free counters
 * for the fast path, a single sync block per call for queue mutations.
 */
@ThreadSafe
public final class GpuLocalExchangeBuffer
        implements Closeable
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final LocalExchangeMemoryManager memoryManager;
    private final Consumer<GpuLocalExchangeBuffer> onFinish;

    @GuardedBy("this")
    private final ArrayDeque<QueuedPage> buffer = new ArrayDeque<>();

    private final AtomicInteger bufferedPages = new AtomicInteger();

    @Nullable
    @GuardedBy("this")
    private SettableFuture<Void> notEmptyFuture; // null indicates no callback has been registered

    private volatile boolean finishing;

    public GpuLocalExchangeBuffer(LocalExchangeMemoryManager memoryManager, Consumer<GpuLocalExchangeBuffer> onFinish)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    /**
     * Takes ownership of {@code page} and enqueues it; {@code bytes} is the device-byte cost
     * charged against the memory manager.
     * If finishing, the page is closed and not charged.
     */
    public void add(@Move GpuPage page, long bytes)
    {
        assertNotHoldsLock();

        SettableFuture<Void> notEmptyFuture = null;
        try (ClosingRef<GpuPage> owned = ClosingRef.own(page)) {
            // Charge before publishing; otherwise a concurrent removePage can poll the page and
            // release its bytes before this producer has accounted for them, briefly leaving the
            // memory manager's counter below zero.
            memoryManager.updateMemoryUsage(bytes);

            boolean added = false;
            synchronized (this) {
                if (!finishing) {
                    bufferedPages.incrementAndGet();
                    // cannot fail
                    buffer.add(new QueuedPage(owned.take(), bytes));
                    added = true;
                }
                if (this.notEmptyFuture != null) {
                    notEmptyFuture = this.notEmptyFuture;
                    this.notEmptyFuture = null;
                }
            }

            if (!added) {
                memoryManager.updateMemoryUsage(-bytes);
            }
        }
        finally {
            // Complete the future outside the lock — listeners may run inline. Use finally so a
            // throw from page.close() (native cuDF column-close failures) doesn't leave a reader
            // that registered waitForReading() blocked.
            if (notEmptyFuture != null) {
                notEmptyFuture.set(null);
            }
        }
    }

    /**
     * Moves ownership of the next page out of the buffer, or returns null if empty.
     */
    public @Move GpuPage removePage()
    {
        assertNotHoldsLock();

        QueuedPage entry;
        synchronized (this) {
            entry = buffer.poll();
            if (entry == null) {
                return null;
            }
        }

        // Release outside the lock — memory-manager listeners can call back into this buffer.
        memoryManager.updateMemoryUsage(-entry.bytes());
        bufferedPages.decrementAndGet();

        checkFinished();

        return entry.page();
    }

    public ListenableFuture<Void> waitForReading()
    {
        assertNotHoldsLock();
        if (finishing || bufferedPages.get() > 0) {
            return NOT_BLOCKED;
        }
        synchronized (this) {
            // Recheck inside the lock — addPage/finish may have completed in the gap.
            if (finishing || bufferedPages.get() > 0) {
                return NOT_BLOCKED;
            }
            if (notEmptyFuture == null) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    public boolean isFinished()
    {
        if (!finishing) {
            return false;
        }
        // Synchronize so a concurrent finish()/close() that flipped `finishing` also publishes
        // its bufferedPages update. `finishing` is one-shot, so no need to re-check it here.
        synchronized (this) {
            return bufferedPages.get() == 0;
        }
    }

    public void finish()
    {
        assertNotHoldsLock();

        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            if (finishing) {
                return;
            }
            finishing = true;
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }
        checkFinished();
    }

    /**
     * Closes any queued pages and marks the buffer finished. Idempotent.
     */
    @Override
    public void close()
    {
        assertNotHoldsLock();

        int remainingPagesCount = 0;
        long remainingPagesBytes = 0;
        List<GpuPage> pagesToClose = new ArrayList<>();
        SettableFuture<Void> notEmptyFuture;
        synchronized (this) {
            finishing = true;
            for (QueuedPage entry : buffer) {
                remainingPagesCount++;
                remainingPagesBytes += entry.bytes();
                pagesToClose.add(entry.page());
            }
            buffer.clear();
            bufferedPages.addAndGet(-remainingPagesCount);
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // Close every queued page even if one of them throws (e.g. a native cuDF column-close
        // failure), and always release the bytes back to the shared memory manager — otherwise
        // sibling buffers see backpressure that never lifts.
        RuntimeException closeError = null;
        try {
            for (GpuPage page : pagesToClose) {
                try {
                    page.close();
                }
                catch (RuntimeException e) {
                    if (closeError == null) {
                        closeError = e;
                    }
                    else {
                        closeError.addSuppressed(e);
                    }
                }
            }
        }
        finally {
            memoryManager.updateMemoryUsage(-remainingPagesBytes);
        }

        if (notEmptyFuture != null) {
            notEmptyFuture.set(null);
        }

        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();

        if (closeError != null) {
            throw closeError;
        }
    }

    private void checkFinished()
    {
        assertNotHoldsLock();

        // The check-then-fire is racy: a concurrent removePage / finish / close can satisfy
        // isFinished simultaneously, so onFinish may run more than once. Listeners must be
        // idempotent — receiving the buffer as an argument helps them deduplicate.
        if (isFinished()) {
            onFinish.accept(this);
        }
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private void assertNotHoldsLock()
    {
        assert !Thread.holdsLock(this) : "Cannot execute this method while holding the lock";
    }

    private record QueuedPage(@Own GpuPage page, long bytes) {}
}
