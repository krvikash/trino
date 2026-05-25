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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.ExceededMemoryLimitException;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.metrics.Metrics;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.unwrapCompletionException;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_GENERATION_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Server-side state of a single remote split task: the {@link ConnectorSplitSource} drained by the
 * coordinator through a long poll. {@link #poll} returns the next batch if it becomes ready within
 * the poll deadline; otherwise it returns {@link Optional#empty()} while leaving the in-flight batch
 * running, so the coordinator's next poll re-attaches to it rather than restarting it. Bounding the
 * response this way keeps a slow batch from tripping the coordinator's HTTP idle timeout.
 * A completed batch remains attached to its token until a poll with the next token acknowledges it,
 * so retrying a poll whose HTTP response was lost replays the same splits.
 *
 * <p>Batches are fetched only when a poll arrives, each with the requesting poll's own dynamic-filter
 * snapshot. The coordinator polls a task one request at a time, so at most one batch is ever in flight.
 *
 * <p>Every poll refreshes the last-access time; the task manager's reaper drops a task only after it
 * has been idle past the stale timeout, and the coordinator heartbeats running tasks between polls so
 * that idleness means a dead or disconnected coordinator rather than a slow consumer.
 *
 * <p>The connector's estimated enumeration memory is reserved against the query's memory on this
 * node at construction and refreshed after every delivered batch; the reservation is released when
 * the source is exhausted or the task closes. A refreshed reservation the pool cannot cover right
 * now pauses further fetches until it can.
 */
final class RemoteSplitsTask
{
    @VisibleForTesting
    static final long STALE_TIMEOUT_NANOS = Duration.ofMinutes(5).toNanos();

    private final ConnectorSplitSource splitSource;
    private final LocalMemoryContext memoryContext;
    private final long requestedDynamicFilterWaitTimeoutMillis;
    @GuardedBy("this")
    private long currentToken;
    @GuardedBy("this")
    private CompletableFuture<List<ConnectorSplit>> currentBatch;
    @GuardedBy("this")
    private CompletableFuture<Void> memoryReservation = completedFuture(null);
    private volatile long lastAccessNanos;
    private volatile boolean finished;
    @GuardedBy("this")
    private boolean closed;

    RemoteSplitsTask(ConnectorSplitSource splitSource, LocalMemoryContext memoryContext, long maxMemoryPerNode)
    {
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        // per SPI contract this must be read before the first getNextBatch call
        this.requestedDynamicFilterWaitTimeoutMillis = splitSource.getRequestedDynamicFilterWaitTimeoutMillis();
        this.lastAccessNanos = System.nanoTime();
        // Reserve the connector's estimate before any batch runs, so a task whose enumeration does
        // not fit on this node fails at creation rather than at the first fetch. Only an estimate
        // that exceeds the per-node query limit outright is deterministic — the limit is uniform
        // across workers — and fails fast with EXCEEDED_LOCAL_MEMORY_LIMIT. Every other shortfall
        // is specific to this node (this query's other tasks holding the headroom under that limit,
        // or the pool having no free memory right now) and is reported with the retryable code so
        // the coordinator moves creation to a worker with more room.
        long memoryEstimate = splitSource.getMemoryUsage();
        if (memoryEstimate > maxMemoryPerNode) {
            throw new TrinoException(
                    EXCEEDED_LOCAL_MEMORY_LIMIT,
                    "Remote split enumeration estimate of %s bytes exceeds the per-node memory limit of %s bytes".formatted(memoryEstimate, maxMemoryPerNode));
        }
        // Return REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE to allow retries in case of temporary memory pressure
        try {
            if (memoryEstimate > 0 && !memoryContext.setBytes(memoryEstimate).isDone()) {
                memoryContext.close();
                throw new TrinoException(
                        REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE,
                        "Remote split enumeration requires %s bytes of memory, which does not fit in the memory pool on this node right now".formatted(memoryEstimate));
            }
        }
        catch (ExceededMemoryLimitException e) {
            memoryContext.close();
            throw new TrinoException(REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE, e.getMessage(), e);
        }
    }

    /**
     * Long-polls the batch identified by {@code token}. A repeated token replays or re-attaches to the
     * same batch; the next token acknowledges that batch and starts a fresh fetch with its own snapshot.
     * An older token is answered as not-ready without changing state, making delayed requests harmless.
     */
    synchronized CompletableFuture<Optional<List<ConnectorSplit>>> poll(long token, int maxSize, DynamicFilterSnapshot snapshot, Duration pollTimeout)
    {
        lastAccessNanos = System.nanoTime();
        checkArgument(token <= currentToken + 1, "Unexpected token: %s", token);
        if (token < currentToken) {
            return completedFuture(Optional.empty());
        }
        if (token > currentToken) {
            checkState(currentBatch != null && currentBatch.isDone(), "Cannot acknowledge an incomplete batch");
            currentToken = token;
            currentBatch = null;
        }
        if (currentBatch == null) {
            if (!memoryReservation.isDone()) {
                // The pool cannot cover the source's current usage right now. That memory is
                // already allocated on this node and the delivered splits rule out moving
                // enumeration elsewhere, so pause instead of fetching more: answer not-ready
                // until the pool frees up and the coordinator's re-poll starts the next batch.
                // Wait on a copy() so the timeout never completes the pool's shared future.
                return memoryReservation.copy()
                        .thenApply(_ -> Optional.<List<ConnectorSplit>>empty())
                        .completeOnTimeout(Optional.empty(), pollTimeout.toMillis(), MILLISECONDS);
            }
            currentBatch = fetchNextBatch(maxSize, snapshot);
        }
        // wait on a copy() so a poll timing out never completes or cancels the shared in-flight batch
        return currentBatch.copy()
                .thenApply(Optional::of)
                .completeOnTimeout(Optional.empty(), pollTimeout.toMillis(), MILLISECONDS);
    }

    /**
     * Returns the in-flight batch's future, starting a fresh fetch when none is running. Folds the
     * batch's completion — exhaustion, failure, or a concurrent close — into the task state, so a
     * delivered batch never masks the source being closed mid-enumeration.
     */
    private synchronized CompletableFuture<List<ConnectorSplit>> fetchNextBatch(int maxSize, DynamicFilterSnapshot snapshot)
    {
        if (finished) {
            return completedFuture(ImmutableList.of());
        }
        if (closed) {
            return failedFuture(new TrinoException(REMOTE_SPLITS_GENERATION_ERROR, "Remote splits source is closed"));
        }
        CompletableFuture<List<ConnectorSplit>> batch;
        try {
            batch = splitSource.getNextBatch(maxSize, snapshot);
        }
        catch (Throwable t) {
            batch = failedFuture(t);
        }
        return batch.handle(this::consumeBatch);
    }

    private List<ConnectorSplit> consumeBatch(List<ConnectorSplit> result, Throwable error)
    {
        lastAccessNanos = System.nanoTime();
        if (error != null) {
            if (transitionToClosed()) {
                releaseSource(true);
            }
            // preserve the connector's own error code (e.g. permission denied) instead of
            // masking it as a generic internal error
            Throwable cause = unwrapCompletionException(error);
            if (cause instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(REMOTE_SPLITS_GENERATION_ERROR, cause);
        }
        synchronized (this) {
            if (closed) {
                throw new TrinoException(REMOTE_SPLITS_GENERATION_ERROR, "Remote splits source was closed during enumeration");
            }
            if (!splitSource.isFinished()) {
                // Refresh the reservation to the source's current usage; a reservation the pool
                // cannot cover right now pauses the next fetch — see poll(). A per-node limit
                // breach (ExceededMemoryLimitException) propagates with its own code: unlike at
                // creation, splits were already handed out, so the coordinator cannot retry this
                // task on another worker, and the retryable code would promise exactly that.
                memoryReservation = toCompletableFuture(memoryContext.setBytes(splitSource.getMemoryUsage()));
                return result;
            }
            finished = true;
        }
        releaseSource(true);
        return result;
    }

    boolean isFinished()
    {
        return finished;
    }

    synchronized Metrics getMetrics()
    {
        checkState(finished, "getMetrics() called before split source is finished");
        return splitSource.getMetrics();
    }

    long requestedDynamicFilterWaitTimeoutMillis()
    {
        return requestedDynamicFilterWaitTimeoutMillis;
    }

    /**
     * Marks the source as recently used. The coordinator heartbeats running tasks between polls,
     * so {@link #isStale()} indicates a dead or disconnected coordinator rather than a slow consumer.
     */
    void touch()
    {
        lastAccessNanos = System.nanoTime();
    }

    @VisibleForTesting
    void expireForTesting()
    {
        lastAccessNanos = System.nanoTime() - STALE_TIMEOUT_NANOS - 1;
    }

    boolean isStale()
    {
        return System.nanoTime() - lastAccessNanos > STALE_TIMEOUT_NANOS;
    }

    void close()
    {
        if (transitionToClosed()) {
            // after natural exhaustion the batch that exhausted the source has already closed it
            releaseSource(!finished);
        }
        CompletableFuture<List<ConnectorSplit>> batch;
        synchronized (this) {
            batch = currentBatch;
        }
        if (batch != null) {
            batch.cancel(false);
        }
    }

    /**
     * Flips the terminal flag under the monitor and reports whether this caller won the transition
     * and must release the source. The release itself runs outside the monitor — see
     * {@link #releaseSource}.
     */
    private synchronized boolean transitionToClosed()
    {
        if (closed) {
            return false;
        }
        closed = true;
        return true;
    }

    /**
     * Releases the connector source and the memory reservation. Deliberately called outside the
     * task monitor: polls run on HTTP serving threads and synchronize on this task, so a slow
     * connector close held under the monitor would pin a server thread for its duration. Safe
     * without the monitor because the terminal flags are already flipped when this runs (later
     * polls fail fast without touching the source) and {@code ConnectorSplitSource.close} is
     * required to tolerate concurrent invocation.
     */
    private void releaseSource(boolean closeSplitSource)
    {
        try {
            if (closeSplitSource) {
                splitSource.close();
            }
        }
        finally {
            // a close failure must not leak the memory reservation
            memoryContext.close();
        }
    }
}
