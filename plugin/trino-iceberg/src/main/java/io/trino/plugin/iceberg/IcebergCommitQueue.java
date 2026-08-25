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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import jakarta.annotation.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Serializes catalog commits to the same table on this coordinator so that concurrent writers
 * take turns instead of racing on the catalog compare-and-swap. A commit that waits for its turn
 * starts from freshly refreshed metadata, so it succeeds on the first attempt. Commits fall
 * through to an unserialized commit when the queue is full or the wait times out, so the queue
 * never delays a commit beyond the configured wait.
 */
public class IcebergCommitQueue
{
    // bounds waiters per table; commits beyond the bound run without serialization
    private static final int MAX_QUEUE_SIZE = 1000;

    private final Duration maxWait;
    private final int maxQueueSize;
    private final ConcurrentHashMap<SchemaTableName, TableQueue> queues = new ConcurrentHashMap<>();

    @Inject
    public IcebergCommitQueue(IcebergConfig config)
    {
        this(config.getCommitQueueMaxWait(), MAX_QUEUE_SIZE);
    }

    @VisibleForTesting
    IcebergCommitQueue(Duration maxWait, int maxQueueSize)
    {
        this.maxWait = requireNonNull(maxWait, "maxWait is null");
        this.maxQueueSize = maxQueueSize;
    }

    public void commitSerially(SchemaTableName table, Runnable commitAction)
    {
        TableQueue queue = register(table);
        if (queue == null) {
            // queue is full, proceed without serialization
            commitAction.run();
            return;
        }

        try {
            if (!acquire(queue.semaphore)) {
                // wait timed out, proceed without serialization
                commitAction.run();
                return;
            }
            try {
                commitAction.run();
            }
            finally {
                queue.semaphore.release();
            }
        }
        finally {
            unregister(table);
        }
    }

    private boolean acquire(Semaphore semaphore)
    {
        try {
            return semaphore.tryAcquire(maxWait.toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Interrupted while waiting to commit", e);
        }
    }

    @Nullable
    private TableQueue register(SchemaTableName table)
    {
        TableQueue[] registered = new TableQueue[1];
        queues.compute(table, (_, queue) -> {
            if (queue == null) {
                queue = new TableQueue();
            }
            if (queue.participants < maxQueueSize) {
                queue.participants++;
                registered[0] = queue;
            }
            return queue;
        });
        return registered[0];
    }

    private void unregister(SchemaTableName table)
    {
        queues.compute(table, (_, queue) -> {
            verifyNotNull(queue, "No queue for %s", table);
            queue.participants--;
            if (queue.participants == 0) {
                return null;
            }
            return queue;
        });
    }

    @VisibleForTesting
    int activeTableCount()
    {
        return queues.size();
    }

    private static class TableQueue
    {
        // fair, so commits proceed in arrival order
        private final Semaphore semaphore = new Semaphore(1, true);
        // includes the committing thread and all waiters; guarded by the queues map entry
        private int participants;
    }
}
