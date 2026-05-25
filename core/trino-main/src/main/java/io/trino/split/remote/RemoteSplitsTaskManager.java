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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.NodeMemoryConfig;
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.metrics.Metrics;
import io.trino.split.ForRemoteSplitsTask;
import io.trino.tracing.TrinoAttributes;

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.resourceOvercommit;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_QUEUE_FULL;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Server-side worker manager for remote split tasks. A coordinator POSTs a {@link CreateRemoteSplitsTaskRequest}
 * to register a task, POSTs batches via {@link #fetchNextBatch}, and DELETEs (via {@link #close}) on
 * completion. Each task is held as a {@link RemoteSplitsTask}. A scheduled job reaps idle entries
 * every minute; the idle threshold lives in {@link RemoteSplitsTask}. The coordinator
 * heartbeats running tasks between fetches, so an idle task indicates a dead or disconnected
 * coordinator rather than a slow consumer.
 */
public class RemoteSplitsTaskManager
{
    private static final Logger log = Logger.get(RemoteSplitsTaskManager.class);
    private static final long CLEANUP_INTERVAL_MINUTES = 1;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    // synthetic stage for split-source memory reservations; outside any real plan fragment,
    // only the query id part of the task id matters for attribution
    private static final int SPLIT_SOURCE_STAGE_ID = Integer.MAX_VALUE;

    private final CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider;
    private final ConnectorServicesProvider connectorServicesProvider;
    private final Tracer tracer;
    private final SessionPropertyManager sessionPropertyManager;
    private final SqlTaskManager sqlTaskManager;
    private final long queryMaxMemoryPerNode;
    private final ExecutorService taskCreationExecutor;
    private final AtomicInteger splitSourceTaskIds = new AtomicInteger();

    private final Map<String, RemoteSplitsTask> tasks = new ConcurrentHashMap<>();

    @Inject
    public RemoteSplitsTaskManager(
            CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider,
            ConnectorServicesProvider connectorServicesProvider,
            Tracer tracer,
            SessionPropertyManager sessionPropertyManager,
            SqlTaskManager sqlTaskManager,
            NodeMemoryConfig nodeMemoryConfig,
            @ForRemoteSplitsTask ExecutorService taskCreationExecutor,
            @ForRemoteSplitsTask ScheduledExecutorService cleanupExecutor)
    {
        this.splitManagerProvider = requireNonNull(splitManagerProvider, "splitManagerProvider is null");
        this.connectorServicesProvider = requireNonNull(connectorServicesProvider, "connectorServicesProvider is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.queryMaxMemoryPerNode = nodeMemoryConfig.getMaxQueryMemoryPerNode().toBytes();
        this.taskCreationExecutor = requireNonNull(taskCreationExecutor, "taskCreationExecutor is null");
        cleanupExecutor.scheduleWithFixedDelay(
                this::cleanupExpired,
                CLEANUP_INTERVAL_MINUTES,
                CLEANUP_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    /**
     * Registers a task on the {@link ForRemoteSplitsTask} executor, off the HTTP serving threads.
     * The future carries the connector's requested dynamic-filter wait timeout for the coordinator.
     * A saturated creation executor (or one draining for shutdown) rejects the task with
     * {@link io.trino.spi.StandardErrorCode#REMOTE_SPLITS_TASK_QUEUE_FULL} — a worker-local,
     * transient condition the coordinator answers by retrying on another worker.
     */
    public CompletableFuture<Long> runSplitsTaskAsync(CreateRemoteSplitsTaskRequest request)
    {
        try {
            return supplyAsync(() -> runSplitsTask(request), taskCreationExecutor);
        }
        catch (RejectedExecutionException e) {
            return failedFuture(new TrinoException(
                    REMOTE_SPLITS_TASK_QUEUE_FULL,
                    "Remote splits task creation queue is full on this worker",
                    e));
        }
    }

    /**
     * Registers a new remote split task. Creates a {@link ConnectorSplitSource} from the local
     * catalog's split manager, wraps it in a {@link RemoteSplitsTask}, and stores it under
     * {@code request.taskId()}.
     * <p>
     * If a task with the same id is already registered (e.g. the coordinator retried a POST whose
     * response was lost and happened to land on the same worker), the newly created source is
     * discarded and the existing task continues unaffected.
     *
     * @return the connector's requested dynamic-filter wait timeout in milliseconds
     */
    public long runSplitsTask(CreateRemoteSplitsTaskRequest request)
    {
        RemoteSplitsTask existing = tasks.get(request.taskId());
        if (existing != null) {
            return existing.requestedDynamicFilterWaitTimeoutMillis();
        }
        ConnectorTableHandle handle = request.handle();
        try (var ignore = scopedSpan(tracer.spanBuilder("RemoteSplitsTaskManager.runSplitsTask")
                .setParent(Context.current().with(request.span()))
                .setAttribute(TrinoAttributes.TABLE, handle.toString())
                .startSpan())) {
            ensureCatalogLoaded(request);
            RemoteSplitsTask task = createTask(request);
            if (tasks.putIfAbsent(request.taskId(), task) != null) {
                task.close();
            }
            return task.requestedDynamicFilterWaitTimeoutMillis();
        }
    }

    /**
     * Long-polls the next batch for {@code taskId}. If the batch is not ready within {@link #POLL_TIMEOUT},
     * returns a {@code noMoreResults=false} response with no splits so the coordinator polls again; the
     * in-flight generation keeps running and the next poll re-attaches to it. The last batch remains
     * registered for replay until the coordinator acknowledges it with DELETE; stale cleanup handles a
     * lost acknowledgement.
     */
    public CompletableFuture<RemoteSplitsTaskResponse> fetchNextBatch(String taskId, GetRemoteSplitsTaskRequest request)
    {
        RemoteSplitsTask task = getAndTouchTask(taskId);
        if (task == null) {
            return failedFuture(new NoSuchElementException("Task not found: " + taskId));
        }
        try {
            return task.poll(request.token(), request.maxBatchSize(), request.dynamicFilterSnapshot(), POLL_TIMEOUT)
                    .thenApply(maybeSplits -> {
                        if (maybeSplits.isEmpty()) {
                            return RemoteSplitsTaskResponse.notReady(request.token());
                        }
                        boolean finished = task.isFinished();
                        Metrics metrics = finished ? task.getMetrics() : null;
                        return new RemoteSplitsTaskResponse(request.token(), request.token() + 1, maybeSplits.get(), finished, metrics, null, null);
                    })
                    .whenComplete((_, error) -> {
                        if (error != null) {
                            close(taskId);
                        }
                    });
        }
        catch (Throwable error) {
            close(taskId);
            return failedFuture(error);
        }
    }

    /**
     * Marks the task as recently used so the stale reaper keeps it alive between fetches.
     * Uses {@link ConcurrentHashMap#computeIfPresent} so the touch is atomic with respect to
     * {@link #cleanupExpired}'s stale check on the same task id — without it, the reaper could
     * observe the task as stale, have this method refresh it right after, and still remove it,
     * leaving the coordinator believing a since-deleted task is alive.
     *
     * @return whether a task with the given id is registered
     */
    public boolean touch(String taskId)
    {
        return getAndTouchTask(taskId) != null;
    }

    /**
     * Removes the task and closes its underlying split source. Idempotent — a no-op if no task
     * with the given id is registered.
     */
    void close(String taskId)
    {
        RemoteSplitsTask task = tasks.remove(taskId);
        if (task != null) {
            closeQuietly(taskId, task);
        }
    }

    private void closeQuietly(String taskId, RemoteSplitsTask task)
    {
        try {
            task.close();
        }
        catch (Throwable t) {
            // A close failure must not mask the error that triggered it.
            log.warn(t, "Failed to close remote splits task %s", taskId);
        }
    }

    private void ensureCatalogLoaded(CreateRemoteSplitsTaskRequest request)
    {
        if (!request.catalogHandle().getType().isInternal()) {
            request.catalogProperties().ifPresent(
                    properties -> connectorServicesProvider.ensureCatalogsLoaded(ImmutableList.of(properties)));
        }
    }

    private RemoteSplitsTask createTask(CreateRemoteSplitsTaskRequest request)
    {
        CatalogHandle catalogHandle = request.catalogHandle();
        Session session = request.session().toSession(sessionPropertyManager);
        ConnectorSplitSource splitSource = splitManagerProvider.getService(catalogHandle).getSplits(
                request.transaction(),
                session.toConnectorSession(catalogHandle),
                request.handle(),
                request.dynamicFilterColumns(),
                request.constraint());
        LocalMemoryContext memoryContext = createMemoryContext(session);
        try {
            return new RemoteSplitsTask(splitSource, memoryContext, maxMemoryPerNode(session));
        }
        catch (Throwable t) {
            try {
                splitSource.close();
            }
            catch (Throwable closeFailure) {
                // A close failure here must not replace the original construction failure.
                t.addSuppressed(closeFailure);
            }
            try {
                memoryContext.close();
            }
            catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
            throw t;
        }
    }

    /**
     * Creates the memory context a remote split task reserves enumeration memory against. Backed by
     * the query's lazily created worker {@link QueryContext} — the same node-level user memory tasks
     * reserve from, so pool attribution, per-node limit enforcement, and memory-killer visibility all
     * apply. The reference held by the registered task keeps the weak-valued context cached until the
     * task closes.
     */
    private LocalMemoryContext createMemoryContext(Session session)
    {
        QueryContext queryContext = sqlTaskManager.getQueryContext(session.getQueryId());
        if (!queryContext.isMemoryLimitsInitialized()) {
            // mirrors SqlTaskManager.updateTask: fault-tolerant queries are limited by the memory pool
            // and the low-memory killer alone; session properties may only lower the per-node limit
            if (getRetryPolicy(session) == TASK) {
                queryContext.initializeMemoryLimits(false, /* unlimited */ Long.MAX_VALUE);
            }
            else {
                queryContext.initializeMemoryLimits(resourceOvercommit(session), maxMemoryPerNode(session));
            }
        }
        TaskId taskId = new TaskId(new StageId(session.getQueryId(), SPLIT_SOURCE_STAGE_ID), splitSourceTaskIds.incrementAndGet(), 0);
        return queryContext.addSplitSourceMemoryContext(taskId, RemoteSplitsTask.class.getSimpleName());
    }

    /**
     * The per-node user memory limit this query's reservations are checked against, used by
     * {@link RemoteSplitsTask} to tell a deterministic rejection (the estimate alone can never fit
     * under this uniform, configured limit) from a node-local one. Fault-tolerant and
     * resource-overcommit queries are bounded by the memory pool alone.
     */
    private long maxMemoryPerNode(Session session)
    {
        if (getRetryPolicy(session) == TASK || resourceOvercommit(session)) {
            return Long.MAX_VALUE;
        }
        return min(getQueryMaxMemoryPerNode(session).toBytes(), queryMaxMemoryPerNode);
    }

    @VisibleForTesting
    void cleanupExpired()
    {
        // computeIfPresent (rather than a plain check-then-close(id)) makes the stale
        // check atomic with touch(), so a concurrent touch can't be silently overridden below
        tasks.keySet().forEach(id -> tasks.computeIfPresent(id, (taskId, task) -> {
            if (task.isStale()) {
                closeQuietly(taskId, task);
                return null;
            }
            return task;
        }));
    }

    private RemoteSplitsTask getAndTouchTask(String taskId)
    {
        return tasks.computeIfPresent(taskId, (_, task) -> {
            task.touch();
            return task;
        });
    }

    @VisibleForTesting
    RemoteSplitsTask getTaskForTesting(String taskId)
    {
        return tasks.get(taskId);
    }
}
