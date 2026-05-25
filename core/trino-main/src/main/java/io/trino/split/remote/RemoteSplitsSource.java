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
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.SessionRepresentation;
import io.trino.metadata.TableHandle;
import io.trino.server.remotetask.Backoff;
import io.trino.spi.TrinoTransportException;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.metrics.Metrics;
import io.trino.tracing.TrinoAttributes;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.spi.HostAddress.fromUri;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_GENERATION_ERROR;
import static io.trino.split.remote.RemoteSplitsSource.TaskStatus.CREATED;
import static io.trino.split.remote.RemoteSplitsSource.TaskStatus.FINISHED;
import static io.trino.split.remote.RemoteSplitsSource.TaskStatus.RUNNING;
import static io.trino.split.remote.RemoteSplitsTaskFailures.isRetryableTaskCreationFailure;
import static io.trino.split.remote.RemoteSplitsTaskFailures.toException;
import static io.trino.split.remote.RemoteSplitsTaskHttpUtil.closeTaskLocation;
import static io.trino.split.remote.RemoteSplitsTaskHttpUtil.fetchRemoteSplitsTaskLocation;
import static io.trino.split.remote.RemoteSplitsTaskHttpUtil.heartbeatLocation;
import static io.trino.split.remote.RemoteSplitsTaskHttpUtil.remoteSplitsTaskLocation;
import static java.lang.Math.incrementExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link ConnectorSplitSource} that delegates split enumeration to a remote worker via HTTP.
 * Task creation retries across workers, but only for worker-specific failures (transport errors,
 * HTTP 503, transient resource shortages); deterministic failures fail fast — see
 * {@link RemoteSplitsTaskFailures#isRetryableTaskCreationFailure}. After creation, transport
 * failures and HTTP 503 responses retry the same fetch token with backoff.
 *
 * <p>A worker retains each batch under its request token. Repeating that token replays the batch,
 * while the next token acknowledges it. A final batch is acknowledged by deleting the remote task.
 * Heartbeats keep a running task alive while the engine is slow to drain splits.
 *
 * <p>{@link #close()} can race with task creation. Closing a {@link TaskStatus#CREATED} source marks
 * it finished without deleting; if the create request subsequently succeeds, its callback observes
 * the state change and deletes the newly created remote task.
 */
public class RemoteSplitsSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RemoteSplitsSource.class);
    private static final int MAX_RETRIES = 3;
    private static final long HEARTBEAT_INTERVAL_SECONDS = 60;

    enum TaskStatus
    {
        CREATED,
        RUNNING,
        FINISHED,
    }

    private final Tracer tracer;
    private final Span parentSpan;
    private final String tableName;
    private final HttpClient httpClient;
    private final List<URI> workers;
    private final String taskId;
    private final int batchSize;
    private final Backoff fetchBackoff;
    private long nextToken;

    private final AtomicReference<TaskStatus> taskStatus = new AtomicReference<>(CREATED);
    private final AtomicBoolean remoteTaskNeedsDelete = new AtomicBoolean();
    private final byte[] createTaskRequestBody;
    private final JsonCodec<CreateRemoteSplitsTaskResponse> createTaskResponseCodec;
    private final JsonCodec<RemoteSplitsTaskResponse> taskResponseCodec;
    private final JsonCodec<GetRemoteSplitsTaskRequest> getTaskRequestCodec;

    private volatile URI workerUri;
    private final ListenableFuture<Long> startFuture;
    private volatile Optional<Metrics> metrics = Optional.empty();
    private final ScheduledExecutorService scheduledExecutor;
    private volatile ScheduledFuture<?> heartbeatTask;

    private final RemoteSplitsSourceMetrics sourceMetrics = new RemoteSplitsSourceMetrics();
    private final AtomicInteger taskCreateAttempts = new AtomicInteger();

    public RemoteSplitsSource(
            SessionRepresentation session,
            TableHandle table,
            Tracer tracer,
            Span parentSpan,
            Optional<CatalogProperties> catalogProperties,
            DynamicFilter dynamicFilter,
            Constraint constraint,
            List<URI> internalNodes,
            HttpClient httpClient,
            ScheduledExecutorService scheduledExecutor,
            int batchSize,
            Duration maxErrorDuration,
            JsonCodec<CreateRemoteSplitsTaskRequest> taskRequestCodec,
            JsonCodec<CreateRemoteSplitsTaskResponse> createTaskResponseCodec,
            JsonCodec<RemoteSplitsTaskResponse> taskResponseCodec,
            JsonCodec<GetRemoteSplitsTaskRequest> getTaskRequestCodec)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.createTaskResponseCodec = requireNonNull(createTaskResponseCodec, "createTaskResponseCodec is null");
        this.taskResponseCodec = requireNonNull(taskResponseCodec, "taskResponseCodec is null");
        this.getTaskRequestCodec = requireNonNull(getTaskRequestCodec, "getTaskRequestCodec is null");
        this.fetchBackoff = new Backoff(requireNonNull(maxErrorDuration, "maxErrorDuration is null"));
        requireNonNull(session, "session is null");
        requireNonNull(table, "table is null");
        requireNonNull(catalogProperties, "catalogProperties is null");
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        requireNonNull(taskRequestCodec, "taskRequestCodec is null");
        requireNonNull(internalNodes, "internalNodes is null");
        checkArgument(!internalNodes.isEmpty(), "internalNodes is empty");
        checkArgument(batchSize > 0, "batchSize must be greater than 0");

        this.batchSize = batchSize;
        // the caller orders candidates (cache-affinity worker first); retries walk the list cyclically
        this.workers = ImmutableList.copyOf(internalNodes);
        this.taskId = UUID.randomUUID().toString().replace("-", "");
        this.tableName = table.connectorHandle().toString();
        this.createTaskRequestBody = taskRequestCodec.toJsonBytes(new CreateRemoteSplitsTaskRequest(
                taskId,
                session,
                table.transaction(),
                table.connectorHandle(),
                parentSpan,
                table.catalogHandle(),
                catalogProperties,
                dynamicFilter.getColumnsCovered(),
                constraint));
        this.startFuture = startTask(MAX_RETRIES);
    }

    private ListenableFuture<Long> startTask(int retriesLeft)
    {
        sourceMetrics.taskCreateAttempt();
        workerUri = workers.get(taskCreateAttempts.getAndIncrement() % workers.size());
        Request request = preparePost()
                .setUri(remoteSplitsTaskLocation(workerUri))
                .setHeader(CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(createTaskRequestBody))
                .setSpanBuilder(createSpanBuilder("create-remote-splits-task"))
                .build();
        ListenableFuture<Long> attempt = Futures.transform(
                Futures.submitAsync(
                        () -> httpClient.executeAsync(request, createFullJsonResponseHandler(createTaskResponseCodec)),
                        directExecutor()),
                response -> {
                    if (response.getStatusCode() == 200) {
                        remoteTaskNeedsDelete.set(true);
                        if (!taskStatus.compareAndSet(CREATED, RUNNING)) {
                            transitionToFinished();
                        }
                        else {
                            scheduleHeartbeats();
                        }
                        return requireNonNull(response.getValue(), "response value is null")
                                .requestedDynamicFilterWaitTimeoutMillis();
                    }
                    throw toException(response, "Failed to create remote splits task with status code: " + response.getStatusCode());
                },
                directExecutor());

        return Futures.catchingAsync(
                attempt,
                Exception.class,
                exception -> {
                    if (retriesLeft > 0 && taskStatus.get() == CREATED && isRetryableTaskCreationFailure(exception)) {
                        log.warn(exception, "Failed to create remote splits task %s on %s, retrying on another node", taskId, workerUri);
                        return startTask(retriesLeft - 1);
                    }
                    transitionToFinished();
                    return Futures.immediateFailedFuture(exception);
                },
                directExecutor());
    }

    /**
     * Heartbeats keep the worker task alive while the engine is legitimately slow to drain the
     * split source (full split queues, slow downstream stages), so the worker's stale reaper
     * only fires when this coordinator is gone. The interval must stay well below the stale
     * timeout in {@link RemoteSplitsTask}.
     */
    private void scheduleHeartbeats()
    {
        heartbeatTask = scheduledExecutor.scheduleWithFixedDelay(this::sendHeartbeat, HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, SECONDS);
        if (isFinished()) {
            stopHeartbeats();
        }
    }

    private void stopHeartbeats()
    {
        ScheduledFuture<?> task = heartbeatTask;
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendHeartbeat()
    {
        if (isFinished()) {
            return;
        }
        sourceMetrics.heartbeatSent();
        sendQuietly(
                preparePost()
                        .setUri(heartbeatLocation(workerUri, taskId))
                        .setSpanBuilder(createSpanBuilder("heartbeat-remote-splits-task"))
                        .build(),
                "Heartbeat",
                throwable -> log.debug(throwable, "Failed to heartbeat remote splits task %s on %s", taskId, workerUri));
    }

    private void sendDeleteAsync()
    {
        sendQuietly(
                prepareDelete()
                        .setUri(closeTaskLocation(workerUri, taskId))
                        .setSpanBuilder(createSpanBuilder("release-remote-splits-task"))
                        .build(),
                "Release",
                throwable -> log.warn(throwable, "Failed to release remote splits task %s on %s", taskId, workerUri));
    }

    /**
     * Fires an async, no-response-body request and logs the outcome, never throws.
     */
    private void sendQuietly(Request request, String operation, Consumer<Throwable> onFailure)
    {
        ListenableFuture<StatusResponse> future = Futures.submitAsync(
                () -> httpClient.executeAsync(request, createStatusResponseHandler()),
                directExecutor());
        addCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(StatusResponse result)
            {
                if (result.getStatusCode() != 200) {
                    log.debug("%s for remote splits task %s on %s returned status %s", operation, taskId, workerUri, result.getStatusCode());
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                onFailure.accept(throwable);
            }
        }, directExecutor());
    }

    private ListenableFuture<List<ConnectorSplit>> fetchBatch(GetRemoteSplitsTaskRequest getRequest)
    {
        ListenableFuture<List<ConnectorSplit>> batch = Futures.transform(
                Futures.submitAsync(() -> fetchResponseWithRetry(getRequest), directExecutor()),
                response -> toSplitBatch(getRequest, response),
                directExecutor());
        return Futures.catchingAsync(
                batch,
                Exception.class,
                exception -> {
                    transitionToFinished();
                    return Futures.immediateFailedFuture(exception);
                },
                directExecutor());
    }

    private ListenableFuture<FullJsonResponseHandler.JsonResponse<RemoteSplitsTaskResponse>> fetchResponseWithRetry(GetRemoteSplitsTaskRequest getRequest)
    {
        if (isFinished()) {
            return Futures.immediateFailedFuture(new IllegalStateException("Task is already done"));
        }
        Request request = preparePost()
                .setUri(fetchRemoteSplitsTaskLocation(workerUri, taskId))
                .setHeader(CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(getTaskRequestCodec.toJsonBytes(getRequest)))
                .setSpanBuilder(createSpanBuilder("fetch-remote-splits-task"))
                .build();
        fetchBackoff.startRequest();
        ListenableFuture<FullJsonResponseHandler.JsonResponse<RemoteSplitsTaskResponse>> response = Futures.submitAsync(
                () -> httpClient.executeAsync(request, createFullJsonResponseHandler(taskResponseCodec)),
                directExecutor());
        ListenableFuture<FullJsonResponseHandler.JsonResponse<RemoteSplitsTaskResponse>> transportRetried = Futures.catchingAsync(
                response,
                Exception.class,
                exception -> scheduleFetchRetry(getRequest, exception),
                directExecutor());
        return Futures.transformAsync(
                transportRetried,
                result -> {
                    if (result.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                        return scheduleFetchRetry(getRequest, new RuntimeException("Remote splits task returned HTTP 503"));
                    }
                    fetchBackoff.success();
                    return Futures.immediateFuture(result);
                },
                directExecutor());
    }

    private ListenableFuture<FullJsonResponseHandler.JsonResponse<RemoteSplitsTaskResponse>> scheduleFetchRetry(
            GetRemoteSplitsTaskRequest getRequest,
            Exception failure)
    {
        if (isFinished()) {
            return Futures.immediateFailedFuture(failure);
        }
        if (fetchBackoff.failure()) {
            return Futures.immediateFailedFuture(new TrinoTransportException(
                    REMOTE_SPLITS_GENERATION_ERROR,
                    fromUri(workerUri),
                    format("Failed to fetch remote splits task %s from %s after %s attempts", taskId, workerUri, fetchBackoff.getFailureCount()),
                    failure));
        }
        sourceMetrics.fetchRetry();
        log.warn(failure, "Failed to fetch remote splits task %s from %s, retrying", taskId, workerUri);
        return Futures.scheduleAsync(
                () -> fetchResponseWithRetry(getRequest),
                fetchBackoff.getBackoffDelayNanos(),
                NANOSECONDS,
                scheduledExecutor);
    }

    private List<ConnectorSplit> toSplitBatch(
            GetRemoteSplitsTaskRequest request,
            FullJsonResponseHandler.JsonResponse<RemoteSplitsTaskResponse> response)
    {
        requireNonNull(response, "response is null");
        sourceMetrics.responseReceived(response.getResponseSize());
        if (response.getStatusCode() != 200) {
            throw toException(response, "Failed to fetch remote splits task result with status code: " + response.getStatusCode());
        }
        RemoteSplitsTaskResponse taskResponse = requireNonNull(response.getValue(), "response value is null");
        checkState(taskResponse.token() == request.token(), "Expected token %s, but received %s", request.token(), taskResponse.token());
        long expectedNextToken = incrementExact(request.token());
        checkState(
                taskResponse.nextToken() == request.token() || taskResponse.nextToken() == expectedNextToken,
                "Expected next token %s or %s, but received %s",
                request.token(),
                expectedNextToken,
                taskResponse.nextToken());
        if (taskResponse.nextToken() == request.token()) {
            checkState(taskResponse.splits().isEmpty() && !taskResponse.noMoreResults(), "A non-advancing response must be empty and non-final");
            sourceMetrics.notReadyPoll();
            return ImmutableList.of();
        }
        nextToken = expectedNextToken;
        sourceMetrics.batchFetched(taskResponse.splits().size());
        if (taskResponse.noMoreResults()) {
            // publish before FINISHED so getMetrics() observing isFinished() sees it
            metrics = Optional.ofNullable(taskResponse.metrics());
            transitionToFinished();
        }
        return ImmutableList.copyOf(taskResponse.splits());
    }

    private void transitionToFinished()
    {
        if (taskStatus.getAndSet(FINISHED) == RUNNING) {
            stopHeartbeats();
        }
        if (remoteTaskNeedsDelete.getAndSet(false)) {
            sendDeleteAsync();
        }
    }

    @Override
    public void close()
    {
        transitionToFinished();
    }

    private SpanBuilder createSpanBuilder(String name)
    {
        return tracer.spanBuilder(name)
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.TABLE, tableName);
    }

    /**
     * Deliberately ignores the engine's {@code maxSize} and always requests a fixed {@code batchSize}
     * from the worker, relaxing the {@link ConnectorSplitSource#getNextBatch} contract. Larger batches
     * amortize the per-batch network round-trip; the engine's {@code BufferingSplitSource} and
     * {@code SourcePartitionedScheduler} absorb the oversized batch downstream.
     */
    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        GetRemoteSplitsTaskRequest getRequest = new GetRemoteSplitsTaskRequest(nextToken, batchSize, dynamicFilterSnapshot);
        return toCompletableFuture(Futures.transformAsync(startFuture, _ -> fetchBatch(getRequest), directExecutor()));
    }

    @Override
    public boolean isFinished()
    {
        return taskStatus.get() == FINISHED;
    }

    /**
     * The dynamic-filter wait timeout arrives with the create-task response. Exposed as the create
     * future so the engine can chain on it instead of blocking a scheduler thread.
     */
    public ListenableFuture<Long> getRequestedDynamicFilterWaitTimeoutMillisFuture()
    {
        return startFuture;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        // TODO: support optimize
        return Optional.empty();
    }

    @Override
    public Metrics getMetrics()
    {
        return metrics.orElse(Metrics.EMPTY).mergeWith(sourceMetrics.snapshot());
    }
}
