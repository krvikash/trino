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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoTransportException;
import io.trino.spi.block.Block;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_QUEUE_FULL;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRemoteSplitsSource
{
    private static final URI WORKER_URI = URI.create("http://worker.invalid:8080");
    private static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(new CatalogName("test"), new CatalogVersion("test"));
    private static final JsonCodec<CreateRemoteSplitsTaskRequest> CREATE_REQUEST_CODEC = new JsonCodecFactory(
            new JsonMapperProvider()
                    .withJsonSerializers(Map.of(
                            Span.class, new SpanSerializer(OpenTelemetry.noop()),
                            Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
                    .get())
            .jsonCodec(CreateRemoteSplitsTaskRequest.class);
    private static final JsonCodec<CreateRemoteSplitsTaskResponse> CREATE_RESPONSE_CODEC = jsonCodec(CreateRemoteSplitsTaskResponse.class);
    private static final JsonCodec<GetRemoteSplitsTaskRequest> GET_REQUEST_CODEC = jsonCodec(GetRemoteSplitsTaskRequest.class);
    private static final JsonCodec<RemoteSplitsTaskResponse> RESPONSE_CODEC = jsonCodec(RemoteSplitsTaskResponse.class);

    @Test
    void testTransportFailureRetriesSameTokenAndAdvancesAfterSuccess()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addTransportFailure();
        processor.addResponse(HttpStatus.OK, response(0, 1, false));
        processor.addResponse(HttpStatus.OK, response(1, 2, true));

        withSource(processor, source -> {
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.isFinished()).isFalse();
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.isFinished()).isTrue();

            assertThat(processor.tokens()).containsExactly(0L, 0L, 1L);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testMetricsCountFetchActivity()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addTransportFailure();
        processor.addResponse(HttpStatus.OK, response(0, 0, false));
        processor.addResponse(HttpStatus.OK, response(0, 1, false));
        processor.addResponse(HttpStatus.OK, response(1, 2, true));

        withSource(processor, source -> {
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.isFinished()).isTrue();
            assertThat(processor.tokens()).containsExactly(0L, 0L, 0L, 1L);

            Map<String, Metric<?>> metrics = source.getMetrics().getMetrics();
            assertThat(metrics.get("remoteSplitsSource.taskCreateAttempts")).isEqualTo(new LongCount(1));
            assertThat(metrics.get("remoteSplitsSource.batchesFetched")).isEqualTo(new LongCount(2));
            assertThat(metrics.get("remoteSplitsSource.splitsFetched")).isEqualTo(new LongCount(0));
            assertThat(metrics.get("remoteSplitsSource.notReadyPolls")).isEqualTo(new LongCount(1));
            assertThat(metrics.get("remoteSplitsSource.fetchRetries")).isEqualTo(new LongCount(1));
            assertThat(((LongCount) metrics.get("remoteSplitsSource.responseBytes")).getTotal()).isPositive();
        });
    }

    @Test
    void testServiceUnavailableIsRetried()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addResponse(HttpStatus.SERVICE_UNAVAILABLE, null);
        processor.addResponse(HttpStatus.OK, response(0, 1, true));

        withSource(processor, source -> {
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(processor.tokens()).containsExactly(0L, 0L);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testPermanentWorkerFailureIsNotRetried()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                RemoteSplitsTaskResponse.fail(0, new TrinoException(PERMISSION_DENIED, "no access", new RuntimeException("root cause"))));

        withSource(processor, source -> {
            assertThatThrownBy(() -> source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(TrinoException.class)
                    .extracting(cause -> ((TrinoException) cause).getErrorCode())
                    .isEqualTo(PERMISSION_DENIED.toErrorCode());
            assertThat(processor.tokens()).containsExactly(0L);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testRetryExhaustionFailsAndDeletesTask()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addTransportFailure();
        processor.addTransportFailure();
        processor.addTransportFailure();

        withSource(processor, source -> {
            assertThatThrownBy(() -> source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(TrinoTransportException.class);
            assertThat(processor.tokens()).containsExactly(0L, 0L, 0L);
            assertThat(source.isFinished()).isTrue();
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testTaskCreationUserErrorFailsFastWithOriginalErrorCode()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addCreateFailure(CreateRemoteSplitsTaskResponse.fail(new TrinoException(PERMISSION_DENIED, "no access")));

        withSource(processor, source -> {
            assertThatThrownBy(() -> source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("no access")
                    .extracting(cause -> ((TrinoException) cause).getErrorCode())
                    .isEqualTo(PERMISSION_DENIED.toErrorCode());
            assertThat(processor.createRequests()).isEqualTo(1);
            assertThat(source.isFinished()).isTrue();
        });
    }

    @Test
    void testTaskCreationOverLocalMemoryLimitFailsFast()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addCreateFailure(CreateRemoteSplitsTaskResponse.fail(new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "over per-node limit")));

        withSource(processor, source -> {
            assertThatThrownBy(() -> source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(TrinoException.class)
                    .extracting(cause -> ((TrinoException) cause).getErrorCode())
                    .isEqualTo(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode());
            assertThat(processor.createRequests()).isEqualTo(1);
            assertThat(source.isFinished()).isTrue();
        });
    }

    @Test
    void testTaskCreationFullMemoryPoolIsRetriedOnAnotherWorker()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addCreateFailure(CreateRemoteSplitsTaskResponse.fail(new TrinoException(REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE, "memory pool is full")));
        processor.addResponse(HttpStatus.OK, response(0, 1, true));

        withSource(processor, source -> {
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.isFinished()).isTrue();
            assertThat(processor.createRequests()).isEqualTo(2);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testTaskCreationQueueFullIsRetriedOnAnotherWorker()
    {
        TestingProcessor processor = new TestingProcessor();
        processor.addCreateFailure(CreateRemoteSplitsTaskResponse.fail(new TrinoException(REMOTE_SPLITS_TASK_QUEUE_FULL, "creation queue is full")));
        processor.addResponse(HttpStatus.OK, response(0, 1, true));

        withSource(processor, source -> {
            assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join()).isEmpty();
            assertThat(source.isFinished()).isTrue();
            assertThat(processor.createRequests()).isEqualTo(2);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testHeartbeatSchedulingFailureDoesNotRetryCreatedTask()
    {
        TestingProcessor processor = new TestingProcessor();
        ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor();
        scheduledExecutor.shutdownNow();

        withSource(processor, scheduledExecutor, source -> {
            assertThatThrownBy(() -> source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join())
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(source.isFinished()).isTrue();
            assertThat(processor.createRequests()).isEqualTo(1);
            assertEventually(() -> assertThat(processor.deleteRequests()).isEqualTo(1));
        });
    }

    @Test
    void testCloseRunningTaskDeletesTaskOnce()
            throws Exception
    {
        TestingProcessor processor = new TestingProcessor();
        ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor();
        try (TestingHttpClient httpClient = new TestingHttpClient(processor)) {
            RemoteSplitsSource source = createSource(httpClient, scheduledExecutor);
            assertThat(source.getRequestedDynamicFilterWaitTimeoutMillisFuture().get(10, SECONDS)).isZero();

            source.close();
            source.close();

            assertThat(source.isFinished()).isTrue();
            assertThat(processor.deleteRequests()).isEqualTo(1);
        }
        finally {
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    void testCloseWhileTaskCreationInFlightDeletesTaskOnce()
            throws Exception
    {
        CompletableFuture<Void> createStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseCreate = new CompletableFuture<>();
        TestingProcessor processor = new TestingProcessor(() -> {
            createStarted.complete(null);
            releaseCreate.join();
        });
        ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor();
        ScheduledExecutorService httpExecutor = newSingleThreadScheduledExecutor();
        try (TestingHttpClient httpClient = new TestingHttpClient(processor, httpExecutor)) {
            RemoteSplitsSource source = createSource(httpClient, scheduledExecutor);
            createStarted.get(10, SECONDS);

            source.close();
            source.close();
            releaseCreate.complete(null);

            assertThat(source.getRequestedDynamicFilterWaitTimeoutMillisFuture().get(10, SECONDS)).isZero();
            source.close();
            httpExecutor.submit(() -> {}).get(10, SECONDS);
            assertThat(source.isFinished()).isTrue();
            assertThat(processor.createRequests()).isEqualTo(1);
            assertThat(processor.deleteRequests()).isEqualTo(1);
        }
        finally {
            releaseCreate.complete(null);
            httpExecutor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }
    }

    private static RemoteSplitsTaskResponse response(long token, long nextToken, boolean noMoreResults)
    {
        return new RemoteSplitsTaskResponse(token, nextToken, ImmutableList.of(), noMoreResults, noMoreResults ? Metrics.EMPTY : null, null, null);
    }

    private static void withSource(TestingProcessor processor, Consumer<RemoteSplitsSource> test)
    {
        ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor();
        try {
            withSource(processor, scheduledExecutor, test);
        }
        finally {
            scheduledExecutor.shutdownNow();
        }
    }

    private static void withSource(TestingProcessor processor, ScheduledExecutorService scheduledExecutor, Consumer<RemoteSplitsSource> test)
    {
        ScheduledExecutorService httpExecutor = newSingleThreadScheduledExecutor();
        try (TestingHttpClient httpClient = new TestingHttpClient(processor, httpExecutor)) {
            RemoteSplitsSource source = createSource(httpClient, scheduledExecutor);
            try {
                test.accept(source);
            }
            finally {
                source.close();
            }
        }
        finally {
            httpExecutor.shutdownNow();
        }
    }

    private static RemoteSplitsSource createSource(TestingHttpClient httpClient, ScheduledExecutorService scheduledExecutor)
    {
        Session session = testSessionBuilder().build();
        return new RemoteSplitsSource(
                session.toSessionRepresentation(),
                new TableHandle(CATALOG_HANDLE, new TestingTableHandle(), TestingTransactionHandle.create()),
                noopTracer(),
                Span.getInvalid(),
                Optional.empty(),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue(),
                ImmutableList.of(WORKER_URI),
                httpClient,
                scheduledExecutor,
                10,
                new Duration(1, NANOSECONDS),
                CREATE_REQUEST_CODEC,
                CREATE_RESPONSE_CODEC,
                RESPONSE_CODEC,
                GET_REQUEST_CODEC);
    }

    private static final class TestingProcessor
            implements TestingHttpClient.Processor
    {
        private final Deque<Function<GetRemoteSplitsTaskRequest, Response>> results = new ArrayDeque<>();
        private final Deque<Supplier<Response>> createResults = new ArrayDeque<>();
        private final List<Long> tokens = new ArrayList<>();
        private final Runnable beforeCreateResponse;
        private int createRequests;
        private int deleteRequests;

        private TestingProcessor()
        {
            this(() -> {});
        }

        private TestingProcessor(Runnable beforeCreateResponse)
        {
            this.beforeCreateResponse = beforeCreateResponse;
        }

        void addTransportFailure()
        {
            results.add(_ -> {
                throw new RuntimeException("transport failure");
            });
        }

        void addResponse(HttpStatus status, RemoteSplitsTaskResponse response)
        {
            results.add(_ -> jsonResponse(status, response == null ? new byte[0] : RESPONSE_CODEC.toJsonBytes(response)));
        }

        void addCreateFailure(CreateRemoteSplitsTaskResponse response)
        {
            createResults.add(() -> jsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, CREATE_RESPONSE_CODEC.toJsonBytes(response)));
        }

        synchronized List<Long> tokens()
        {
            return ImmutableList.copyOf(tokens);
        }

        synchronized int deleteRequests()
        {
            return deleteRequests;
        }

        synchronized int createRequests()
        {
            return createRequests;
        }

        @Override
        public synchronized Response handle(Request request)
        {
            String path = request.getUri().getPath();
            if (request.getMethod().equals("DELETE")) {
                deleteRequests++;
                return jsonResponse(HttpStatus.OK, new byte[0]);
            }
            if (path.endsWith("/result")) {
                GetRemoteSplitsTaskRequest getRequest = GET_REQUEST_CODEC.fromJson(((StaticBodyGenerator) request.getBodyGenerator()).getBody());
                tokens.add(getRequest.token());
                return results.remove().apply(getRequest);
            }
            if (path.endsWith("/heartbeat")) {
                return jsonResponse(HttpStatus.OK, new byte[0]);
            }
            createRequests++;
            beforeCreateResponse.run();
            if (!createResults.isEmpty()) {
                return createResults.remove().get();
            }
            return jsonResponse(HttpStatus.OK, CREATE_RESPONSE_CODEC.toJsonBytes(CreateRemoteSplitsTaskResponse.forCreatedTask(0)));
        }

        private static TestingResponse jsonResponse(HttpStatus status, byte[] body)
        {
            return new TestingResponse(
                    status,
                    ImmutableListMultimap.of(CONTENT_TYPE, MediaType.JSON_UTF_8.toString()),
                    body);
        }
    }
}
