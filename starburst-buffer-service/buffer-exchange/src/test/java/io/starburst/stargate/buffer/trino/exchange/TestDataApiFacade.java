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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import dev.failsafe.CircuitBreakerOpenException;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacade.RetryExecutorConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataApiFacade
{
    private static final String EXCHANGE_0 = "exchange-0";

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);

    @AfterAll
    public void teardown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testRetriesWithDelay()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new RuntimeException("random exception")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));

        ListenableFuture<ChunkList> future = dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty());

        Thread.sleep(100);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(1);
        Thread.sleep(550);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(2);
        Thread.sleep(1000);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(3);
        assertThat(future).isDone();
    }

    @Test
    public void testRetriesOnRuntimeException()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new RuntimeException("random exception")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(5, SECONDS)
                .isEqualTo(result);

        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty()))
                .isEqualTo(3);
    }

    @Test
    public void testRetriesOnInternalErrorException()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(5, SECONDS)
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(value = ErrorCode.class)
    public void testDoNotRetryOnMostDataApiExceptions(ErrorCode errorCode)
    {
        if (errorCode == ErrorCode.INTERNAL_ERROR || errorCode == ErrorCode.BUFFER_NODE_NOT_FOUND || errorCode == ErrorCode.OVERLOADED) {
            return; // skip
        }

        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(errorCode, "blah")));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(errorCode));

        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(1);
    }

    @Test
    public void testAddDataPagesRetries()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        // OK after INTERNAL_ERROR
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(2);

        // Immediate DRAINING
        dataApiDelegate.recordAddDataPages("exchange-2", 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "blah")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-2", 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINING));
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-2", 0, 0, 0)).isEqualTo(1);

        // DRAINING after INTERNAL_ERROR
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "blah")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-3", 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINING_ON_RETRY))
                .withMessageContaining("Received DRAINING error code on retry");
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-3", 0, 0, 0)).isEqualTo(2);
    }

    @Test
    public void testDoNotRetryOnSuccess()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(100, MILLISECONDS) // returns immediately
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(1);
    }

    @Test
    public void testAddDataPagesRequestCancellation()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        // test cancelling an in-progress request doesn't trigger retry
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0L, listeningDecorator(executor).submit(() -> {
            sleepUninterruptibly(java.time.Duration.ofMillis(500));
            return Optional.empty();
        }));
        ListenableFuture<Void> addDataPagesFuture0 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0L, ImmutableListMultimap.of());
        Thread.sleep(100); // wait for addDataPages job above to start execution
        addDataPagesFuture0.cancel(true);
        assertThatThrownBy(() -> getFutureValue(addDataPagesFuture0)).isInstanceOf(CancellationException.class);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0L)).isEqualTo(1);

        // test cancelling after request failure will cancel the next retry
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 1, 1, 1L, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ListenableFuture<Void> addDataPagesFuture1 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 1, 1, 1L, ImmutableListMultimap.of());
        sleepUninterruptibly(java.time.Duration.ofMillis(500));
        addDataPagesFuture1.cancel(true);
        sleepUninterruptibly(java.time.Duration.ofMillis(1000));
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 1, 1, 1L)).isEqualTo(1);
        assertThatThrownBy(() -> getFutureValue(addDataPagesFuture1)).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testShortCircuitResponseIfNodeDrained()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.DRAINED,
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")));

        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()));
        // we expect no communication with client
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(0);

        // check other calls too
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.pingExchange(TestingDataApi.NODE_ID, EXCHANGE_0));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.finishExchange(TestingDataApi.NODE_ID, EXCHANGE_0));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.removeExchange(TestingDataApi.NODE_ID, EXCHANGE_0));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.getChunkData(TestingDataApi.NODE_ID, EXCHANGE_0, 1, 1L, 1L));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.registerExchange(TestingDataApi.NODE_ID, EXCHANGE_0, STANDARD));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 1, 1, 1L, ImmutableListMultimap.of()));
    }

    @Test
    public void testCircuitBreaker()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")),
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new RuntimeException("unexpected exception")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(2);

        // on immediate call we should still get CircuitBreakerOpenException
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // and number of calls should not change
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(2);

        Thread.sleep(600); // we are in HALF_OPEN state now
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // one more "try" request should be sent and OPEN state should be prolonged
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(3);

        // on immediate call we should still get CircuitBreakerOpenException
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // and number of calls should not change
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(3);

        Thread.sleep(600); // we are in HALF_OPEN state now again
        // and next request should be successful
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(1, SECONDS)
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(4);

        // on next call we should get one retry but request should succeed
        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(1, SECONDS)
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(6);
    }

    @ParameterizedTest
    @EnumSource(value = ErrorCode.class)
    public void testNoCircuitBreakerOnMostDataApiExceptions(ErrorCode errorCode)
    {
        if (errorCode == ErrorCode.INTERNAL_ERROR || errorCode == ErrorCode.OVERLOADED) {
            return; // skip
        }

        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")));

        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(errorCode, "chunk")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(errorCode, "chunk")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(errorCode, "chunk")));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFailedFuture(new DataApiException(errorCode, "chunk")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        dataApiDelegate.recordListClosedChunks(EXCHANGE_0, OptionalLong.empty(), immediateFuture(result));

        for (int i = 0; i < 4; ++i) {
            assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                    .failsWithin(1, SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(errorCode));
        }
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(4);

        assertThat(dataApiFacade.listClosedChunks(TestingDataApi.NODE_ID, EXCHANGE_0, OptionalLong.empty()))
                .succeedsWithin(1, SECONDS)
                .isEqualTo(result);
        assertThat(dataApiDelegate.getListClosedChunksCallCount(EXCHANGE_0, OptionalLong.empty())).isEqualTo(5);
    }

    @Test
    public void testCircuitBreakerForAddDataPages()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")));

        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new RuntimeException("unexpected exception")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));

        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(2);

        // on immediate call we should still get CircuitBreakerOpenException
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // and number of calls should not change
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(2);

        Thread.sleep(600); // we are in HALF_OPEN state now
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // one more "try" request should be sent and OPEN state should be prolonged
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(3);

        // on immediate call we should still get CircuitBreakerOpenException
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CircuitBreakerOpenException.class);
        // and number of calls should not change
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(3);

        Thread.sleep(600); // we are in HALF_OPEN state now again
        // and next request should be successful
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(1, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(4);

        // on next call we should get one retry but request should succeed
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(1, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(6);
    }

    @Test
    public void testAddDataPagesStats()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                // misconfigured on purpose
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 2, 1, Duration.valueOf("500ms")));

        DataApiFacadeStats.AddDataPagesOperationStats stats = dataApiFacade.getStats().getAddDataPagesOperationStats();
        assertThat(stats.getSuccessfulRequestTime().getAllTime().getCount()).isEqualTo(0.0);
        assertThat(stats.getFailedRequestTime().getAllTime().getCount()).isEqualTo(0.0);
        assertThat(stats.getOverloadedRequestErrorCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getCircuitBreakerOpenRequestErrorCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getAnyRequestErrorCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getFailedOperationCount().getTotalCount()).isEqualTo(0);

        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.OVERLOADED, "overloaded")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new RuntimeException("unexpected exception")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));

        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS);
        Thread.sleep(600);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(1, SECONDS);
        Thread.sleep(600);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(1, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(1, SECONDS);

        assertThat(stats.getSuccessfulRequestTime().getAllTime().getCount()).isEqualTo(2.0);
        assertThat(stats.getFailedRequestTime().getAllTime().getCount()).isEqualTo(4.0);
        assertThat(stats.getOverloadedRequestErrorCount().getTotalCount()).isEqualTo(1);
        assertThat(stats.getCircuitBreakerOpenRequestErrorCount().getTotalCount()).isEqualTo(13);
        assertThat(stats.getAnyRequestErrorCount().getTotalCount()).isEqualTo(17);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(13);
        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(2);
        assertThat(stats.getFailedOperationCount().getTotalCount()).isEqualTo(4);
    }

    private DataApiFacade createDataApiFacade(
            TestingDataApi dataApiDelegate,
            BufferNodeState bufferNodeState,
            RetryExecutorConfig defaultRetryExecutorConfig,
            RetryExecutorConfig addDataPagesRetryExecutorConfig)
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, bufferNodeState));
        TestingApiFactory apiFactory = new TestingApiFactory();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        return new DataApiFacade(
                discoveryManager,
                apiFactory,
                new DataApiFacadeStats(),
                defaultRetryExecutorConfig,
                addDataPagesRetryExecutorConfig,
                executor);
    }

    private static void assertShortCircuitResponseIfNodeDrained(Supplier<ListenableFuture<?>> call)
    {
        assertThat(call.get())
                .failsWithin(100, MILLISECONDS) // returns immediatelly
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINED))
                .withMessageContaining("Node already DRAINED");
    }

    private static class TestingDataApi
            implements DataApi
    {
        public static final int NODE_ID = 1;

        private record ListClosedChunksKey(String exchangeId, OptionalLong pagingId) {}

        private record AddDataPagesKey(String exchangeId, int taskId, int attemptId, long dataPagesId) {}

        private final ListMultimap<ListClosedChunksKey, ListenableFuture<ChunkList>> listClosedChunksResponses = ArrayListMultimap.create();
        private final Map<ListClosedChunksKey, AtomicLong> listClosedChunksCounters = new HashMap<>();
        private final ListMultimap<AddDataPagesKey, ListenableFuture<Optional<RateLimitInfo>>> addDataPagesResponses = ArrayListMultimap.create();
        private final Map<AddDataPagesKey, AtomicLong> addDataPagesCounters = new HashMap<>();

        @Override
        public BufferNodeInfo getInfo()
        {
            return new BufferNodeInfo(NODE_ID, URI.create("http://testing"), Optional.empty(), BufferNodeState.ACTIVE, Instant.now());
        }

        @Override
        public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
        {
            ListClosedChunksKey key = new ListClosedChunksKey(exchangeId, pagingId);
            List<ListenableFuture<ChunkList>> responses = listClosedChunksResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            listClosedChunksCounters.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordListClosedChunks(String exchangeId, OptionalLong pagingId, ListenableFuture<ChunkList> result)
        {
            listClosedChunksResponses.put(new ListClosedChunksKey(exchangeId, pagingId), result);
        }

        public synchronized long getListClosedChunksCallCount(String exchangeId, OptionalLong pagingId)
        {
            return listClosedChunksCounters.computeIfAbsent(new ListClosedChunksKey(exchangeId, pagingId), ignored -> new AtomicLong()).get();
        }

        @Override
        public ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ListenableFuture<Void> setChunkDeliveryMode(String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ListenableFuture<Void> registerExchange(String exchangeId, ChunkDeliveryMode chunkDeliveryMode)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> pingExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> removeExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Optional<RateLimitInfo>> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
        {
            AddDataPagesKey key = new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId);
            List<ListenableFuture<Optional<RateLimitInfo>>> responses = addDataPagesResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            addDataPagesCounters.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordAddDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListenableFuture<Optional<RateLimitInfo>> result)
        {
            addDataPagesResponses.put(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), result);
        }

        public synchronized long getAddDataPagesCallCount(String exchangeId, int taskId, int attemptId, long dataPagesId)
        {
            return addDataPagesCounters.computeIfAbsent(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), ignored -> new AtomicLong()).get();
        }

        @Override
        public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
        {
            throw new RuntimeException("not implemented");
        }
    }

    private static class TestingApiFactory
            implements ApiFactory
    {
        private final Map<Long, DataApi> dataApis = new HashMap<>();

        @Override
        public DiscoveryApi createDiscoveryApi()
        {
            throw new RuntimeException("not implemented");
        }

        public void setDataApi(long nodeId, DataApi dataApi)
        {
            dataApis.put(nodeId, dataApi);
        }

        public void removeDataApi(long nodeId)
        {
            dataApis.remove(nodeId);
        }

        @Override
        public DataApi createDataApi(BufferNodeInfo nodeInfo)
        {
            DataApi dataApi = dataApis.get(nodeInfo.nodeId());
            if (dataApi == null) {
                throw new RuntimeException("cannot create dataApi for node " + nodeInfo.nodeId());
            }
            return dataApi;
        }
    }
}
