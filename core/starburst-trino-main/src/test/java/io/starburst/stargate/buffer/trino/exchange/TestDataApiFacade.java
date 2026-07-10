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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.BufferNodeExchangeMetrics;
import io.starburst.stargate.buffer.data.client.ChunkDeliveryMode;
import io.starburst.stargate.buffer.data.client.ChunkList;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.starburst.stargate.buffer.data.client.RateLimitInfo;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.trino.exchange.DataApiFacade.RetryExecutorConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
    public void testAddDataPagesSuccess()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(1);
    }

    @Test
    public void testAddDataPagesRetries()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // OK after INTERNAL_ERROR
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(2);

        // DRAINING after INTERNAL_ERROR should produce DRAINING_ON_RETRY
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages("exchange-3", 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.DRAINING, "blah")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, "exchange-3", 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(5, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINING_ON_RETRY))
                .withMessageContaining("Received DRAINING error code on retry");
        assertThat(dataApiDelegate.getAddDataPagesCallCount("exchange-3", 0, 0, 0)).isEqualTo(2);
    }

    @Test
    public void testAddDataPagesMaxRetriesExceeded()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // 3 failures with maxRetries=2 means first attempt + 2 retries = 3 attempts, all fail
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah1")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah2")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah3")));

        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .failsWithin(5, SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(DataApiException.class)
                .withMessageContaining("blah3");
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(3);
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
                new RetryExecutorConfig(2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // test cancelling an in-progress request
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0L, listeningDecorator(executor).submit(() -> {
            sleepUninterruptibly(java.time.Duration.ofMillis(500));
            return Optional.empty();
        }));
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture0 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0L, ImmutableListMultimap.of());
        Thread.sleep(100);
        addDataPagesFuture0.cancel(true);
        assertThatThrownBy(() -> getFutureValue(addDataPagesFuture0)).isInstanceOf(CancellationException.class);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0L)).isEqualTo(1);

        // test cancelling after request failure prevents retry
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 1, 1, 1L, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture1 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 1, 1, 1L, ImmutableListMultimap.of());
        sleepUninterruptibly(java.time.Duration.ofMillis(500));
        addDataPagesFuture1.cancel(true);
        sleepUninterruptibly(java.time.Duration.ofSeconds(1));
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 1, 1, 1L)).isEqualTo(1);
        assertThatThrownBy(() -> getFutureValue(addDataPagesFuture1)).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testAddDataPagesConcurrencyLimit()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        int maxConcurrent = 2;
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                maxConcurrent);

        // Use SettableFutures to control when responses complete
        SettableFuture<Optional<RateLimitInfo>> responseFuture0 = SettableFuture.create();
        SettableFuture<Optional<RateLimitInfo>> responseFuture1 = SettableFuture.create();
        SettableFuture<Optional<RateLimitInfo>> responseFuture2 = SettableFuture.create();
        SettableFuture<Optional<RateLimitInfo>> responseFuture3 = SettableFuture.create();
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, responseFuture0);
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, responseFuture1);
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, responseFuture2);
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 3, responseFuture3);

        // submit 4 requests
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture0 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of());
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture1 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of());
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture2 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of());
        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture3 = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 3, ImmutableListMultimap.of());

        Thread.sleep(200); // give time for dispatch

        // only maxConcurrent requests should have been dispatched
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(1);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 1)).isEqualTo(1);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 2)).isEqualTo(0);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 3)).isEqualTo(0);

        // complete first two, which should unblock the next two
        responseFuture0.set(Optional.empty());
        responseFuture1.set(Optional.empty());
        assertThat(addDataPagesFuture0).succeedsWithin(5, SECONDS);
        assertThat(addDataPagesFuture1).succeedsWithin(5, SECONDS);

        Thread.sleep(200);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 2)).isEqualTo(1);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 3)).isEqualTo(1);

        responseFuture2.set(Optional.empty());
        responseFuture3.set(Optional.empty());
        assertThat(addDataPagesFuture2).succeedsWithin(5, SECONDS);
        assertThat(addDataPagesFuture3).succeedsWithin(5, SECONDS);
    }

    @Test
    public void testDestroy()
            throws InterruptedException
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                // long backoff so the retry request is still in backoff when destroy is called
                new RetryExecutorConfig(5, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                1);
        dataApiFacade.init();

        // request 0 fails immediately with INTERNAL_ERROR, entering retry backoff
        dataApiDelegate.recordAddDataPages(
                EXCHANGE_0,
                0,
                0,
                0,
                immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "boom")));
        // request 1 gets dispatched after request 0 fails and frees the slot
        SettableFuture<Optional<RateLimitInfo>> inFlight = SettableFuture.create();
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, inFlight);
        // request 2 never gets dispatched because maxConcurrent=1

        ListenableFuture<DataApiFacade.AddDataPagesResponse> backoffRequestFuture = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of());
        dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of());
        ListenableFuture<DataApiFacade.AddDataPagesResponse> pendingRequestFuture = dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of());

        Thread.sleep(200);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(1);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 1)).isEqualTo(1);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 2)).isEqualTo(0);
        assertThat(backoffRequestFuture).isNotDone();
        assertThat(pendingRequestFuture).isNotDone();

        executor.schedule(() -> inFlight.set(Optional.empty()), 100, MILLISECONDS);
        dataApiFacade.destroy();

        // destroy cancels both the backoff and pending result futures
        assertThat(backoffRequestFuture.isCancelled()).isTrue();
        assertThat(pendingRequestFuture.isCancelled()).isTrue();
        assertThat(inFlight).succeedsWithin(5, SECONDS);
    }

    @Test
    public void testAddDataPagesStats()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        DataApiFacade.AddDataPagesOperationStats stats = dataApiFacade.getAddDataPagesOperationStats();
        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getFailedOperationCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(0);
        assertThat(stats.getAnyRequestErrorCount().getTotalCount()).isEqualTo(0);

        // success on first attempt
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(1);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(0);

        // success after 1 retry
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(2);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(1);
        assertThat(stats.getAnyRequestErrorCount().getTotalCount()).isEqualTo(1);

        // failure after max retries
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah1")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah2")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah3")));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of()))
                .failsWithin(5, SECONDS);

        assertThat(stats.getSuccessOperationCount().getTotalCount()).isEqualTo(2);
        assertThat(stats.getFailedOperationCount().getTotalCount()).isEqualTo(1);
        assertThat(stats.getRequestRetryCount().getTotalCount()).isEqualTo(3); // 1 from before + 2 retries
        assertThat(stats.getAnyRequestErrorCount().getTotalCount()).isEqualTo(4); // 1 from before + 3 errors (2 retries + 1 final failure)

        // overloaded error counted
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 3, immediateFailedFuture(new DataApiException(ErrorCode.OVERLOADED, "overloaded")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 3, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 3, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        assertThat(stats.getOverloadedRequestErrorCount().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testAddDataPagesRetriesOnRuntimeException()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // RuntimeException should also be retried in the new path
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFailedFuture(new RuntimeException("random exception")));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0)).isEqualTo(2);
    }

    @Test
    public void testEnforcedByServerRateLimitInfo()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // First request returns rate limit info: 2 req/s with 0ms avg process time -> 500ms interval
        RateLimitInfo rateLimitInfo = new RateLimitInfo(2.0, 0);
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.of(rateLimitInfo)));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.of(rateLimitInfo)));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFuture(Optional.of(rateLimitInfo)));

        Stopwatch stopwatch = Stopwatch.createStarted();

        // First request - should complete immediately
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        // Second request - should be delayed ~500ms due to rate limit
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        // Third request - should be delayed another ~500ms
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        long elapsedMs = stopwatch.elapsed(MILLISECONDS);
        // 3 requests with 500ms spacing should take at least ~1000ms (first is immediate, then 500ms + 500ms)
        assertThat(elapsedMs).isGreaterThanOrEqualTo(800); // allow some slack
    }

    @Test
    public void testNoDelayWithoutRateLimitInfo()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // All requests return empty rate limit info - no throttling
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.empty()));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.empty()));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFuture(Optional.empty()));

        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        long elapsedMs = stopwatch.elapsed(MILLISECONDS);
        // Without rate limiting, all 3 requests should complete very quickly
        assertThat(elapsedMs).isLessThan(500);
    }

    @Test
    public void testAdjustsWhenServerChangesRateLimit()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // First request returns tight rate limit (2 req/s -> 500ms interval)
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.of(new RateLimitInfo(2.0, 0))));
        // Second request returns no rate limit (server relaxed)
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.empty()));
        // Third and fourth requests should be fast since rate limit was cleared
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFuture(Optional.empty()));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 3, immediateFuture(Optional.empty()));

        // First request
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        // Second request - delayed by ~500ms from first (rate limit from first response)
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        Stopwatch stopwatch = Stopwatch.createStarted();
        // Third and fourth requests - should be fast since server cleared rate limit
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 3, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        long elapsedMs = stopwatch.elapsed(MILLISECONDS);
        // After rate limit cleared, these should complete quickly
        assertThat(elapsedMs).isLessThan(500);
    }

    @Test
    public void testAccountsForProcessTime()
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        DataApiFacade dataApiFacade = createDataApiFacade(
                dataApiDelegate,
                BufferNodeState.ACTIVE,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16);

        // rateLimit=2 -> expectedTotal=500ms, averageProcessTime=400ms -> interval=100ms
        // Much shorter interval than without process time accounting
        RateLimitInfo rateLimitInfo = new RateLimitInfo(2.0, 400);
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, immediateFuture(Optional.of(rateLimitInfo)));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.of(rateLimitInfo)));
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 2, immediateFuture(Optional.of(rateLimitInfo)));

        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);
        assertThat(dataApiFacade.addDataPages(TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 2, ImmutableListMultimap.of()))
                .succeedsWithin(5, SECONDS);

        long elapsedMs = stopwatch.elapsed(MILLISECONDS);
        // With 100ms interval, 3 requests should take ~200ms, definitely less than 500ms
        assertThat(elapsedMs).isLessThan(500);
    }

    @Test
    public void testInFlightRequestCompletesOnCleanUp()
            throws Exception
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(
                discoveryManager,
                apiFactory,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16,
                executor);

        SettableFuture<Optional<RateLimitInfo>> inFlight = SettableFuture.create();
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 0, inFlight);

        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture = dataApiFacade.addDataPages(
                TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of());

        // wait until the request reaches the fake data API
        long deadlineNanos = System.nanoTime() + SECONDS.toNanos(2);
        while (dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0) == 0) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("request never reached data api");
            }
            Thread.sleep(5);
        }
        assertThat(addDataPagesFuture).isNotDone();

        // mark the node DRAINED and trigger cleanup explicitly
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.DRAINED));
        dataApiFacade.cleanUp();

        // the in-flight request's future must NOT be cancelled — we wait for the actual outcome
        assertThat(addDataPagesFuture).isNotDone();

        // complete the underlying HTTP future with a success after cleanup ran;
        // the caller must see the success, not a cancellation
        inFlight.set(Optional.empty());
        assertThat(addDataPagesFuture).succeedsWithin(2, SECONDS);

        // run cleanup to remove the now-fully-drained processing state
        dataApiFacade.cleanUp();

        // a subsequent addDataPages for the drained node must fail fast with DRAINED
        dataApiDelegate.recordAddDataPages(EXCHANGE_0, 0, 0, 1, immediateFuture(Optional.empty()));
        assertShortCircuitResponseIfNodeDrained(() -> dataApiFacade.addDataPages(
                TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 1, ImmutableListMultimap.of()));
    }

    @Test
    public void testRetryBackoffAfterCleanUpCompletesWithDrainingOnRetry()
            throws Exception
    {
        TestingDataApi dataApiDelegate = new TestingDataApi();
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.ACTIVE));
        TestingApiFactory apiFactory = new TestingApiFactory();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        DataApiFacade dataApiFacade = new DataApiFacade(
                discoveryManager,
                apiFactory,
                new RetryExecutorConfig(0, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                // long backoff so we can race with cleanup
                new RetryExecutorConfig(5, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, 10, 5, Duration.valueOf("30s")),
                16,
                executor);

        // first attempt fails with INTERNAL_ERROR (eligible for retry)
        dataApiDelegate.recordAddDataPages(
                EXCHANGE_0,
                0,
                0,
                0,
                immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "boom")));
        // leave a second response recorded in case retry somehow resurrects
        dataApiDelegate.recordAddDataPages(
                EXCHANGE_0,
                0,
                0,
                0,
                immediateFuture(Optional.empty()));

        ListenableFuture<DataApiFacade.AddDataPagesResponse> addDataPagesFuture = dataApiFacade.addDataPages(
                TestingDataApi.NODE_ID, EXCHANGE_0, 0, 0, 0, ImmutableListMultimap.of());

        // wait for first call to land
        long deadlineNanos = System.nanoTime() + SECONDS.toNanos(2);
        while (dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0) == 0) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("first request never reached data api");
            }
            Thread.sleep(5);
        }

        // while retry is in its 500ms backoff window, mark node DRAINED and clean up
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, BufferNodeState.DRAINED));
        dataApiFacade.cleanUp();

        // caller must see DRAINING_ON_RETRY once backoff fires and re-enqueue is rejected (state is draining)
        assertThatThrownBy(() -> addDataPagesFuture.get(3, SECONDS))
                .cause()
                .isInstanceOf(DataApiException.class)
                .extracting(e -> ((DataApiException) e).getErrorCode())
                .isEqualTo(ErrorCode.DRAINING_ON_RETRY);

        // wait long enough for the retry backoff (500 ms) to have fired, with CI headroom
        Thread.sleep(1500);

        // retry must NOT have fired against the data api
        assertThat(dataApiDelegate.getAddDataPagesCallCount(EXCHANGE_0, 0, 0, 0))
                .as("retry must not re-execute after cleanup marked state as draining")
                .isEqualTo(1);

        // map must have no lingering state for this node after one more cleanup
        dataApiFacade.cleanUp();
    }

    private DataApiFacade createDataApiFacade(
            TestingDataApi dataApiDelegate,
            BufferNodeState bufferNodeState,
            RetryExecutorConfig defaultRetryExecutorConfig,
            RetryExecutorConfig addDataPagesRetryExecutorConfig,
            int maxConcurrentAddDataPagesPerNode)
    {
        TestingBufferNodeDiscoveryManager discoveryManager = new TestingBufferNodeDiscoveryManager();
        discoveryManager.setBufferNodes(builder -> builder.putNode(TestingDataApi.NODE_ID, bufferNodeState));
        TestingApiFactory apiFactory = new TestingApiFactory();
        apiFactory.setDataApi(TestingDataApi.NODE_ID, dataApiDelegate);

        return new DataApiFacade(
                discoveryManager,
                apiFactory,
                defaultRetryExecutorConfig,
                addDataPagesRetryExecutorConfig,
                maxConcurrentAddDataPagesPerNode,
                executor);
    }

    private static void assertShortCircuitResponseIfNodeDrained(Supplier<ListenableFuture<?>> call)
    {
        assertThat(call.get())
                .failsWithin(100, MILLISECONDS) // returns immediatelly
                .withThrowableOfType(ExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(ErrorCode.DRAINED))
                .withMessageMatching(".*Node .* already DRAINED");
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
            return new BufferNodeInfo(NODE_ID, URI.create("http://testing"), Optional.empty(), Optional.empty(), BufferNodeState.ACTIVE, Instant.now());
        }

        @Override
        public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
        {
            ListClosedChunksKey key = new ListClosedChunksKey(exchangeId, pagingId);
            List<ListenableFuture<ChunkList>> responses = listClosedChunksResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            listClosedChunksCounters.computeIfAbsent(key, _ -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
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
        public ListenableFuture<Void> registerExchange(String exchangeId, ChunkDeliveryMode chunkDeliveryMode, Span exchangeSpan)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<BufferNodeExchangeMetrics> pingExchange(String exchangeId)
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
            addDataPagesCounters.computeIfAbsent(key, _ -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordAddDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListenableFuture<Optional<RateLimitInfo>> result)
        {
            addDataPagesResponses.put(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), result);
        }

        public synchronized long getAddDataPagesCallCount(String exchangeId, int taskId, int attemptId, long dataPagesId)
        {
            return addDataPagesCounters.computeIfAbsent(new AddDataPagesKey(exchangeId, taskId, attemptId, dataPagesId), _ -> new AtomicLong()).get();
        }

        @Override
        public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<DataApi.ChunkDataResponse> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
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
