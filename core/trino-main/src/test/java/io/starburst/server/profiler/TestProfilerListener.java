/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import io.starburst.server.profiler.results.QueryProfilerResult;
import io.trino.spi.QueryId;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestProfilerListener
{
    @Test
    public void testProfilerProducesResultForCompletedQuery()
    {
        CapturingResultSink sink = new CapturingResultSink();
        try (StandaloneQueryRunner queryRunner = ProfilerQueryRunner.create(sink)) {
            QueryId queryId = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), "SELECT * FROM tpch.tiny.nation").queryId();

            assertEventually(() -> assertThat(sink.results).containsKey(queryId.toString()));
            assertThat(sink.results.get(queryId.toString())).isNotNull();
        }
    }

    @Test
    public void testFailedQueryIsStillProfiled()
    {
        CapturingResultSink sink = new CapturingResultSink();
        try (StandaloneQueryRunner queryRunner = ProfilerQueryRunner.create(sink)) {
            QueryFailedException failure = catchThrowableOfType(
                    QueryFailedException.class,
                    () -> queryRunner.execute(queryRunner.getDefaultSession(), "SELECT 1 / (nationkey - nationkey) FROM tpch.tiny.nation"));
            assertThat(failure).hasMessageContaining("Division by zero");
            QueryId queryId = failure.getQueryId();

            assertEventually(() -> assertThat(sink.results).containsKey(queryId.toString()));
            assertThat(sink.results.get(queryId.toString())).isNotNull();
        }
    }

    @Test
    public void testQueriesAreDroppedWhenProfilerQueueIsFull()
    {
        CapturingResultSink sink = new CapturingResultSink();
        try (StandaloneQueryRunner queryRunner = ProfilerQueryRunner.create(sink)) {
            int queryCount = 25;
            sink.blockProfilerWorker();
            try {
                for (int i = 0; i < queryCount; i++) {
                    queryRunner.execute(queryRunner.getDefaultSession(), "SELECT " + i);
                }
            }
            finally {
                sink.releaseProfilerWorker();
            }

            assertEventually(() -> assertThat(sink.results).isNotEmpty());
            assertThat(sink.results.size()).isLessThan(queryCount);
        }
    }

    private static final class CapturingResultSink
            implements ProfilerResultSink
    {
        public final Map<String, QueryProfilerResult> results = new ConcurrentHashMap<>();
        private volatile CountDownLatch gate = new CountDownLatch(0);

        public void blockProfilerWorker()
        {
            gate = new CountDownLatch(1);
        }

        public void releaseProfilerWorker()
        {
            gate.countDown();
        }

        @Override
        public void accept(QueryProfilerResult result)
        {
            try {
                gate.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for profiler gate", e);
            }
            results.put(result.queryId(), result);
        }
    }
}
