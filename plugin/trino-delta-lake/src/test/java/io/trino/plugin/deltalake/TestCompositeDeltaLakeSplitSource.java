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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCompositeDeltaLakeSplitSource
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new DeltaLakeSessionProperties(
                    new DeltaLakeConfig()
                            .setCompositeSplitsEnabled(true)
                            .setMaxSplitSize(DataSize.ofBytes(10_000)),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    @Test
    void testGroupsByPartition()
            throws ExecutionException, InterruptedException
    {
        List<DeltaLakeSplit> splits = ImmutableList.of(
                split("a1", 1_000, "a"),
                split("b1", 1_000, "b"),
                split("a2", 1_000, "a"),
                split("n1", 1_000, ImmutableMap.of("part", Optional.empty())),
                split("n2", 1_000, ImmutableMap.of("part", Optional.empty())));

        List<ConnectorSplit> result = drain(splits);

        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isInstanceOfSatisfying(
                CompositeDeltaLakeSplit.class,
                composite -> assertThat(composite.splits()).extracting(DeltaLakeSplit::path).containsExactly("a1", "a2"));
        assertThat(result.get(1)).isInstanceOfSatisfying(DeltaLakeSplit.class, split -> assertThat(split.path()).isEqualTo("b1"));
        assertThat(result.get(2)).isInstanceOfSatisfying(
                CompositeDeltaLakeSplit.class,
                composite -> assertThat(composite.splits()).extracting(DeltaLakeSplit::path).containsExactly("n1", "n2"));
    }

    @Test
    void testFlushesWhenTargetExceeded()
            throws ExecutionException, InterruptedException
    {
        List<DeltaLakeSplit> splits = ImmutableList.of(
                split("f1", 6_000, ImmutableMap.of()),
                split("f2", 6_000, ImmutableMap.of()),
                split("f3", 3_000, ImmutableMap.of()));

        List<ConnectorSplit> result = drain(splits);

        assertThat(result).hasSize(2);
        assertThat(result.get(0)).isInstanceOfSatisfying(DeltaLakeSplit.class, split -> assertThat(split.path()).isEqualTo("f1"));
        assertThat(result.get(1)).isInstanceOfSatisfying(
                CompositeDeltaLakeSplit.class,
                composite -> assertThat(composite.splits()).extracting(DeltaLakeSplit::path).containsExactly("f2", "f3"));
    }

    @Test
    void testUnpartitioned()
            throws ExecutionException, InterruptedException
    {
        List<DeltaLakeSplit> splits = ImmutableList.of(
                split("f1", 1_000, ImmutableMap.of()),
                split("f2", 1_000, ImmutableMap.of()),
                split("f3", 1_000, ImmutableMap.of()));

        List<ConnectorSplit> result = drain(splits);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst()).isInstanceOfSatisfying(
                CompositeDeltaLakeSplit.class,
                composite -> assertThat(composite.splits()).extracting(DeltaLakeSplit::path).containsExactly("f1", "f2", "f3"));
    }

    @Test
    void testWaitsForFullBatch()
            throws Exception
    {
        // The producer queues a split only once the test releases it
        Semaphore released = new Semaphore(0);
        Stream<DeltaLakeSplit> splits = Stream.of(
                        split("f1", 1_000, ImmutableMap.of()),
                        split("f2", 1_000, ImmutableMap.of()),
                        split("f3", 1_000, ImmutableMap.of()))
                .peek(_ -> released.acquireUninterruptibly());
        ExecutorService executor = newSingleThreadExecutor(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        try {
            ConnectorSplitSource splitSource = createSplitSource(splits, executor);
            CompletableFuture<List<ConnectorSplit>> batch = splitSource.getNextBatch(3, new DynamicFilterSnapshot(TupleDomain.all(), true));

            released.release(2);
            assertThat(batch).isNotDone();

            released.release();
            List<ConnectorSplit> result = batch.get(10, SECONDS);
            assertThat(result).hasSize(1);
            assertThat(result.getFirst()).isInstanceOfSatisfying(
                    CompositeDeltaLakeSplit.class,
                    composite -> assertThat(composite.splits()).extracting(DeltaLakeSplit::path).containsExactly("f1", "f2", "f3"));
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static List<ConnectorSplit> drain(List<DeltaLakeSplit> splits)
            throws ExecutionException, InterruptedException
    {
        ConnectorSplitSource splitSource = createSplitSource(splits.stream(), newDirectExecutorService());
        ImmutableList.Builder<ConnectorSplit> result = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            result.addAll(splitSource.getNextBatch(100, new DynamicFilterSnapshot(TupleDomain.all(), true)).get());
        }
        return result.build();
    }

    private static ConnectorSplitSource createSplitSource(Stream<DeltaLakeSplit> splits, ExecutorService executor)
    {
        return new DeltaLakeSplitSource(
                new SchemaTableName("schema", "table"),
                splits,
                executor,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                new Duration(0, SECONDS),
                false,
                SESSION,
                Optional.empty());
    }

    private static DeltaLakeSplit split(String path, long length, String partitionValue)
    {
        return split(path, length, ImmutableMap.of("part", Optional.of(partitionValue)));
    }

    private static DeltaLakeSplit split(String path, long length, Map<String, Optional<String>> partitionKeys)
    {
        return new DeltaLakeSplit(
                path,
                0,
                length,
                length,
                Optional.empty(),
                0,
                Optional.empty(),
                Optional.empty(),
                SplitWeight.standard(),
                TupleDomain.all(),
                partitionKeys);
    }
}
