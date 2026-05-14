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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import io.trino.operator.gpu.GpuTestUtils;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSink;
import io.trino.operator.gpu.exchange.GpuLocalExchange.GpuLocalExchangeSinkFactory;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.sql.planner.SystemPartitioningHandle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGpuLocalExchange
{
    @Test
    public void singleDistributionHasOneBuffer()
    {
        GpuLocalExchange exchange = new GpuLocalExchange(SINGLE_DISTRIBUTION, 4, new int[0], 1L << 20);
        assertThat(exchange.getBufferCount()).isEqualTo(1);
    }

    @Test
    public void fixedHashHasConcurrencyBuffers()
    {
        GpuLocalExchange exchange = new GpuLocalExchange(FIXED_HASH_DISTRIBUTION, 4, new int[] {0}, 1L << 20);
        assertThat(exchange.getBufferCount()).isEqualTo(4);
    }

    @Test
    public void rejectsUnsupportedPartitioning()
    {
        assertThatThrownBy(() -> new GpuLocalExchange(
                SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION,
                4,
                new int[0],
                1024L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported partitioning");
    }

    @Test
    public void singleDistributionRejectsPartitionChannels()
    {
        assertThatThrownBy(() -> new GpuLocalExchange(SINGLE_DISTRIBUTION, 1, new int[] {0}, 1024L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void gatherRoundtripPreservesAllValues()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        GpuLocalExchange exchange = new GpuLocalExchange(SINGLE_DISTRIBUTION, 1, new int[0], 1L << 20);
        GpuLocalExchangeSinkFactory factory = exchange.createSinkFactory();
        GpuLocalExchangeSink sink = factory.createSink();
        try (GpuPage p1 = GpuTestUtils.deviceIntColumn(new int[] {1, 2, 3});
                GpuPage p2 = GpuTestUtils.deviceIntColumn(new int[] {4, 5})) {
            sink.addPage(p1);
            sink.addPage(p2);
        }
        sink.finish();
        factory.close();

        GpuLocalExchangeBuffer buffer = exchange.getNextSource();
        List<Integer> drained = drainAll(buffer, 0);
        assertThat(drained).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    public void fixedHashRoundtripPreservesAllValuesAndKeysSamePartition()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        GpuLocalExchange exchange = new GpuLocalExchange(FIXED_HASH_DISTRIBUTION, partitions, new int[] {0}, 1L << 20);
        GpuLocalExchangeSinkFactory factory = exchange.createSinkFactory();
        GpuLocalExchangeSink sink = factory.createSink();
        try (GpuPage p1 = deviceTwoIntColumns(new int[] {1, 2, 3, 4}, new int[] {100, 200, 300, 400});
                GpuPage p2 = deviceTwoIntColumns(new int[] {5, 6, 7, 1}, new int[] {500, 600, 700, 100})) {
            sink.addPage(p1);
            sink.addPage(p2);
        }
        sink.finish();
        factory.close();

        Set<Integer> seenKeys = new HashSet<>();
        int totalRows = 0;
        // Track which partition each key landed in to assert determinism across two consume() calls.
        Map<Integer, Integer> partitionByKey = new HashMap<>();
        for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
            GpuLocalExchangeBuffer buffer = exchange.getNextSource();
            while (true) {
                GpuPage page = buffer.removePage();
                if (page == null) {
                    break;
                }
                try (page) {
                    List<Integer> keys = collectIntValues(page, 0);
                    List<Integer> values = collectIntValues(page, 1);
                    totalRows += keys.size();
                    for (int i = 0; i < keys.size(); i++) {
                        int key = keys.get(i);
                        assertThat(values.get(i)).as("value pairing for key %s", key).isEqualTo(key * 100);
                        seenKeys.add(key);
                        Integer prevPartition = partitionByKey.put(key, partitionIndex);
                        if (prevPartition != null) {
                            assertThat(prevPartition)
                                    .as("key %s appeared in partition %s after first appearing in %s", key, partitionIndex, prevPartition)
                                    .isEqualTo(partitionIndex);
                        }
                    }
                }
            }
        }
        assertThat(totalRows).isEqualTo(8);
        assertThat(seenKeys).isEqualTo(Set.of(1, 2, 3, 4, 5, 6, 7));
    }

    @Test
    public void multipleSinkFactoriesContributeToSameBuffers()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        GpuLocalExchange exchange = new GpuLocalExchange(FIXED_HASH_DISTRIBUTION, partitions, new int[] {0}, 1L << 20);

        // Two sibling sink factories (analogous to two parallel sink drivers in production) each
        // create one sink and write disjoint key ranges into the shared exchange. Both close before
        // the sources drain.
        GpuLocalExchangeSinkFactory factoryA = exchange.createSinkFactory();
        GpuLocalExchangeSink sinkA = factoryA.createSink();
        try (GpuPage pageA = GpuTestUtils.deviceIntColumn(new int[] {10, 11, 12, 13})) {
            sinkA.addPage(pageA);
        }
        sinkA.finish();
        factoryA.close();

        GpuLocalExchangeSinkFactory factoryB = exchange.createSinkFactory();
        GpuLocalExchangeSink sinkB = factoryB.createSink();
        try (GpuPage pageB = GpuTestUtils.deviceIntColumn(new int[] {20, 21, 22, 23})) {
            sinkB.addPage(pageB);
        }
        sinkB.finish();
        factoryB.close();

        Set<Integer> drained = new HashSet<>();
        for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
            drained.addAll(drainAll(exchange.getNextSource(), 0));
        }
        assertThat(drained).isEqualTo(Set.of(10, 11, 12, 13, 20, 21, 22, 23));
    }

    @Test
    public void sourceFinishedFlagPropagatesAfterAllSinksClose()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        GpuLocalExchange exchange = new GpuLocalExchange(SINGLE_DISTRIBUTION, 1, new int[0], 1L << 20);
        GpuLocalExchangeSinkFactory factory = exchange.createSinkFactory();
        GpuLocalExchangeSink sink = factory.createSink();
        GpuLocalExchangeBuffer buffer = exchange.getNextSource();

        try (GpuPage page = GpuTestUtils.deviceIntColumn(new int[] {1})) {
            sink.addPage(page);
        }
        assertThat(buffer.isFinished()).isFalse();
        sink.finish();
        // Sink finish alone is not enough: the factory has to close, then noMoreSinkFactories.
        assertThat(buffer.isFinished()).isFalse();
        factory.close();
        factory.noMoreSinkFactories();
        // Now buffer should be marked finishing — but it still has the queued page until drained.
        try (GpuPage page = buffer.removePage()) {
            assertThat(page).isNotNull();
            assertThat(page.positionCount()).isEqualTo(1);
        }
        assertThat(buffer.isFinished()).isTrue();
    }

    private static GpuPage deviceTwoIntColumns(int[] keys, int[] values)
    {
        ColumnVector keyColumnVector = ColumnVector.fromInts(keys);
        ColumnVector valueColumnVector = ColumnVector.fromInts(values);
        try (DeviceMemory keyColumn = new DeviceMemory(keyColumnVector);
                DeviceMemory valueColumn = new DeviceMemory(valueColumnVector)) {
            return new GpuPage(keys.length, new Column[] {keyColumn, valueColumn});
        }
    }

    private static List<Integer> drainAll(GpuLocalExchangeBuffer buffer, int columnIndex)
    {
        List<Integer> out = new ArrayList<>();
        while (true) {
            GpuPage page = buffer.removePage();
            if (page == null) {
                return out;
            }
            try (page) {
                out.addAll(collectIntValues(page, columnIndex));
            }
        }
    }

    private static List<Integer> collectIntValues(GpuPage page, int columnIndex)
    {
        DeviceMemory column = (DeviceMemory) page.column(columnIndex);
        ColumnVector columnVector = column.columnVector();
        List<Integer> out = new ArrayList<>(page.positionCount());
        try (HostColumnVector host = columnVector.copyToHost()) {
            for (int i = 0; i < page.positionCount(); i++) {
                out.add(host.getInt(i));
            }
        }
        return out;
    }
}
