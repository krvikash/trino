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
import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.GpuTestUtils;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuHashPartitioningExchanger
{
    @Test
    public void distributesAcrossBuffersByHashKey()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        List<GpuLocalExchangeBuffer> buffers = IntStream.range(0, partitions)
                .mapToObj(_ -> new GpuLocalExchangeBuffer(memory, _ -> {}))
                .toList();
        GpuExchanger exchanger = new GpuHashPartitioningExchanger(buffers, memory, new int[] {0});

        try (GpuPage input = GpuTestUtils.deviceIntColumn(new int[] {1, 2, 3, 4, 5, 6, 7, 8})) {
            exchanger.accept(input);
        }

        int total = 0;
        for (GpuLocalExchangeBuffer buffer : buffers) {
            try (GpuPage page = buffer.removePage()) {
                if (page != null) {
                    total += page.positionCount();
                }
            }
        }
        assertThat(total).isEqualTo(8);
    }

    @Test
    public void multipleAcceptCallsAccumulateAllRows()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        List<GpuLocalExchangeBuffer> buffers = IntStream.range(0, partitions)
                .mapToObj(_ -> new GpuLocalExchangeBuffer(memory, _ -> {}))
                .toList();
        GpuExchanger exchanger = new GpuHashPartitioningExchanger(buffers, memory, new int[] {0});

        // Three consume() calls — value column carries key*100 so we can verify pairing post-partition.
        try (GpuPage in1 = deviceTwoIntColumns(new int[] {1, 2}, new int[] {100, 200});
                GpuPage in2 = deviceTwoIntColumns(new int[] {3, 4, 5}, new int[] {300, 400, 500});
                GpuPage in3 = deviceTwoIntColumns(new int[] {6, 7, 8, 9}, new int[] {600, 700, 800, 900})) {
            exchanger.accept(in1);
            exchanger.accept(in2);
            exchanger.accept(in3);
        }

        Set<Integer> observedKeys = new HashSet<>();
        for (GpuLocalExchangeBuffer buffer : buffers) {
            while (true) {
                GpuPage page = buffer.removePage();
                if (page == null) {
                    break;
                }
                try (page) {
                    List<Integer> keys = collectIntValues(page, 0);
                    List<Integer> values = collectIntValues(page, 1);
                    for (int i = 0; i < keys.size(); i++) {
                        assertThat(values.get(i)).as("key %s value pairing", keys.get(i))
                                .isEqualTo(keys.get(i) * 100);
                        assertThat(observedKeys.add(keys.get(i))).as("duplicate key %s", keys.get(i)).isTrue();
                    }
                }
            }
        }
        assertThat(observedKeys).isEqualTo(Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    public void zeroRowPartitionsDoNotReachBuffers()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        List<GpuLocalExchangeBuffer> buffers = IntStream.range(0, partitions)
                .mapToObj(_ -> new GpuLocalExchangeBuffer(memory, _ -> {}))
                .toList();
        GpuExchanger exchanger = new GpuHashPartitioningExchanger(buffers, memory, new int[] {0});

        // A single row hashes to exactly one partition; the other three should remain empty rather
        // than receive a zero-row page (matches host PartitioningExchanger).
        try (GpuPage input = GpuTestUtils.deviceIntColumn(new int[] {42})) {
            exchanger.accept(input);
        }

        int populatedBuffers = 0;
        for (GpuLocalExchangeBuffer buffer : buffers) {
            try (GpuPage page = buffer.removePage()) {
                if (page != null) {
                    populatedBuffers++;
                    assertThat(page.positionCount()).isEqualTo(1);
                    assertThat(collectIntValues(page, 0)).containsExactly(42);
                }
            }
        }
        assertThat(populatedBuffers).isEqualTo(1);
    }

    @Test
    public void sameKeysFromDifferentCallsLandInSameBuffer()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int partitions = 4;
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        List<GpuLocalExchangeBuffer> buffers = IntStream.range(0, partitions)
                .mapToObj(_ -> new GpuLocalExchangeBuffer(memory, _ -> {}))
                .toList();
        GpuExchanger exchanger = new GpuHashPartitioningExchanger(buffers, memory, new int[] {0});

        // Same key repeated across two consume() calls. cuDF MURMUR3 is deterministic, so the second
        // page's rows for key=7 must land in the same buffer as the first page's row for key=7.
        try (GpuPage in1 = GpuTestUtils.deviceIntColumn(new int[] {7, 7, 7});
                GpuPage in2 = GpuTestUtils.deviceIntColumn(new int[] {7, 7})) {
            exchanger.accept(in1);
            exchanger.accept(in2);
        }

        int bufferWithKey = -1;
        int totalRowsForKey = 0;
        for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
            int rowsInThisPartition = 0;
            while (true) {
                GpuPage page = buffers.get(partitionIndex).removePage();
                if (page == null) {
                    break;
                }
                try (page) {
                    rowsInThisPartition += page.positionCount();
                    assertThat(collectIntValues(page, 0)).allMatch(value -> value == 7);
                }
            }
            if (rowsInThisPartition > 0) {
                if (bufferWithKey == -1) {
                    bufferWithKey = partitionIndex;
                }
                else {
                    throw new AssertionError("key 7 spread across buffers " + bufferWithKey + " and " + partitionIndex);
                }
                totalRowsForKey = rowsInThisPartition;
            }
        }
        assertThat(totalRowsForKey).isEqualTo(5);
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
