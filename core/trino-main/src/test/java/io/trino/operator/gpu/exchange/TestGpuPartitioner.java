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
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuPartitioner
{
    @Test
    public void roundTripPreservesAllRows()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        try (GpuPage input = GpuTestUtils.deviceIntColumn(new int[] {1, 2, 3, 4, 5, 6, 7, 8})) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 4);
            try {
                int total = 0;
                for (GpuPage part : parts) {
                    total += part.positionCount();
                }
                assertThat(total).isEqualTo(8);
                assertThat(parts).hasSize(4);
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
    }

    @Test
    public void emptyInputProducesEmptyPartitions()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        try (GpuPage input = GpuTestUtils.deviceIntColumn(new int[0])) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 3);
            try {
                assertThat(parts).hasSize(3);
                for (GpuPage part : parts) {
                    assertThat(part.positionCount()).isZero();
                }
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
    }

    @Test
    public void singlePartitionPreservesAllRowsAndValues()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int[] inputValues = {10, 20, 30, 40, 50};
        try (GpuPage input = GpuTestUtils.deviceIntColumn(inputValues)) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 1);
            try {
                assertThat(parts).hasSize(1);
                assertThat(parts[0].positionCount()).isEqualTo(inputValues.length);
                assertThat(collectIntValues(parts[0], 0)).containsExactlyInAnyOrder(10, 20, 30, 40, 50);
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
    }

    @Test
    public void partitioningPreservesValuesAcrossAllPartitions()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        int[] inputValues = new int[256];
        for (int i = 0; i < inputValues.length; i++) {
            inputValues[i] = i;
        }
        try (GpuPage input = GpuTestUtils.deviceIntColumn(inputValues)) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 8);
            try {
                assertThat(parts).hasSize(8);
                Set<Integer> seen = new HashSet<>();
                int totalRows = 0;
                for (GpuPage part : parts) {
                    totalRows += part.positionCount();
                    for (Integer value : collectIntValues(part, 0)) {
                        assertThat(seen.add(value)).as("duplicate value %s across partitions", value).isTrue();
                    }
                }
                assertThat(totalRows).isEqualTo(inputValues.length);
                for (int v : inputValues) {
                    assertThat(seen).contains(v);
                }
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
    }

    @Test
    public void sameKeysLandInSamePartition()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        // Many copies of just two distinct keys; cuDF MURMUR3 is deterministic so each key always
        // lands in the same partition, with the matching rows clustered there.
        int[] keys = new int[64];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = (i % 2 == 0) ? 7 : 13;
        }
        try (GpuPage input = GpuTestUtils.deviceIntColumn(keys)) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 4);
            try {
                int partitionFor7 = -1;
                int partitionFor13 = -1;
                int rowsFor7 = 0;
                int rowsFor13 = 0;
                for (int partitionIndex = 0; partitionIndex < parts.length; partitionIndex++) {
                    for (int value : collectIntValues(parts[partitionIndex], 0)) {
                        if (value == 7) {
                            rowsFor7++;
                            if (partitionFor7 == -1) {
                                partitionFor7 = partitionIndex;
                            }
                            else {
                                assertThat(partitionIndex).as("key 7 spread across partitions").isEqualTo(partitionFor7);
                            }
                        }
                        else if (value == 13) {
                            rowsFor13++;
                            if (partitionFor13 == -1) {
                                partitionFor13 = partitionIndex;
                            }
                            else {
                                assertThat(partitionIndex).as("key 13 spread across partitions").isEqualTo(partitionFor13);
                            }
                        }
                    }
                }
                assertThat(rowsFor7).isEqualTo(32);
                assertThat(rowsFor13).isEqualTo(32);
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
    }

    @Test
    public void nonKeyColumnValuesArePreserved()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        // Each row carries a (key, value) where value = key * 100. After partitioning by the key
        // column, every row must still carry its original value in whichever partition it lands.
        int[] keys = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] values = {100, 200, 300, 400, 500, 600, 700, 800};
        try (GpuPage input = deviceTwoIntColumns(keys, values)) {
            GpuPage[] parts = GpuPartitioner.partition(input, new int[] {0}, 4);
            try {
                assertThat(parts).hasSize(4);
                int totalRows = 0;
                for (GpuPage part : parts) {
                    totalRows += part.positionCount();
                    List<Integer> partKeys = collectIntValues(part, 0);
                    List<Integer> partValues = collectIntValues(part, 1);
                    assertThat(partKeys).hasSameSizeAs(partValues);
                    for (int i = 0; i < partKeys.size(); i++) {
                        int expectedValue = partKeys.get(i) * 100;
                        assertThat(partValues.get(i))
                                .as("row with key %s should carry value %s", partKeys.get(i), expectedValue)
                                .isEqualTo(expectedValue);
                    }
                }
                assertThat(totalRows).isEqualTo(keys.length);
            }
            finally {
                for (GpuPage part : parts) {
                    part.close();
                }
            }
        }
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
