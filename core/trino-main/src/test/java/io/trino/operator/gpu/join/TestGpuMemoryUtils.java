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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import java.util.List;
import java.util.Optional;

import static ai.rapids.cudf.DType.INT32;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getFilterGpuDeviceMemoryUsage;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getHashJoinAdditionalGpuDeviceMemoryUsage;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.getNullColumnMemoryUsage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static io.trino.spi.gpu.GpuTypeConversion.toGpuMapping;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
class TestGpuMemoryUtils
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testGetHashJoinAdditionalGpuDeviceMemoryUsage()
    {
        for (Type type : TESTED_GPU_TYPES) {
            for (NullsProvider nullsProvider : NullsProvider.values()) {
                for (Integer positionCount : List.of(1, 10, 25, 1024, 10_000, 1_234_567)) {
                    testGetHashJoinAdditionalGpuDeviceMemoryUsage(type, nullsProvider, positionCount);
                }
            }
        }
    }

    @Test
    void testGetHashJoinAdditionalGpuDeviceMemoryUsageAtFourMbRoundingBoundary()
    {
        // Regression: cuco's storage allocates in two separately 2 MB-rounded chunks, so its
        // effective rounding granularity is 4 MB. These row counts produce cucoBytes that
        // crosses a 4 MB boundary but not a 2 MB boundary, where 2 MB-rounded estimates
        // underestimated actual usage by ~1.3 MB.
        for (int positionCount : List.of(5_000_000, 5_100_000)) {
            testGetHashJoinAdditionalGpuDeviceMemoryUsage(BIGINT, NO_NULLS, positionCount);
        }
    }

    private static void testGetHashJoinAdditionalGpuDeviceMemoryUsage(Type type, NullsProvider nullsProvider, int positionCount)
    {
        Block block = createBlock(type, positionCount, nullsProvider);
        long baseline = Rmm.getTotalBytesAllocated();

        try (GpuPage gpuPage = getOnlyElement(copyToDevice(List.of(new Page(block)), List.of(type)));
                Table keyTable = toTable(gpuPage)) {
            long afterKeyTable = Rmm.getTotalBytesAllocated();

            try (HashJoin hashJoin = new HashJoin(keyTable, /*compareNullsEqual=*/ false)) {
                long actual = Rmm.getTotalBytesAllocated() - afterKeyTable;
                long reported = getHashJoinAdditionalGpuDeviceMemoryUsage(keyTable);

                assertThat(reported)
                        .as("reported >= actual, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                        .isGreaterThanOrEqualTo(actual);

                if (reported < 1024) {
                    // "small" values can be overestimated as long as the estimate is also "small"
                }
                else {
                    assertThat(reported)
                            .as("reported within 5%% of actual, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                            .isLessThanOrEqualTo((long) (actual * 1.05));
                }
            }
        }
        assertThat(Rmm.getTotalBytesAllocated())
                .as("liveBytes after free, type=%s nullsProvider=%s positionCount=%s", type, nullsProvider, positionCount)
                .isEqualTo(baseline);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGetNullColumnMemoryUsage(Type type)
    {
        GpuTypeConversion.GpuTypeMapping mapping = toGpuMapping(type).orElseThrow();

        for (int positionCount : List.of(1, 10, 25, 1024, 10_000, 1_234_567)) {
            try (Scalar nullScalar = mapping.toScalar().copyToScalar(Optional.empty())) {
                long estimated = getNullColumnMemoryUsage(mapping.dType(), positionCount);

                try (ColumnVector vector = ColumnVector.fromScalar(nullScalar, positionCount)) {
                    long actual = vector.getDeviceMemorySize();
                    assertThat(estimated)
                            .isEqualTo(actual);
                }
            }
        }
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testGetFilterGpuDeviceMemoryUsage(Type type)
    {
        for (int positionCount : List.of(1024, 10_000, 1_234_567)) {
            Block tableBlock = createBlock(type, positionCount, NO_NULLS);
            Block maskBlock = createBlock(BOOLEAN, positionCount, NO_NULLS);

            GpuTypeConversion.ToColumn toColumn = GpuTypeConversion.toGpuMapping(BOOLEAN).orElseThrow().toColumn();

            try (GpuPage page = getOnlyElement(copyToDevice(List.of(new Page(tableBlock)), List.of(type)));
                    Table table = toTable(page);
                    ColumnVector mask = toColumn.copyToDevice(new Column.Blocks(List.of(maskBlock)));
                    Scalar sum = mask.sum(INT32)) {
                int retained = sum.getInt();

                Rmm.resetScopedMaximumBytesAllocated(0);
                try (Table _ = table.filter(mask)) {
                    long actual = Rmm.getScopedMaximumBytesAllocated();
                    long estimated = getFilterGpuDeviceMemoryUsage(page, retained);
                    assertThat(estimated)
                            .isLessThanOrEqualTo((long) (actual * 1.05));
                }
            }
        }
    }
}
