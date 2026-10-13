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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.tests.GpuWindowFrameTestParameters.AGGREGATIONS;
import static io.trino.tests.GpuWindowFrameTestParameters.AVG_DECIMAL_TYPES;
import static io.trino.tests.GpuWindowFrameTestParameters.ORDERINGS;
import static io.trino.tests.GpuWindowFrameTestParameters.avgQuery;
import static io.trino.tests.GpuWindowFrameTestParameters.avgRow;
import static io.trino.tests.GpuWindowFrameTestParameters.cartesianProduct;
import static io.trino.tests.GpuWindowFrameTestParameters.partitions;
import static io.trino.tests.GpuWindowFrameTestParameters.unscaledValues;
import static io.trino.tests.GpuWindowTable.ALL_NULLS;
import static io.trino.tests.GpuWindowTable.EDGE_VALUES;
import static io.trino.tests.GpuWindowTable.EMPTY;
import static io.trino.tests.GpuWindowTable.MIXED;
import static io.trino.tests.GpuWindowTable.SINGLE_ROW;
import static io.trino.tests.GpuWindowTable.TIES;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowRangeFrames
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .configureGpuLocalExecution()
                .addExtraProperty("task.concurrency", "1")
                .build();
    }

    @BeforeAll
    void createTables()
    {
        for (GpuWindowTable table : GpuWindowTable.values()) {
            table.create(getQueryRunner());
        }
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderings")
    void testSinglePartitionKeyAndOrderBy(String function, Type orderKeyType, String ordering)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key %3$s)
                """.formatted(function, MIXED.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderings")
    void testNoPartitionBy(String function, Type orderKeyType, String ordering)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM %2$s
                WINDOW w AS (ORDER BY order_key %3$s)
                """.formatted(function, MIXED.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderings")
    void testTies(String function, Type orderKeyType, String ordering)
    {
        // Peer groups are found by comparing order keys, so every type is tested
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key %3$s)
                """.formatted(function, TIES.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderings")
    void testAllNulls(String function, Type orderKeyType, String ordering)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key %3$s)
                """.formatted(function, ALL_NULLS.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndKeyTypes")
    void testSingleRow(String function, Type orderKeyType)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                FROM %2$s
                WINDOW w AS (ORDER BY order_key)
                """.formatted(function, SINGLE_ROW.withOrderKey(orderKeyType))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndKeyTypes")
    void testEmptyInput(String function, Type orderKeyType)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key)
                """.formatted(function, EMPTY.withOrderKey(orderKeyType))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndKeyTypes")
    void testNoOrderBy(String function, Type partitionKeyType)
    {
        // Without ORDER BY, all rows of a partition are peers, so the default frame is the whole partition
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key)
                FROM %2$s
                """.formatted(function, MIXED.withPartitionKey(partitionKeyType))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#AGGREGATIONS")
    void testNoPartitionByNoOrderBy(String function)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER ()
                FROM %2$s
                """.formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#AGGREGATIONS")
    void testDuplicateFunctions(String function)
    {
        // The planner merges only calls written the same, so a call with the default NULLS LAST spelled out is
        // computed again in the same window
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key),
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key NULLS LAST)
                FROM %2$s
                """.formatted(function, TIES.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#GPU_UNSUPPORTED_AGGREGATIONS")
    void testUnsupportedAggregations(String function)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key)
                FROM %2$s
                """.formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesWithoutGpu();
    }

    @ParameterizedTest(name = "{0} {1} {2} {3}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderingPairs")
    void testMultipleKeys(String function, Type orderKeyType, String ordering, String secondOrdering)
    {
        // The second keys subdivide partitions and peer groups
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key, second_partition_key ORDER BY order_key %3$s, second_order_key %4$s)
                FROM %2$s
                """.formatted(function, TIES.withOrderKey(orderKeyType), ordering, secondOrdering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("aggregationsOrderKeyTypesAndOrderings")
    void testEdgeValueOrderKeys(String function, Type orderKeyType, String ordering)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key %3$s),
                    %1$s OVER (ORDER BY order_key %3$s)
                FROM %2$s
                """.formatted(function, EDGE_VALUES.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndKeyTypes")
    void testEdgeValuePartitionKeys(String function, Type partitionKeyType)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY row_id),
                    %1$s OVER (PARTITION BY partition_key)
                FROM %2$s
                """.formatted(function, EDGE_VALUES.withPartitionKey(partitionKeyType))))
                .executesAllWithGpu(WindowNode.class);
    }

    @Test
    void testAggregationsInterleavedWithRankingFunctions()
    {
        // Aggregations and ranking functions are computed in separate batches, whose results must be written back
        // in the order of the functions. Interleaving them, rather than listing them by kind, catches results that
        // are written back batch by batch. The 38-digit avg is computed as several computations.
        assertThat(query(
                """
                SELECT
                    row_id,
                    avg(argument_decimal_9_2) OVER w,
                    rank() OVER w,
                    avg(argument_decimal_38_6) OVER w,
                    row_number() OVER w,
                    dense_rank() OVER w,
                    avg(argument_decimal_19_4) OVER w
                FROM %s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key, row_id)
                """.formatted(MIXED.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @MethodSource("io.trino.tests.GpuWindowFrameTestParameters#avgDecimalSumTypes")
    void testAvgDecimalSumExceedsDeclaredPrecision(DecimalType type)
    {
        // Ten values close to the maximum of the type add up past its precision, and for short decimals past
        // the 64 bits they are stored in, which works only because the running sum is not narrowed to the input
        // type. Above precision 28 the running sum is kept as 64-bit sums of the 32-bit chunks of the values.
        BigInteger max = TEN.pow(type.getPrecision()).subtract(ONE);
        for (BigInteger value : List.of(max, max.negate())) {
            List<BigInteger> values = ImmutableList.<BigInteger>builder()
                    .add(value.subtract(BigInteger.valueOf(9L * value.signum()))) // to keep the divisions inexact
                    .addAll(nCopies(9, value))
                    .build();
            assertAvgExecutesWithGpu(List.of(type), partitions(List.of(values)), "ASC NULLS LAST");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 6, 38})
    void testAvgDecimalSumExceeds128Bits(int scale)
    {
        // Two values near the decimal(38, s) maximum add up past the signed 128-bit range, but their average is
        // still a valid decimal(38, s). The order keys make the first two rows peers, so they share a frame
        // whose sum overflows 128 bits, and so does the frame of the last row. The cancelling rows in between
        // return the sum to a small value.
        BigInteger max = TEN.pow(38).subtract(ONE);
        BigInteger nearMax = max.subtract(BigInteger.valueOf(7));
        List<Long> orderKeys = List.of(0L, 0L, 1L, 2L, 3L, 4L, 5L);
        for (BigInteger sign : List.of(ONE, ONE.negate())) {
            List<BigInteger> values = List.of(
                    max.multiply(sign),
                    nearMax.multiply(sign),
                    max.multiply(sign).negate(),
                    nearMax.multiply(sign).negate(),
                    BigInteger.valueOf(3).multiply(sign),
                    max.multiply(sign),
                    max.multiply(sign));
            List<GpuWindowFrameTestParameters.AvgRow> rows = IntStream.range(0, values.size())
                    .mapToObj(index -> avgRow(0L, orderKeys.get(index), values.get(index)))
                    .collect(toImmutableList());
            assertAvgExecutesWithGpu(List.of(createDecimalType(38, scale)), rows, "ASC NULLS LAST");
        }
    }

    @ParameterizedTest
    @MethodSource("io.trino.tests.GpuWindowFrameTestParameters#decimalPrecisions")
    void testAvgDecimalHalfUpRounding(int precision)
    {
        // Every scale of the precision is tested, so that avg is compiled for every decimal type. Unscaled values
        // stay below 10, so that they are valid for every type. In the first two partitions, a sum of 3 over two
        // rows lands exactly halfway, which HALF_UP rounds away from zero for both signs. In the others, 7 / 3
        // rounds towards zero and 8 / 3 away from it.
        List<DecimalType> types = IntStream.rangeClosed(0, precision)
                .mapToObj(scale -> createDecimalType(precision, scale))
                .collect(toImmutableList());
        assertAvgExecutesWithGpu(
                types,
                partitions(List.of(
                        unscaledValues(1, 2),
                        unscaledValues(-1, -2),
                        unscaledValues(1, 3, 3),
                        unscaledValues(-1, -3, -3),
                        unscaledValues(2, 3, 3),
                        unscaledValues(-2, -3, -3))),
                "ASC NULLS LAST");
    }

    @Test
    void testAvgDecimalFrameWithOnlyNulls()
    {
        // In the first partition, the cumulative frames of the first two rows hold NULLs only, so their count is
        // zero and the average is NULL even though the partition has non-NULL rows further down. The second
        // partition holds NULLs only, so its frames are NULL-only over the whole partition as well.
        assertAvgExecutesWithGpu(
                AVG_DECIMAL_TYPES,
                List.of(
                        avgRow(0L, 0L, null),
                        avgRow(0L, 1L, null),
                        avgRow(0L, 2L, BigInteger.valueOf(2)),
                        avgRow(0L, 3L, BigInteger.valueOf(4)),
                        avgRow(1L, 4L, null),
                        avgRow(1L, 5L, null)),
                "ASC NULLS LAST");
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#ORDERINGS")
    void testAvgDecimalFrameWithOnlyNullsAndNullOrderKeys(String ordering)
    {
        // Rows with a NULL order key are peers of each other. With NULLS FIRST they open the partition, so their
        // shared frame holds NULLs only; with NULLS LAST they close it, so their frame covers the whole partition.
        // The second partition holds NULL order keys and NULL values only.
        assertAvgExecutesWithGpu(
                AVG_DECIMAL_TYPES,
                List.of(
                        avgRow(0L, null, null),
                        avgRow(0L, null, null),
                        avgRow(0L, 1L, BigInteger.valueOf(2)),
                        avgRow(0L, 2L, null),
                        avgRow(0L, 2L, BigInteger.valueOf(4)),
                        avgRow(1L, null, null),
                        avgRow(1L, null, null)),
                ordering);
    }

    static Stream<Arguments> aggregationsOrderKeyTypesAndOrderings()
    {
        return cartesianProduct(AGGREGATIONS, TESTED_GPU_TYPES, ORDERINGS);
    }

    static Stream<Arguments> aggregationsOrderKeyTypesAndOrderingPairs()
    {
        return cartesianProduct(AGGREGATIONS, TESTED_GPU_TYPES, ORDERINGS, ORDERINGS);
    }

    static Stream<Arguments> aggregationsAndKeyTypes()
    {
        return cartesianProduct(AGGREGATIONS, TESTED_GPU_TYPES);
    }

    /// Asserts that avg of a decimal column per type, each holding the unscaled values of the rows, is computed
    /// on the GPU over cumulative frames and over whole partitions, with and without PARTITION BY
    private void assertAvgExecutesWithGpu(List<DecimalType> types, List<GpuWindowFrameTestParameters.AvgRow> rows, String ordering)
    {
        // With ORDER BY, the frame runs from the start of the partition to the last peer of the current row.
        // Without it, the frame is the whole partition, which without PARTITION BY is the whole input.
        List<String> windows = List.of(
                "PARTITION BY partition_key ORDER BY order_key " + ordering,
                "PARTITION BY partition_key",
                "ORDER BY order_key " + ordering,
                "");
        assertThat(query(avgQuery(types, rows, windows)))
                .executesAllWithGpu(WindowNode.class);
    }
}
