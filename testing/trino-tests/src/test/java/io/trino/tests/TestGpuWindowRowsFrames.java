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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.tests.GpuWindowFrameTestParameters.AGGREGATIONS;
import static io.trino.tests.GpuWindowFrameTestParameters.FRAME_SUM_DECIMAL_TYPES;
import static io.trino.tests.GpuWindowFrameTestParameters.ORDERINGS;
import static io.trino.tests.GpuWindowFrameTestParameters.cartesianProduct;
import static io.trino.tests.GpuWindowFrameTestParameters.decimalAggregationQuery;
import static io.trino.tests.GpuWindowFrameTestParameters.decimalRow;
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
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowRowsFrames
        extends AbstractTestQueryFramework
{
    private static final List<String> FRAMES = List.of(
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 0 PRECEDING AND 0 FOLLOWING",
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING",
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
            "ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
            "ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING",
            // Empty frames, which end before they start
            "ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING",
            "ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING",
            // Per-row offsets, of which some make the frame empty
            "ROWS BETWEEN start_offset PRECEDING AND end_offset FOLLOWING",
            "ROWS BETWEEN start_offset PRECEDING AND end_offset PRECEDING",
            "ROWS BETWEEN start_offset FOLLOWING AND end_offset FOLLOWING",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND end_offset PRECEDING",
            "ROWS BETWEEN start_offset FOLLOWING AND UNBOUNDED FOLLOWING",
            // Offsets of other types, which the planner casts to BIGINT
            "ROWS BETWEEN CAST(start_offset AS tinyint) PRECEDING AND CAST(end_offset AS integer) FOLLOWING");

    // Frames anchored at either end of the partition, sliding ones, and ones that are empty at the ends of it
    private static final List<String> DECIMAL_AGGREGATION_FRAMES = List.of(
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING",
            "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING");

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

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("framesAndTables")
    void testFrames(String frame, GpuWindowTable table)
    {
        String functions = AGGREGATIONS.stream()
                .flatMap(function -> Stream.of(
                        "%s OVER (PARTITION BY partition_key ORDER BY order_key, row_id %s)".formatted(function, frame),
                        "%s OVER (ORDER BY order_key, row_id %s)".formatted(function, frame)))
                .collect(joining(",\n"));
        assertThat(query(
                """
                SELECT
                    row_id,
                %s
                FROM %s
                """.formatted(functions.indent(4).stripTrailing(), table.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#AGGREGATIONS")
    void testAggregationsNoOrderBy(String function)
    {
        // Without ORDER BY, rows of a partition are in any order, so only a frame of the whole partition is deterministic
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    %1$s OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                FROM %2$s
                """.formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("orderKeyTypesAndOrderings")
    void testOrderKeyTypes(Type orderKeyType, String ordering)
    {
        // Frame bounds are row positions, so they depend on the sort but not on the order key values
        assertThat(query(
                """
                SELECT
                    row_id,
                    avg(argument_decimal_38_6) OVER (w ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING),
                    avg(argument_decimal_38_6) OVER (w ROWS BETWEEN start_offset FOLLOWING AND end_offset FOLLOWING)
                FROM %s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key %s, row_id)
                """.formatted(TIES.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndOrderings")
    void testMultiplePartitionKeys(String function, String ordering)
    {
        // The second partition key subdivides partitions, so frames end at more partition boundaries
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
                    %1$s OVER (w ROWS BETWEEN start_offset PRECEDING AND end_offset PRECEDING),
                    %1$s OVER (w ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key, second_partition_key ORDER BY order_key %3$s, second_order_key, row_id)
                """.formatted(function, TIES.withOrderKey(BIGINT), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#AGGREGATIONS")
    void testRowsAndRangeFramesInOneWindow(String function)
    {
        // Functions over one specification share a WindowNode, which then needs both partition ends for ROWS frames
        // and peer groups for the default RANGE frame and rank(). Ties in the order key keep the peer groups non-trivial,
        // and a ROWS frame of the whole partition is deterministic despite them.
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER w,
                    %1$s OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    rank() OVER w
                FROM %2$s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key)
                """.formatted(function, TIES.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#AGGREGATIONS")
    void testOffsetsPastPartitionAndInput(String function)
    {
        // Offsets far past the ends of the partition are clamped to it, and those near Long.MAX_VALUE would overflow
        // when added to the row index. Without PARTITION BY, the partition is the whole input.
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key, row_id %3$s),
                    %1$s OVER (ORDER BY order_key, row_id %3$s),
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key, row_id %4$s),
                    %1$s OVER (ORDER BY order_key, row_id %4$s),
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key, row_id %5$s),
                    %1$s OVER (ORDER BY order_key, row_id %5$s)
                FROM %2$s
                """.formatted(
                        function,
                        TIES.withOrderKey(BIGINT),
                        "ROWS BETWEEN 9223372036854775807 PRECEDING AND 9223372036854775807 FOLLOWING",
                        "ROWS BETWEEN 9223372036854775807 - start_offset FOLLOWING AND 9223372036854775807 FOLLOWING",
                        "ROWS BETWEEN 100000 PRECEDING AND 9223372036854775806 PRECEDING")))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @FieldSource("io.trino.operator.gpu.GpuTestUtils#TESTED_GPU_TYPES")
    void testEdgeValuePartitionKeysWithFrames(Type partitionKeyType)
    {
        // Frames are clamped to the partition, which is found by comparing partition keys, so every type is tested
        assertThat(query(
                """
                SELECT
                    row_id,
                    avg(argument_decimal_38_6) OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
                    avg(argument_decimal_38_6) OVER (w ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
                FROM %s
                WINDOW w AS (PARTITION BY partition_key ORDER BY row_id)
                """.formatted(EDGE_VALUES.withPartitionKey(partitionKeyType))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest
    @MethodSource("io.trino.tests.GpuWindowFrameTestParameters#frameSumFunctionsAndDecimalSumTypes")
    void testDecimalSumExceedsDeclaredPrecision(String function, DecimalType type)
    {
        // Values close to the maximum of the type add up past its precision, and for short decimals past the 64 bits
        // they are stored in. Frames that start mid-partition take the difference of two such prefix sums.
        BigInteger max = TEN.pow(type.getPrecision()).subtract(ONE);
        for (BigInteger value : List.of(max, max.negate())) {
            List<BigInteger> values = ImmutableList.<BigInteger>builder()
                    .add(value.subtract(BigInteger.valueOf(9L * value.signum()))) // to keep the divisions inexact
                    .addAll(nCopies(9, value))
                    .build();
            assertDecimalAggregationExecutesWithGpu(function, List.of(type), partitions(List.of(values)), "ASC NULLS LAST");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 6, 38})
    void testAvgDecimalSumExceeds128Bits(int scale)
    {
        // Two values near the decimal(38, s) maximum add up past the signed 128-bit range, but their average is
        // still a valid decimal(38, s). Sliding frames over the first two rows and over the last two overflow
        // 128 bits. The frame from the third row to the end is the difference of prefix sums that both overflow,
        // since the cancelling rows in between return the prefix sum to a small value.
        BigInteger max = TEN.pow(38).subtract(ONE);
        BigInteger nearMax = max.subtract(BigInteger.valueOf(7));
        for (BigInteger sign : List.of(ONE, ONE.negate())) {
            List<BigInteger> values = Stream.of(max, nearMax, max.negate(), nearMax.negate(), BigInteger.valueOf(3), max, max)
                    .map(value -> value.multiply(sign))
                    .collect(toImmutableList());
            assertDecimalAggregationExecutesWithGpu("avg", List.of(createDecimalType(38, scale)), partitions(List.of(values)), "ASC NULLS LAST");
        }
    }

    @ParameterizedTest
    @MethodSource("io.trino.tests.GpuWindowFrameTestParameters#decimalPrecisions")
    void testAvgDecimalHalfUpRounding(int precision)
    {
        // Every scale of the precision is tested, so that avg is compiled for every decimal type. Unscaled values
        // stay below 10, so that they are valid for every type. A sum of 3 over two rows lands exactly halfway,
        // which HALF_UP rounds away from zero for both signs, and 7 / 3 rounds towards zero and 8 / 3 away from it.
        List<DecimalType> types = IntStream.rangeClosed(0, precision)
                .mapToObj(scale -> createDecimalType(precision, scale))
                .collect(toImmutableList());
        assertDecimalAggregationExecutesWithGpu(
                "avg",
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

    @ParameterizedTest
    @FieldSource("io.trino.tests.GpuWindowFrameTestParameters#FRAME_SUM_FUNCTIONS")
    void testDecimalFrameWithOnlyNulls(String function)
    {
        // In the first partition, NULLs sit at the start, in the middle and at the end, so that frames anchored at
        // either end of the partition, as well as sliding ones, hold NULLs only. The second partition holds NULLs only.
        assertDecimalAggregationExecutesWithGpu(
                function,
                FRAME_SUM_DECIMAL_TYPES,
                partitions(List.of(
                        Arrays.asList(null, null, BigInteger.valueOf(2), null, null, BigInteger.valueOf(4), null, null),
                        Arrays.asList(null, null))),
                "ASC NULLS LAST");
    }

    @ParameterizedTest
    @MethodSource("io.trino.tests.GpuWindowFrameTestParameters#frameSumFunctionsAndOrderings")
    void testDecimalFrameWithOnlyNullsAndNullOrderKeys(String function, String ordering)
    {
        // Rows with a NULL order key open the partition with NULLS FIRST and close it with NULLS LAST. Unlike
        // in RANGE frames, they are not peers, so frames include only some of them. The second partition holds
        // NULL order keys and NULL values only.
        assertDecimalAggregationExecutesWithGpu(
                function,
                FRAME_SUM_DECIMAL_TYPES,
                List.of(
                        decimalRow(0L, null, null),
                        decimalRow(0L, null, null),
                        decimalRow(0L, 1L, BigInteger.valueOf(2)),
                        decimalRow(0L, 2L, null),
                        decimalRow(0L, 2L, BigInteger.valueOf(4)),
                        decimalRow(1L, null, null),
                        decimalRow(1L, null, null)),
                ordering);
    }

    private void assertDecimalAggregationExecutesWithGpu(String function, List<DecimalType> types, List<GpuWindowFrameTestParameters.DecimalRow> rows, String ordering)
    {
        // The row ID breaks ties between order keys, so that ROWS frames are deterministic
        List<String> windows = DECIMAL_AGGREGATION_FRAMES.stream()
                .flatMap(frame -> Stream.of(
                        "PARTITION BY partition_key ORDER BY order_key %s, row_id %s".formatted(ordering, frame),
                        "ORDER BY order_key %s, row_id %s".formatted(ordering, frame)))
                .collect(toImmutableList());
        assertThat(query(decimalAggregationQuery(function, types, rows, windows)))
                .executesAllWithGpu(WindowNode.class);
    }

    static Stream<Arguments> framesAndTables()
    {
        return cartesianProduct(FRAMES, tables());
    }

    static List<GpuWindowTable> tables()
    {
        return List.of(MIXED, TIES, SINGLE_ROW, ALL_NULLS, EDGE_VALUES, EMPTY);
    }

    static Stream<Arguments> orderKeyTypesAndOrderings()
    {
        return cartesianProduct(TESTED_GPU_TYPES, ORDERINGS);
    }

    static Stream<Arguments> aggregationsAndOrderings()
    {
        return cartesianProduct(AGGREGATIONS, ORDERINGS);
    }
}
