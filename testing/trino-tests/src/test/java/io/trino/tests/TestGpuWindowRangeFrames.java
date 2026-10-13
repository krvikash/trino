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

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.tests.GpuWindowFrameTestParameters.AGGREGATIONS;
import static io.trino.tests.GpuWindowFrameTestParameters.ORDERINGS;
import static io.trino.tests.GpuWindowFrameTestParameters.cartesianProduct;
import static io.trino.tests.GpuWindowTable.ALL_NULLS;
import static io.trino.tests.GpuWindowTable.EDGE_VALUES;
import static io.trino.tests.GpuWindowTable.EMPTY;
import static io.trino.tests.GpuWindowTable.MIXED;
import static io.trino.tests.GpuWindowTable.SINGLE_ROW;
import static io.trino.tests.GpuWindowTable.TIES;
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
}
