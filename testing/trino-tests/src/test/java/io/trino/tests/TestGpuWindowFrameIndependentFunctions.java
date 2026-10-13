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
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.tests.GpuWindowFrameTestParameters.ORDERINGS;
import static io.trino.tests.GpuWindowFrameTestParameters.cartesianProduct;
import static io.trino.tests.GpuWindowTable.ALL_NULLS;
import static io.trino.tests.GpuWindowTable.EMPTY;
import static io.trino.tests.GpuWindowTable.MIXED;
import static io.trino.tests.GpuWindowTable.SINGLE_ROW;
import static io.trino.tests.GpuWindowTable.TIES;
import static io.trino.tests.GpuWindowTable.secondOrderKeyType;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowFrameIndependentFunctions
        extends AbstractTestQueryFramework
{
    private static final List<String> PEER_RANKING_FUNCTIONS = List.of(
            "rank()",
            "dense_rank()");
    private static final List<String> POSITION_RANKING_FUNCTIONS = List.of(
            "row_number()");

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

    @ParameterizedTest(name = "{0} {1} {2} {3}")
    @MethodSource("rankingFunctionsTablesOrderKeyTypesAndOrderings")
    void testRankingFunctions(String function, GpuWindowTable table, Type orderKeyType, String ordering)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key %3$s%4$s),
                    %1$s OVER (ORDER BY order_key %3$s%4$s)
                FROM %2$s
                """.formatted(function, table.withOrderKey(orderKeyType), ordering, tieBreaker(function))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("peerRankingFunctionsAndPartitionKeyTypes")
    void testNoOrderBy(String function, Type partitionKeyType)
    {
        // Without ORDER BY, all rows of a partition are peers
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
    @FieldSource("io.trino.tests.TestGpuWindowFrameIndependentFunctions#PEER_RANKING_FUNCTIONS")
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
    @FieldSource("io.trino.tests.TestGpuWindowFrameIndependentFunctions#POSITION_RANKING_FUNCTIONS")
    void testPositionRankingFunctionsNoOrderBy(String function)
    {
        // Alone, a function without ORDER BY is planned as a RowNumberNode, which runs on the CPU. Rows are numbered
        // in any order, so row_id is not selected.
        assertThat(query(
                """
                SELECT
                    partition_key,
                    %1$s OVER (PARTITION BY partition_key)
                FROM %2$s
                """.formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesWithoutGpu();
        assertThat(query("SELECT %s OVER () FROM %s".formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesWithoutGpu();
    }

    @ParameterizedTest
    @MethodSource("rankingFunctions")
    void testNoSourceColumns(String function)
    {
        // Alone, a position ranking function without PARTITION BY and ORDER BY may be planned as a RowNumberNode
        // rather than a window, so rank() is computed next to it
        assertThat(query("SELECT %s OVER (), rank() OVER () FROM %s".formatted(function, MIXED.withOrderKey(BIGINT))))
                .executesAllWithGpu(WindowNode.class);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("peerRankingFunctionsAndOrderings")
    void testPeerRankingFunctionsWithVarbinaryOrderKey(String function, String ordering)
    {
        // The GPU compares VARBINARY peers for aggregations, but not for ranking functions
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key %3$s)
                FROM %2$s
                """.formatted(function, MIXED.withOrderKey(VARBINARY), ordering)))
                .executesWithoutGpu();
    }

    @ParameterizedTest(name = "{0} {1} {2} {3}")
    @MethodSource("rankingFunctionsOrderKeyTypesAndOrderingPairs")
    void testMultipleKeys(String function, Type orderKeyType, String ordering, String secondOrdering)
    {
        // The second keys subdivide partitions and peer groups
        String sql =
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key, second_partition_key ORDER BY order_key %3$s, second_order_key %4$s%5$s)
                FROM %2$s
                """.formatted(function, TIES.withOrderKey(orderKeyType), ordering, secondOrdering, tieBreaker(function));
        if (PEER_RANKING_FUNCTIONS.contains(function) && (orderKeyType.equals(VARBINARY) || secondOrderKeyType(orderKeyType).equals(VARBINARY))) {
            assertThat(query(sql))
                    .executesWithoutGpu();
        }
        else {
            assertThat(query(sql))
                    .executesAllWithGpu(WindowNode.class);
        }
    }

    @ParameterizedTest(name = "{0} {1} {2}")
    @MethodSource("tablesOrderKeyTypesAndOrderings")
    void testAllFunctions(GpuWindowTable table, Type orderKeyType, String ordering)
    {
        // Functions of different kinds are computed by different kernels, and their results must come back in the
        // order of the functions. The order is total, so that row_number is deterministic.
        String functions = rankingFunctions().stream()
                .map(function -> function + " OVER w")
                .collect(joining(",\n"));
        assertThat(query(
                """
                SELECT
                    row_id,
                %s
                FROM %s
                WINDOW w AS (PARTITION BY partition_key ORDER BY order_key %s, row_id)
                """.formatted(functions.indent(4).stripTrailing(), table.withOrderKey(orderKeyType), ordering)))
                .executesAllWithGpu(WindowNode.class);
    }

    static Stream<Arguments> rankingFunctionsTablesOrderKeyTypesAndOrderings()
    {
        // Peer groups are found by comparing order keys, so ties are tested with every key type
        List<GpuWindowTable> tables = List.of(MIXED, TIES, SINGLE_ROW, ALL_NULLS, EMPTY);
        return Stream.of(
                        cartesianProduct(PEER_RANKING_FUNCTIONS, tables, withoutVarbinary(TESTED_GPU_TYPES), ORDERINGS),
                        cartesianProduct(POSITION_RANKING_FUNCTIONS, tables, TESTED_GPU_TYPES, ORDERINGS))
                .flatMap(identity());
    }

    static Stream<Arguments> rankingFunctionsOrderKeyTypesAndOrderingPairs()
    {
        return Stream.of(
                        cartesianProduct(PEER_RANKING_FUNCTIONS, TESTED_GPU_TYPES, ORDERINGS, ORDERINGS),
                        cartesianProduct(POSITION_RANKING_FUNCTIONS, TESTED_GPU_TYPES, ORDERINGS, ORDERINGS))
                .flatMap(identity());
    }

    static Stream<Arguments> peerRankingFunctionsAndPartitionKeyTypes()
    {
        return cartesianProduct(PEER_RANKING_FUNCTIONS, TESTED_GPU_TYPES);
    }

    static Stream<Arguments> peerRankingFunctionsAndOrderings()
    {
        return cartesianProduct(PEER_RANKING_FUNCTIONS, ORDERINGS);
    }

    static List<String> rankingFunctions()
    {
        return ImmutableList.<String>builder()
                .addAll(PEER_RANKING_FUNCTIONS)
                .addAll(POSITION_RANKING_FUNCTIONS)
                .build();
    }

    static Stream<Arguments> tablesOrderKeyTypesAndOrderings()
    {
        return cartesianProduct(List.of(MIXED, TIES, ALL_NULLS), withoutVarbinary(TESTED_GPU_TYPES), ORDERINGS);
    }

    // Position ranking functions number peers in any order, so the row ID breaks ties between them. Peer ranking
    // functions are tested without it, so that their results depend on ties.
    private static String tieBreaker(String function)
    {
        if (POSITION_RANKING_FUNCTIONS.contains(function)) {
            return ", row_id";
        }
        return "";
    }

    // Peer ranking functions over a VARBINARY order key run on the CPU, see testPeerRankingFunctionsWithVarbinaryOrderKey
    private static List<Type> withoutVarbinary(List<Type> types)
    {
        return types.stream()
                .filter(type -> !type.equals(VARBINARY))
                .collect(toImmutableList());
    }
}
