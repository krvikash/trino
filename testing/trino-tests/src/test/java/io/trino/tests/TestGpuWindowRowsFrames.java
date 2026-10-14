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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.tests.GpuWindowFrameTestParameters.AGGREGATIONS;
import static io.trino.tests.GpuWindowFrameTestParameters.cartesianProduct;
import static io.trino.tests.GpuWindowTable.MIXED;
import static io.trino.tests.GpuWindowTable.TIES;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowRowsFrames
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

    @ParameterizedTest
    @ValueSource(strings = {
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
    })
    void testUnsupportedFrames(String frame)
    {
        assertThat(query(
                """
                SELECT
                    row_id,
                    avg(argument_decimal_38_6) OVER (PARTITION BY partition_key ORDER BY order_key %1$s)
                FROM %2$s
                """.formatted(frame, TIES.withOrderKey(BIGINT))))
                .executesWithoutGpu();
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("aggregationsAndFrames")
    void testAggregations(String function, String frame)
    {
        // The GPU computes aggregations over RANGE frames only
        assertThat(query(
                """
                SELECT
                    row_id,
                    %1$s OVER (PARTITION BY partition_key ORDER BY order_key, row_id %2$s)
                FROM %3$s
                """.formatted(function, frame, MIXED.withOrderKey(BIGINT))))
                .executesWithoutGpu();
    }

    static Stream<Arguments> aggregationsAndFrames()
    {
        return cartesianProduct(
                AGGREGATIONS,
                List.of(
                        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                        "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                        "ROWS BETWEEN start_offset PRECEDING AND end_offset FOLLOWING"));
    }
}
