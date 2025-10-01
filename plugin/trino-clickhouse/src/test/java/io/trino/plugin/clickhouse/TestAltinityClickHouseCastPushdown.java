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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.ProjectNode;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.ALTINITY_DEFAULT_IMAGE;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAltinityClickHouseCastPushdown
        extends BaseClickHouseCastPushdown
{
    @Override
    protected DockerImageName clickHouseServerImage()
    {
        return ALTINITY_DEFAULT_IMAGE;
    }

    @Override
    @Test // Due to https://github.com/ClickHouse/ClickHouse/pull/83982 cast pushdown from Date to Timestamp is not supported in <25.8 version
    void testMinAndMaxDateCastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("id", "Int32", asList(1, 2))
                .addColumn("c_date", "date", asList("'%s'".formatted(MIN_SUPPORTED_DATE_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATE_VALUE)))
                .execute(onRemoteDatabase(), "tpch.min_max_date_")) {
            assertThat(query("SELECT CAST(c_date AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES (DATE '%s'), (DATE '%s')".formatted(MIN_SUPPORTED_DATE_VALUE, MAX_SUPPORTED_DATE_VALUE))
                    .isFullyPushedDown();

            // https://github.com/ClickHouse/ClickHouse/pull/83982 (Fixed in ClickHouse 25.8)
            assertThat(query("SELECT CAST(c_date AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE_VALUE, "2149-06-06 00:00:00.000"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT CAST(c_date AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE_VALUE, MAX_SUPPORTED_DATE_VALUE))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Override
    @Test // Due to https://github.com/ClickHouse/ClickHouse/pull/83982 cast pushdown from Date to Timestamp is not supported in <25.8 version
    void testMinAndMaxDate32CastPushdown()
    {
        try (CastDataTypeTestTable table = CastDataTypeTestTable.create(2)
                .addColumn("c_date32", "date32", asList("'%s'".formatted(MIN_SUPPORTED_DATE32_VALUE), "'%s'".formatted(MAX_SUPPORTED_DATE32_VALUE)))
                .execute(onRemoteDatabase(), "tpch.min_max_date32_")) {
            assertThat(query("SELECT CAST(c_date32 AS date) FROM %s".formatted(table.getName())))
                    .matches("VALUES DATE '%s', DATE '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, MAX_SUPPORTED_DATE32_VALUE))
                    .isFullyPushedDown();

            // https://github.com/ClickHouse/ClickHouse/pull/83982 (Fixed in ClickHouse 25.8)
            assertThat(query("SELECT CAST(c_date32 AS timestamp) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, "2299-12-31 00:00:00.000"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT CAST(c_date32 AS timestamp(0)) FROM %s".formatted(table.getName())))
                    .matches("VALUES TIMESTAMP '%s', TIMESTAMP '%s'".formatted(MIN_SUPPORTED_DATE32_VALUE, MAX_SUPPORTED_DATE32_VALUE))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Override
    @Test // Due to https://github.com/ClickHouse/ClickHouse/pull/83982 cast pushdown from Date to Timestamp is not supported in <25.8 version
    void testDateToTimestampCastPushdownWithGroupBy()
    {
        assertThat(query("SELECT CAST(c_date AS timestamp) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_date AS timestamp)".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 00:00:00.000', BIGINT '1'), (TIMESTAMP '2019-08-15 00:00:00.000', BIGINT '1'), (null, BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_date32 AS timestamp(0)) as c_timestamp, count(*) FROM %s GROUP BY CAST(c_date32 AS timestamp(0))".formatted(leftTable())))
                .matches("VALUES (TIMESTAMP '2024-09-08 00:00:00', BIGINT '1'), (TIMESTAMP '2019-08-15 00:00:00', BIGINT '1'), (null, BIGINT '1')")
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Override
    protected List<CastTestCase> addCastFromDateToTimestampTestCases()
    {
        return ImmutableList.of();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .addAll(super.unsupportedCastTypePushdown())
                .addAll(super.addCastFromDateToTimestampTestCases())
                .build();
    }
}
