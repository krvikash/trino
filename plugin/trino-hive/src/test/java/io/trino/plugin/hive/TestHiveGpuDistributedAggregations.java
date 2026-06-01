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
package io.trino.plugin.hive;

import io.trino.FeaturesConfig;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGpuDistributedAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        checkState(
                !new FeaturesConfig().isGpuExecution(),
                "Otherwise %s would be the GPU test and this class redundant",
                TestHiveConnectorTest.class);

        return HiveQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .build();
    }

    // By not using VALUES as in super.testSumDecimalOverflow, we're avoiding
    //  - PushAggregationIntoValues
    //  - aggregation being SINGLE step over VALUES
    @Test
    public void testSumDecimalOverflowWithTable()
    {
        // max DECIMAL(38,0)
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 0))",
                List.of(
                        "DECIMAL '99999999999999999999999999999999999999'",
                        "DECIMAL '99999999999999999999999999999999999999'"))) {
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 0))",
                List.of(
                        "DECIMAL '-99999999999999999999999999999999999999'",
                        "DECIMAL '-99999999999999999999999999999999999999'"))) {
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        // max DECIMAL(38,10)
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 10))",
                List.of(
                        "DECIMAL '9999999999999999999999999999.9999999999'",
                        "DECIMAL '9999999999999999999999999999.9999999999'"))) {
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 10))",
                List.of(
                        "DECIMAL '-9999999999999999999999999999.9999999999'",
                        "DECIMAL '-9999999999999999999999999999.9999999999'"))) {
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }

        // Overflow after adding couple values
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 0))",
                List.of(
                        "DECIMAL '45000000000000000000000000000000000000'",
                        "DECIMAL '45000000000000000000000000000000000000'",
                        "DECIMAL '45000000000000000000000000000000000000'",
                        "DECIMAL '45000000000000000000000000000000000000'"))) {
            assertThat(query("SELECT sum(v) FROM (SELECT v FROM " + table.getName() + " LIMIT 2)"))
                    .matches("VALUES DECIMAL '90000000000000000000000000000000000000'");
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 0))",
                List.of(
                        "DECIMAL '-45000000000000000000000000000000000000'",
                        "DECIMAL '-45000000000000000000000000000000000000'",
                        "DECIMAL '-45000000000000000000000000000000000000'",
                        "DECIMAL '-45000000000000000000000000000000000000'"))) {
            assertThat(query("SELECT sum(v) FROM (SELECT v FROM " + table.getName() + " LIMIT 2)"))
                    .matches("VALUES DECIMAL '-90000000000000000000000000000000000000'");
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        // same with non-zero scale: DECIMAL(38,10)
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 10))",
                List.of(
                        "DECIMAL '4500000000000000000000000000.0000000000'",
                        "DECIMAL '4500000000000000000000000000.0000000000'",
                        "DECIMAL '4500000000000000000000000000.0000000000'",
                        "DECIMAL '4500000000000000000000000000.0000000000'"))) {
            assertThat(query("SELECT sum(v) FROM (SELECT v FROM " + table.getName() + " LIMIT 2)"))
                    .matches("VALUES DECIMAL '9000000000000000000000000000.0000000000'");
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sum_decimal_overflow",
                "(v DECIMAL(38, 10))",
                List.of(
                        "DECIMAL '-4500000000000000000000000000.0000000000'",
                        "DECIMAL '-4500000000000000000000000000.0000000000'",
                        "DECIMAL '-4500000000000000000000000000.0000000000'",
                        "DECIMAL '-4500000000000000000000000000.0000000000'"))) {
            assertThat(query("SELECT sum(v) FROM (SELECT v FROM " + table.getName() + " LIMIT 2)"))
                    .matches("VALUES DECIMAL '-9000000000000000000000000000.0000000000'");
            assertThat(query("SELECT sum(v) FROM " + table.getName()))
                    .failure().hasMessageContaining("Decimal overflow");
        }
    }
}
