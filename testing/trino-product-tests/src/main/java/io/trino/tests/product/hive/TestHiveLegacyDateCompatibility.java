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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE4;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveLegacyDateCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hive";
    private static final String SCHEMA = "default";
    private static final Map<Integer, String> ID_TO_DATE = ImmutableMap.of(
            1, "0001-01-01",
            2, "1001-01-01",
            3, "1234-01-01",
            4, "1582-10-05",
            5, "1582-10-04",
            6, "1582-10-10",
            7, "1582-10-14",
            8, "1582-10-15",
            9, "1582-10-16",
            10, "2022-04-13");

    private static final String TABLE_VALUES = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> "(%d, '%s')".formatted(entry.getKey(), entry.getValue()))
            .collect(joining(","));

    private static final List<QueryAssert.Row> EXPECTED_ROWS = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> row(entry.getKey(), Date.valueOf(entry.getValue())))
            .collect(toImmutableList());

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyDateCompatibilityWithHybridCalendar()
    {
        testHiveParquetLegacyDateCompatibility(false);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyDateCompatibilityWithProlepticCalendar()
    {
        testHiveParquetLegacyDateCompatibility(true);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyDatePartitionedTableCompatibilityWithHybridCalendar()
    {
        testHiveParquetLegacyDatePartitionedTableCompatibility(false);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyDatePartitionedTableCompatibilityWithProlepticCalendar()
    {
        testHiveParquetLegacyDatePartitionedTableCompatibility(true);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyDateCompatibilityWithProlepticCalendar()
    {
        testHiveOrcLegacyDateCompatibility(true);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyDateCompatibilityWithHybridCalendar()
    {
        testHiveOrcLegacyDateCompatibility(false);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyDatePartitionedTableCompatibilityWithHybridCalendar()
    {
        testHiveOrcLegacyDatePartitionedTableCompatibility(false);
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyDatePartitionedTableCompatibilityWithProlepticCalendar()
    {
        testHiveOrcLegacyDatePartitionedTableCompatibility(true);
    }

    private void testHiveParquetLegacyDateCompatibility(boolean hiveWritesInProlepticGregorian)
    {
        String hiveTableName = "test_hive_parquet_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian=" + hiveWritesInProlepticGregorian);
            onHive().executeQuery("CREATE TABLE %s.%s (id integer, date_col date) STORED AS PARQUET".formatted(SCHEMA, hiveTableName));
            onHive().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, hiveTableName, TABLE_VALUES));

            assertThat(onHive().executeQuery("SELECT id, date_col FROM " + hiveTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    private void testHiveParquetLegacyDatePartitionedTableCompatibility(boolean hiveWritesInProlepticGregorian)
    {
        String hiveTableName = "test_hive_parquet_legacy_date_compatibility_partitioned_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian=" + hiveWritesInProlepticGregorian);
            onHive().executeQuery("CREATE TABLE %s.%s (id integer) PARTITIONED BY (date_col date) STORED AS PARQUET ".formatted(SCHEMA, hiveTableName));
            onHive().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, hiveTableName, TABLE_VALUES));

            ID_TO_DATE.forEach((id, date) ->
            {
                assertThat(onHive().executeQuery("SELECT id FROM %s WHERE date_col = '%s'".formatted(hiveTableName, date)))
                        .containsOnly(row(id));

                assertThat(onTrino().executeQuery("SELECT id FROM %s WHERE date_col = (DATE '%s')".formatted(trinoTableName, date)))
                        .containsOnly(row(id));
            });
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    private void testHiveOrcLegacyDateCompatibility(boolean hiveWritesInProlepticGregorian)
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("CREATE TABLE %s.%s (id integer, date_col date) STORED AS ORC tblproperties (\"orc.proleptic.gregorian\" = \"%s\")".formatted(SCHEMA, hiveTableName, hiveWritesInProlepticGregorian));
            onHive().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, hiveTableName, TABLE_VALUES));

            assertThat(onHive().executeQuery("SELECT id, date_col FROM " + hiveTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    private void testHiveOrcLegacyDatePartitionedTableCompatibility(boolean hiveWritesInProlepticGregorian)
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_partitioned_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("CREATE TABLE %s.%s (id integer) PARTITIONED BY (date_col date) STORED AS ORC tblproperties (\"orc.proleptic.gregorian\" = \"%s\")".formatted(SCHEMA, hiveTableName, hiveWritesInProlepticGregorian));
            onHive().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, hiveTableName, TABLE_VALUES));

            assertThat(onHive().executeQuery("SELECT id, date_col FROM " + hiveTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }
}
