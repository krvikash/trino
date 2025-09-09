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
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
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

            String ctasTable = hiveTableName + "_ctas";
            onTrino().executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(ctasTable, trinoTableName));
            assertThat(onTrino().executeQuery("SELECT id, date_col from %s".formatted(ctasTable))).containsOnly(EXPECTED_ROWS);
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

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyLeapYearDateTableCompatibility()
    {
        String hiveTableName = "test_hive_parquet_legacy_leap_year_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian=false");
            onHive().executeQuery("CREATE TABLE %s.%s (id int, date_col date) STORED AS PARQUET ".formatted(SCHEMA, hiveTableName));
            onHive().executeQuery("INSERT INTO %s.%s VALUES (1, '1000-02-29'), (2,'1600-02-29'), (3,'1700-02-29'), (4,'2000-02-29')".formatted(SCHEMA, hiveTableName));

            // Hive cannot accept Julian leap year dates
            QueryAssert.Row[] expectedRows = {
                    row(1, null),
                    row(2, Date.valueOf("1600-02-29")),
                    row(3, null),
                    row(4, Date.valueOf("2000-02-29"))};

            assertThat(onHive().executeQuery("SELECT id, date_col FROM " + hiveTableName)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(expectedRows);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetHandlePushdownWhenLegacyDate()
    {
        String hiveTableName = "test_hive_parquet_handle_pushdown_when_legacy_date_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian=false");
            onHive().executeQuery("CREATE TABLE %s.%s (date_col date) STORED AS PARQUET ".formatted(SCHEMA, hiveTableName));
            onHive().executeQuery("INSERT INTO %s.%s VALUES ('0001-01-01')".formatted(SCHEMA, hiveTableName));

            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE date_col = DATE '0001-01-01'"))
                    .containsOnly(row(1));
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetHandlePushdownWhenLegacyDateComplexStructureNotSupported()
    {
        String hiveTableName = "test_hive_parquet_handle_pushdown_when_legacy_date_complex%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            onTrino().executeQuery("SET SESSION hive.timestamp_precision = '%s'".formatted(MILLISECONDS));
            onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian=false");
            onHive().executeQuery("""
                    CREATE TABLE %s.%s(
                    id INT,
                    date_struct STRUCT<id:INT, date_col:DATE, tmst:TIMESTAMP>,
                    dates ARRAY<DATE>,
                    date_map MAP<DATE, INT>,
                    timestamps ARRAY<TIMESTAMP>,
                    timestamp_map MAP<TIMESTAMP, INT>,
                    dates_timestamps ARRAY<STRUCT<date_col:DATE, tmst:TIMESTAMP>>)
                    STORED AS PARQUET""".formatted(SCHEMA, hiveTableName));

            onHive().executeQuery("""
                    INSERT INTO %s.%s VALUES
                    (
                        1,
                        named_struct('id', 1, 'date_col', DATE '0001-01-01', 'tmst', TIMESTAMP '0001-01-01 00:00:00'),
                        array(DATE '0001-01-01', DATE '2022-04-13'),
                        map(DATE '0001-01-01', 1, DATE '2022-04-13', 2),
                        array(TIMESTAMP '0001-01-01 00:00:00', TIMESTAMP '2022-04-13 12:34:56.789') ,
                        map(TIMESTAMP '0001-01-01 00:00:00', 1, TIMESTAMP '2022-04-13 12:34:56.789', 2),
                        array(named_struct('date_col', DATE '0001-01-01', 'tmst', TIMESTAMP '0001-01-01 00:00:00'),
                              named_struct('date_col', DATE '2022-04-13', 'tmst', TIMESTAMP '2022-04-13 12:34:56.789'))
                    )
                    """.formatted(SCHEMA, hiveTableName));

            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE date_struct.date_col = DATE '0001-01-01'")).hasNoRows();
            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE date_struct.tmst = TIMESTAMP '0001-01-01 00:00:00'")).hasNoRows();
            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE contains(dates, DATE '0001-01-01')")).hasNoRows();
            assertQueryFailure(() -> onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE date_map[DATE '0001-01-01'] = 1;")).hasMessageContaining("Key not present in map: 0001-01-01");
            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE contains(timestamps, TIMESTAMP '0001-01-01 00:00:00')")).hasNoRows();
            assertQueryFailure(() -> onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " WHERE timestamp_map[TIMESTAMP '0001-01-01 00:00:00'] = 1;")).hasMessageContaining("Key not present in map: 0001-01-01");
            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " CROSS JOIN  UNNEST(" + trinoTableName + ".dates_timestamps) as arr(d,t)  where d = DATE '0001-01-01';")).hasNoRows();
            assertThat(onTrino().executeQuery("SELECT 1 FROM " + trinoTableName + " CROSS JOIN  UNNEST(" + trinoTableName + ".dates_timestamps) as arr(d,t)  where t = TIMESTAMP '0001-01-01 00:00:00.000';")).hasNoRows();
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

            String ctasTable = hiveTableName + "_ctas";
            onTrino().executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(ctasTable, trinoTableName));
            assertThat(onTrino().executeQuery("SELECT id, date_col from %s".formatted(ctasTable))).containsOnly(EXPECTED_ROWS);
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

            String ctasTable = hiveTableName + "_ctas";
            onTrino().executeQuery("CREATE TABLE %s AS SELECT * FROM %s".formatted(ctasTable, trinoTableName));
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM %s".formatted(ctasTable))).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }
}
