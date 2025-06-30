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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_133;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_143;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_154;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeLegacyDateTimeCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private static final String DELTA_CATALOG = "delta";
    private static final String SCHEMA = "default";
    private static final Map<Integer, String> ID_TO_DATE = ImmutableMap.of(
            1, "0001-01-01",
            2, "1001-01-01",
            3, "1234-01-01",
            4, "1582-10-04",
            5, "1582-10-15",
            6, "1883-11-10",
            7, "1883-11-20",
            8, "1969-12-31",
            9, "1970-01-01",
            10, "2022-04-13");

    private static final String TABLE_VALUES = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> "(%d, DATE '%s')".formatted(entry.getKey(), entry.getValue()))
            .collect(joining(","));

    private static final List<QueryAssert.Row> EXPECTED_ROWS = ID_TO_DATE.entrySet()
            .stream()
            .map(entry -> row(entry.getKey(), Date.valueOf(entry.getValue())))
            .collect(toImmutableList());

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_133, DELTA_LAKE_DATABRICKS_143, DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaParquetLegacyDateCompatibilityWithHybridCalendar()
    {
        testDeltaParquetLegacyDateCompatibility("LEGACY");
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_133, DELTA_LAKE_DATABRICKS_143, DELTA_LAKE_DATABRICKS_154, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaParquetLegacyDateCompatibilityWithProlepticCalendar()
    {
        testDeltaParquetLegacyDateCompatibility("CORRECTED");
    }

    private void testDeltaParquetLegacyDateCompatibility(String rebaseMode)
    {
        String deltaTableName = "test_deltalake_parquet_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = "%s.%s.%s".formatted(DELTA_CATALOG, SCHEMA, deltaTableName);

        try {
            onDelta().executeQuery("CREATE TABLE %s.%s (id integer, date_col date) USING DELTA LOCATION 's3://%s/databricks-compatibility-test-%s'".formatted(SCHEMA, deltaTableName, bucketName, deltaTableName));
            onDelta().executeQuery("SET spark.sql.parquet.datetimeRebaseModeInWrite=%s".formatted(rebaseMode));
            onDelta().executeQuery("INSERT INTO %s.%s VALUES %s".formatted(SCHEMA, deltaTableName, TABLE_VALUES));

            assertThat(onDelta().executeQuery("SELECT id, date_col FROM " + deltaTableName)).containsOnly(EXPECTED_ROWS);
            assertThat(onTrino().executeQuery("SELECT id, date_col FROM " + trinoTableName)).containsOnly(EXPECTED_ROWS);
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + deltaTableName);
        }
    }
}
