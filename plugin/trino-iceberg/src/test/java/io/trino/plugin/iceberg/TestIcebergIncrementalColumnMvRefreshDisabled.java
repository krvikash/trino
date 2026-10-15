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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the {@code incremental_column} MV property is hidden and has no
 * effect when {@code iceberg.materialized-view-incremental-column-refresh.enabled=false}.
 */
public class TestIcebergIncrementalColumnMvRefreshDisabled
        extends AbstractTestQueryFramework
{
    private static final String ENABLED_CATALOG = "iceberg_incremental_column_enabled";
    private static final String TEST_SCHEMA = "incremental_column_disabled_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.materialized-view-incremental-column-refresh.enabled", "false"))
                .build();
        queryRunner.createCatalog(ENABLED_CATALOG, "iceberg", ImmutableMap.of(
                "iceberg.materialized-view-incremental-column-refresh.enabled", "true",
                "hive.metastore.catalog.dir", "local:///" + ICEBERG_CATALOG));
        queryRunner.execute("CREATE SCHEMA " + ICEBERG_CATALOG + "." + TEST_SCHEMA);
        return queryRunner;
    }

    @AfterAll
    public final void cleanupSchema()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + TEST_SCHEMA + " CASCADE");
    }

    @Test
    public void testIncrementalColumnPropertyHiddenWhenFlagOff()
    {
        String source = TEST_SCHEMA + ".flag_off_src_" + randomNameSuffix();
        String mvName = TEST_SCHEMA + ".flag_off_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW " + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts FROM " + source,
                ".*Catalog 'iceberg' materialized view property 'incremental_column' does not exist.*");
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testIncrementalColumnPropertyNotListedWhenFlagOff()
    {
        assertThat(computeActual("SELECT property_name FROM system.metadata.materialized_view_properties WHERE catalog_name = '" + ICEBERG_CATALOG + "'").getOnlyColumnAsSet())
                .contains("partitioning")
                .doesNotContain("incremental_column");
        assertThat(computeActual("SELECT property_name FROM system.metadata.materialized_view_properties WHERE catalog_name = '" + ENABLED_CATALOG + "'").getOnlyColumnAsSet())
                .contains("incremental_column");
    }

    @Test
    public void testExistingIncrementalColumnIgnoredWhenFlagOff()
    {
        String source = TEST_SCHEMA + ".existing_src_" + randomNameSuffix();
        String mvName = TEST_SCHEMA + ".existing_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + ENABLED_CATALOG + "." + mvName +
                        " WITH (incremental_column = 'ts') AS SELECT id, ts FROM " + ICEBERG_CATALOG + "." + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + ENABLED_CATALOG + "." + mvName, 2);

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + ENABLED_CATALOG + "." + mvName))
                .contains("incremental_column = 'ts'");
        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + mvName))
                .doesNotContain("incremental_column =");

        // ts below the current watermark: an incremental_column refresh would skip this row
        assertUpdate("INSERT INTO " + source + " VALUES (3, 50)", 1);
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);
        assertThat(query("SELECT id, ts FROM " + mvName))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '50')");

        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }

    @Test
    public void testRefreshFallsBackToFullWhenFlagOff()
    {
        // Without incremental_column, a plain MV still refreshes normally (full/incremental
        // plan-shape heuristic). This is a regression guard — the flag-off state must not
        // break standard MVs.
        String source = TEST_SCHEMA + ".fallback_src_" + randomNameSuffix();
        String mvName = TEST_SCHEMA + ".fallback_mv_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, ts BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);
        assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT id, ts FROM " + source);
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 2);
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200')");
        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        // Snapshot-based incremental refresh writes only the newly added row.
        assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 1);
        assertThat(query("SELECT id, ts FROM " + mvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");
        assertUpdate("DROP MATERIALIZED VIEW " + mvName);
        assertUpdate("DROP TABLE " + source);
    }
}
