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
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.parseStoredWatermark;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With {@code iceberg.materialized-views.hide-storage-table} disabled, nothing on a storage table points
 * back at the view that owns it, so a refresh only knows which column to watermark because the name is
 * carried over from analysis.
 */
public class TestIcebergIncrementalColumnMvRefreshWithVisibleStorageTable
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "incremental_column_visible_storage_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.materialized-view-incremental-column-refresh.enabled", "true",
                        "iceberg.materialized-views.hide-storage-table", "false"))
                .build();
        queryRunner.execute("CREATE SCHEMA " + ICEBERG_CATALOG + "." + TEST_SCHEMA);
        return queryRunner;
    }

    @AfterAll
    public final void cleanupSchema()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + TEST_SCHEMA + " CASCADE");
    }

    @Test
    public void testIncrementalColumnWatermarkIsStoredOnVisibleStorageTable()
    {
        String source = TEST_SCHEMA + ".visible_storage_src_" + randomNameSuffix();
        String mvName = "visible_storage_mv_" + randomNameSuffix();
        String qualifiedMvName = TEST_SCHEMA + "." + mvName;
        assertUpdate("CREATE TABLE " + source + " (id BIGINT, event_timestamp BIGINT)");
        assertUpdate("INSERT INTO " + source + " VALUES (1, 100), (2, 200)", 2);

        assertUpdate(
                "CREATE MATERIALIZED VIEW " + qualifiedMvName +
                        " WITH (incremental_column = 'event_timestamp') AS SELECT id, event_timestamp FROM " + source);

        // The storage table is a plain table here, not a hidden one named after the view.
        String storageTable = storageTableName(mvName);
        assertThat(storageTable).doesNotContain(mvName);

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 2);
        assertThat(latestStoredWatermarkValue(storageTable)).contains("200");

        assertUpdate("INSERT INTO " + source + " VALUES (3, 300)", 1);
        String plan = getExplainPlan("REFRESH MATERIALIZED VIEW " + qualifiedMvName, ExplainType.Type.DISTRIBUTED);
        assertThat(plan).doesNotContain("CrossJoin");
        String sourceTableName = source.substring(source.lastIndexOf('.') + 1);
        assertThat(plan).containsPattern("ScanFilter\\[table = .*" + sourceTableName + ".*filterPredicate = \\(bigint '200' < event_timestamp\\)]");

        assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMvName, 1);
        assertThat(latestStoredWatermarkValue(storageTable)).contains("300");
        assertThat(query("SELECT id, event_timestamp FROM " + qualifiedMvName + " ORDER BY id"))
                .matches("VALUES (BIGINT '1', BIGINT '100'), (BIGINT '2', BIGINT '200'), (BIGINT '3', BIGINT '300')");

        assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMvName);
        assertUpdate("DROP TABLE " + source);
    }

    private String storageTableName(String mvName)
    {
        MaterializedRow row = getOnlyElement(computeActual(
                "SELECT storage_table FROM system.metadata.materialized_views " +
                        "WHERE catalog_name = '" + ICEBERG_CATALOG + "' AND schema_name = '" + TEST_SCHEMA + "' AND name = '" + mvName + "'")
                .getMaterializedRows());
        return (String) row.getField(0);
    }

    private Optional<String> latestStoredWatermarkValue(String storageTable)
    {
        MaterializedRow row = getOnlyElement(computeActual(
                "SELECT element_at(summary, 'trino-incremental-column-watermark') FROM " + TEST_SCHEMA + ".\"" + storageTable + "$snapshots\" " +
                        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1")
                .getMaterializedRows());
        return Optional.ofNullable((String) row.getField(0)).map(watermark -> parseStoredWatermark(watermark).value());
    }
}
