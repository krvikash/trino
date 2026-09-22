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
package io.trino.plugin.deltalake;

import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.function.IntFunction;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.COMPOSITE_SPLITS_ENABLED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeCompositeSplits
        extends AbstractTestQueryFramework
{
    private static final int FILE_COUNT = 6;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.experimental.composite-splits.enabled", "true")
                .build();
    }

    @Test
    void testUnpartitionedTableBundlesFiles()
    {
        String tableName = "test_composite_unpartitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");
        assertThat(fileCount(tableName)).isEqualTo(FILE_COUNT);

        assertThat(scanSplitCount("SELECT * FROM " + tableName)).isLessThan(FILE_COUNT);
        assertMatchesNonComposite("SELECT id, value FROM " + tableName);
        assertMatchesNonComposite("SELECT count(*), sum(id) FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testPartitionedTableBundlesWithinPartitions()
    {
        String tableName = "test_composite_partitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, part varchar) WITH (partitioned_by = ARRAY['part'])");
        // Partitions interleave in the transaction log
        insertOneFilePerStatement(tableName, "part", i -> "'p" + (i % 3) + "'");
        assertThat(fileCount(tableName)).isEqualTo(FILE_COUNT);

        // One composite per partition
        assertThat(scanSplitCount("SELECT * FROM " + tableName)).isEqualTo(3);
        assertMatchesNonComposite("SELECT id, part FROM " + tableName);
        assertMatchesNonComposite("SELECT part, count(*) FROM " + tableName + " GROUP BY part");
        assertMatchesNonComposite("SELECT id FROM " + tableName + " WHERE part = 'p1'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testNullPartitionValue()
    {
        String tableName = "test_composite_null_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, part varchar) WITH (partitioned_by = ARRAY['part'])");
        insertOneFilePerStatement(tableName, "part", i -> i % 2 == 0 ? "NULL" : "'p'");

        assertMatchesNonComposite("SELECT id, part FROM " + tableName);
        assertMatchesNonComposite("SELECT count(*) FROM " + tableName + " WHERE part IS NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSchemaEvolution()
    {
        String tableName = "test_composite_schema_evolution_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN extra bigint");
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c', 30)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'd', 40)", 1);

        assertQuery("SELECT id, value, extra FROM " + tableName, "VALUES (1, 'a', NULL), (2, 'b', NULL), (3, 'c', 30), (4, 'd', 40)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testColumnMappingModes()
    {
        for (String mode : new String[] {"id", "name"}) {
            String tableName = "test_composite_column_mapping_" + mode + "_" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar) WITH (column_mapping_mode = '" + mode + "')");
            insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN value TO renamed");
            assertUpdate("INSERT INTO " + tableName + " VALUES (100, 'after_rename')", 1);

            assertThat(scanSplitCount("SELECT * FROM " + tableName)).isLessThan(FILE_COUNT + 1);
            assertMatchesNonComposite("SELECT id, renamed FROM " + tableName);

            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testDeletionVectors()
    {
        String tableName = "test_composite_deletion_vectors_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar) WITH (deletion_vectors_enabled = true)");
        for (int i = 0; i < FILE_COUNT; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES (" + (2 * i) + ", 'a'), (" + (2 * i + 1) + ", 'b')", 2);
        }
        assertUpdate("DELETE FROM " + tableName + " WHERE value = 'b'", FILE_COUNT);

        assertQuery("SELECT count(*), sum(id) FROM " + tableName, "VALUES (6, 30)");
        assertMatchesNonComposite("SELECT id, value FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testDml()
    {
        String tableName = "test_composite_dml_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        assertUpdate("UPDATE " + tableName + " SET value = 'updated' WHERE id % 2 = 0", 3);
        assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
        assertUpdate(
                "MERGE INTO " + tableName + " t USING (VALUES (3, 'merged'), (100, 'inserted')) s(id, value) ON t.id = s.id " +
                        "WHEN MATCHED THEN UPDATE SET value = s.value " +
                        "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
                2);

        assertQuery(
                "SELECT id, value FROM " + tableName,
                "VALUES (0, 'updated'), (2, 'updated'), (3, 'merged'), (4, 'updated'), (5, 'v5'), (100, 'inserted')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testOptimize()
    {
        String tableName = "test_composite_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        // OPTIMIZE records scanned files, so it must see every file individually
        Session singleWriter = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", "1")
                .build();
        assertUpdate(singleWriter, "ALTER TABLE " + tableName + " EXECUTE optimize");
        assertThat(fileCount(tableName)).isEqualTo(1);
        assertQuery("SELECT count(*), sum(id) FROM " + tableName, "VALUES (6, 15)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSynthesizedColumnsDisableComposites()
    {
        String tableName = "test_composite_synthesized_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        assertThat(scanSplitCount("SELECT id, \"$path\" FROM " + tableName)).isEqualTo(FILE_COUNT);
        assertThat(scanSplitCount("SELECT id, \"$file_size\" FROM " + tableName)).isEqualTo(FILE_COUNT);
        assertThat(scanSplitCount("SELECT id, \"$file_modified_time\" FROM " + tableName)).isEqualTo(FILE_COUNT);
        assertMatchesNonComposite("SELECT id, \"$path\", \"$file_size\" FROM " + tableName);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testDisabledBySessionProperty()
    {
        String tableName = "test_composite_disabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, value varchar)");
        insertOneFilePerStatement(tableName, "value", i -> "'v" + i + "'");

        assertThat(scanSplitCount(nonCompositeSession(), "SELECT * FROM " + tableName)).isEqualTo(FILE_COUNT);

        assertUpdate("DROP TABLE " + tableName);
    }

    private void insertOneFilePerStatement(String tableName, String secondColumn, IntFunction<String> secondValue)
    {
        for (int i = 0; i < FILE_COUNT; i++) {
            assertUpdate("INSERT INTO " + tableName + " (id, " + secondColumn + ") VALUES (" + i + ", " + secondValue.apply(i) + ")", 1);
        }
    }

    private long fileCount(String tableName)
    {
        return (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + tableName);
    }

    private void assertMatchesNonComposite(@Language("SQL") String sql)
    {
        assertSameRows(getSession(), sql, nonCompositeSession(), sql);
    }

    private void assertSameRows(Session session, @Language("SQL") String sql, Session expectedSession, @Language("SQL") String expectedSql)
    {
        assertThat(computeActual(session, sql).getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(computeActual(expectedSession, expectedSql).getMaterializedRows());
    }

    private Session nonCompositeSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, COMPOSITE_SPLITS_ENABLED, "false")
                .build();
    }

    private long scanSplitCount(@Language("SQL") String sql)
    {
        return scanSplitCount(getSession(), sql);
    }

    private long scanSplitCount(Session session, @Language("SQL") String sql)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, sql);
        return scanOperatorStats(result.queryId()).getTotalDrivers();
    }

    private OperatorStats scanOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("TableScanOperator") || summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .collect(onlyElement());
    }
}
