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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogEndpointInvocations.RestCatalogEndpoint.LOAD_TABLE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogEndpointInvocations.RestCatalogEndpoint.LOAD_VIEW;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogEndpointInvocations.assertRestCatalogInvocations;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // cache and stats are global
abstract class BaseIcebergRestCatalogMetastoreCacheTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return configureQueryRunner()
                .addIcebergProperty("iceberg.rest-catalog.metastore-cache.ttl", "10m")
                .build();
    }

    protected abstract IcebergQueryRunner.Builder configureQueryRunner()
            throws Exception;

    @Test
    void testSelect()
    {
        try (TestTable table = testTable("test_select_cache_")) {
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testInsert()
    {
        try (TestTable table = testTable("test_insert_values_cache_")) {
            assertInvocations(() -> assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1),
                    ImmutableMultiset.builder()
                            .addCopies(LOAD_TABLE, 2)
                            .add(LOAD_VIEW)
                            .build());
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1, 2"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1, 2"),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testAlterTable()
    {
        try (TestTable table = testTable("test_alter_cache_")) {
            assertInvocations(
                    () -> assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b bigint"),
                    ImmutableMultiset.of(LOAD_TABLE));
            assertInvocations(() -> assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 30)", 1),
                    ImmutableMultiset.builder()
                            .addCopies(LOAD_TABLE, 2)
                            .add(LOAD_VIEW)
                            .build());
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, NULL), (3, 30)"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, NULL), (3, 30)"),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testDropTable()
    {
        String table = "test_drop_cache_" + randomNameSuffix();

        assertInvocations(
                () -> assertUpdate("CREATE TABLE " + table + " (a bigint)"),
                ImmutableMultiset.of(LOAD_TABLE));
        assertInvocations(() -> assertUpdate("INSERT INTO " + table + " VALUES 1", 1),
                ImmutableMultiset.builder()
                        .addCopies(LOAD_TABLE, 2)
                        .add(LOAD_VIEW)
                        .build());
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + table, "VALUES 1"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertUpdate("DROP TABLE " + table),
                ImmutableMultiset.of());

        assertUpdate("DROP TABLE IF EXISTS " + table);
    }

    @Test
    void testInsertSelect()
    {
        try (TestTable source = testTable("test_insert_select_source_cache_");
                TestTable target = new TestTable(getQueryRunner()::execute, "test_insert_select_target_cache_", "(a bigint)")) {
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + source.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> computeActual("SELECT * FROM " + target.getName()),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

            assertInvocations(
                    () -> assertUpdate("INSERT INTO " + target.getName() + " SELECT * FROM " + source.getName(), 1),
                    ImmutableMultiset.builder()
                            .add(LOAD_TABLE)
                            .build());
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + target.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + source.getName(), "VALUES 1"),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testCreateTableAsSelect()
    {
        String target = "test_ctas_cache_" + randomNameSuffix();

        try (TestTable source = testTable("test_ctas_source_cache_")) {
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + source.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertUpdate("CREATE TABLE " + target + " AS SELECT * FROM " + source.getName(), 1),
                    ImmutableMultiset.builder()
                            .addCopies(LOAD_TABLE, 2)
                            .build());
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + target, "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + source.getName(), "VALUES 1"),
                    ImmutableMultiset.of());
        }

        assertUpdate("DROP TABLE IF EXISTS " + target);
    }

    @Test
    void testCommentOnTableInvalidatesCache()
    {
        try (TestTable table = testTable("test_comment_cache_")) {
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'test comment'"),
                    ImmutableMultiset.of(LOAD_TABLE));

            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"),
                    ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testRenameTableInvalidatesCache()
    {
        String originalName = "test_rename_cache_original_" + randomNameSuffix();
        String newName = "test_rename_cache_new_" + randomNameSuffix();

        assertInvocations(
                () -> assertUpdate("CREATE TABLE " + originalName + " (a bigint)"),
                ImmutableMultiset.of(LOAD_TABLE));
        assertInvocations(() -> assertUpdate("INSERT INTO " + originalName + " VALUES 1", 1),
                ImmutableMultiset.builder()
                        .addCopies(LOAD_TABLE, 2)
                        .add(LOAD_VIEW)
                        .build());

        // Populate cache
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + originalName, "VALUES 1"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        // Rename
        assertInvocations(
                () -> assertUpdate("ALTER TABLE " + originalName + " RENAME TO " + newName),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        // Old name should fail
        assertInvocations(
                () -> assertQueryFails("SELECT * FROM " + originalName, ".*does not exist"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        // New name should miss
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + newName, "VALUES 1"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        // Subsequent SELECT should hit
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + newName, "VALUES 1"),
                ImmutableMultiset.of());

        assertUpdate("DROP TABLE IF EXISTS " + originalName);
        assertUpdate("DROP TABLE IF EXISTS " + newName);
    }

    @Test
    void testSystemTablesShareCacheEntry()
    {
        try (TestTable table = testTable("test_system_tables_cache_")) {
            assertInvocations(
                    () -> computeActual("SELECT * FROM \"" + table.getName() + "$snapshots\""),
                    ImmutableMultiset.of(LOAD_TABLE));
            assertInvocations(
                    () -> computeActual("SELECT * FROM \"" + table.getName() + "$history\""),
                    ImmutableMultiset.of());

            assertInvocations(
                    () -> assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1),
                    ImmutableMultiset.of(LOAD_TABLE));

            assertInvocations(
                    () -> computeActual("SELECT * FROM \"" + table.getName() + "$snapshots\""),
                    ImmutableMultiset.of(LOAD_TABLE));
            assertInvocations(
                    () -> computeActual("SELECT * FROM \"" + table.getName() + "$history\""),
                    ImmutableMultiset.of());
        }
    }

    @Test
    void testSelectFromView()
    {
        String view = "test_view_select_cache_" + randomNameSuffix();

        assertInvocations(
                () -> assertUpdate("CREATE VIEW " + view + " AS SELECT BIGINT '1' AS id"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                ImmutableMultiset.of(LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                ImmutableMultiset.of());

        assertUpdate("DROP VIEW IF EXISTS " + view);
    }

    @Test
    void testSelectFromViewOverTable()
    {
        String view = "test_view_over_table_view_" + randomNameSuffix();

        try (TestTable table = testTable("test_view_over_table_cache_")) {
            assertInvocations(
                    () -> assertUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + table.getName()),
                    ImmutableMultiset.builder()
                            .addCopies(LOAD_TABLE, 2)
                            .addCopies(LOAD_VIEW, 2)
                            .build());

            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                    ImmutableMultiset.of(LOAD_VIEW));
            assertInvocations(
                    () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                    ImmutableMultiset.of());
        }

        assertUpdate("DROP VIEW IF EXISTS " + view);
    }

    @Test
    void testCreateOrReplaceViewInvalidatesCache()
    {
        String view = "test_view_replace_cache_" + randomNameSuffix();

        assertInvocations(
                () -> assertUpdate("CREATE VIEW " + view + " AS SELECT BIGINT '1' AS id"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                ImmutableMultiset.of(LOAD_VIEW));

        assertInvocations(
                () -> assertUpdate("CREATE OR REPLACE VIEW " + view + " AS SELECT BIGINT '2' AS id"),
                ImmutableMultiset.of());

        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 2"),
                ImmutableMultiset.of(LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 2"),
                ImmutableMultiset.of());

        assertUpdate("DROP VIEW IF EXISTS " + view);
    }

    @Test
    void testDropViewInvalidatesCache()
    {
        String view = "test_view_drop_cache_" + randomNameSuffix();

        assertInvocations(
                () -> assertUpdate("CREATE VIEW " + view + " AS SELECT BIGINT '1' AS id"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 1"),
                ImmutableMultiset.of(LOAD_VIEW));

        assertInvocations(
                () -> assertUpdate("DROP VIEW " + view),
                ImmutableMultiset.of());

        assertInvocations(
                () -> assertUpdate("CREATE VIEW " + view + " AS SELECT BIGINT '9' AS id"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + view, "VALUES 9"),
                ImmutableMultiset.of(LOAD_VIEW));

        assertUpdate("DROP VIEW IF EXISTS " + view);
    }

    @Test
    void testCreateOrReplaceTableInvalidatesCache()
    {
        String table = "test_table_replace_cache_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + table + " AS SELECT BIGINT '1' AS a", 1);
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + table, "VALUES 1"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + table, "VALUES 1"),
                ImmutableMultiset.of());

        assertInvocations(
                () -> assertUpdate("CREATE OR REPLACE TABLE " + table + " AS SELECT BIGINT '2' AS a", 1),
                ImmutableMultiset.of(LOAD_TABLE));

        assertInvocations(
                () -> assertQuery("SELECT * FROM " + table, "VALUES 2"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + table, "VALUES 2"),
                ImmutableMultiset.of());

        assertUpdate("DROP TABLE IF EXISTS " + table);
    }

    @Test
    void testReplaceViewWithTableInvalidatesCache()
    {
        String name = "test_view_to_table_cache_" + randomNameSuffix();

        assertUpdate("CREATE VIEW " + name + " AS SELECT BIGINT '1' AS a");
        assertInvocations(
                () -> assertQuery("SELECT * FROM " + name, "VALUES 1"),
                ImmutableMultiset.of(LOAD_VIEW));

        assertUpdate("DROP VIEW " + name);
        assertUpdate("CREATE TABLE " + name + " (a bigint)");
        assertUpdate("INSERT INTO " + name + " VALUES 2", 1);

        assertInvocations(
                () -> assertQuery("SELECT * FROM " + name, "VALUES 2"),
                ImmutableMultiset.of(LOAD_TABLE, LOAD_VIEW));

        assertUpdate("DROP TABLE IF EXISTS " + name);
        assertUpdate("DROP VIEW IF EXISTS " + name);
    }

    private TestTable testTable(String prefix)
    {
        return new TestTable(getQueryRunner()::execute, prefix, "(a bigint)", List.of("1"));
    }

    private void assertInvocations(Runnable runnable, Multiset<?> expectedInvocations)
    {
        assertRestCatalogInvocations(getDistributedQueryRunner(), runnable, expectedInvocations);
    }
}
