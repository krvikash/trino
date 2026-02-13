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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.plugin.hive.metastore.MetastoreInvocations.assertMetastoreInvocationsForQuery;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.REPLACE_TABLE;

@Execution(ExecutionMode.SAME_THREAD) // metastore invocation counters share mutable state so can't be run from many threads simultaneously
final class TestIcebergCachingHiveCatalogAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 5;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addIcebergProperty("hive.metastore-cache-ttl", "30m")
                .build();
    }

    @Test
    void testSelectFromTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

            // First select populates the cache
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Second select is served from cache
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Insert repopulates cache after commit
            assertUpdate("INSERT INTO test_select_from VALUES ('Alice', 30)", 1);
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Alter table invalidates cache without repopulating it
            assertUpdate("ALTER TABLE test_select_from ADD COLUMN address VARCHAR");
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Comment on table invalidates cache
            assertUpdate("COMMENT ON TABLE test_select_from IS 'test comment'");
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Rename table invalidates cache
            assertUpdate("ALTER TABLE test_select_from RENAME TO test_select_from_renamed");
            assertQueryFails("SELECT * FROM test_select_from", ".*Table '.*test_select_from' does not exist");
            assertMetastoreInvocations("SELECT * FROM test_select_from_renamed",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM test_select_from_renamed",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_renamed");
        }
    }

    @Test
    void testSelectFromView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
            assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

            // First select populates the cache for the view (underlying table is already cached from CREATE VIEW)
            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Second select is served from cache
            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());

            // Insert into underlying table repopulates cache after commit
            assertUpdate("INSERT INTO test_select_view_table VALUES ('Alice', 30)", 1);
            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());

            // Comment on view invalidates its cache entry
            assertUpdate("COMMENT ON VIEW test_select_view_view IS 'test comment'");
            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_view_table");
        }
    }

    @Test
    void testSelectFromMaterializedView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_mview_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table");

            // First refresh creates storage table and populates cache for source table and MV
            assertMetastoreInvocations("REFRESH MATERIALIZED VIEW test_select_mview_view",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .add(REPLACE_TABLE)
                            .build());

            // Select is served from cache (source table and MV cached by refresh, storage table location in query-scope cache)
            assertMetastoreInvocations("SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.of());

            // Insert into source table invalidates its cache entry and makes MV stale
            assertUpdate("INSERT INTO test_select_mview_table VALUES ('Alice', 30)", 1);

            // Refresh repopulates the cache
            assertMetastoreInvocations("REFRESH MATERIALIZED VIEW test_select_mview_view",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .add(REPLACE_TABLE)
                            .build());

            // Select after refresh is served from cache
            assertMetastoreInvocations("SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.of());

            // Rename MV invalidates cache
            assertUpdate("ALTER MATERIALIZED VIEW test_select_mview_view RENAME TO test_select_mview_view_renamed");
            assertQueryFails("SELECT * FROM test_select_mview_view", ".*Table '.*test_select_mview_view' does not exist");
            assertMetastoreInvocations("SELECT * FROM test_select_mview_view_renamed",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM test_select_mview_view_renamed",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_view");
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_view_renamed");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_mview_table");
        }
    }

    @Test
    void testSelectFromSystemTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_snapshots AS SELECT 2 AS age", 1);

            // First select from $history populates cache
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Other system tables are served from cache
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.of());

            // Insert repopulates cache after commit
            assertUpdate("INSERT INTO test_select_snapshots VALUES (3)", 1);
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.of());
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.of());

            // Rename table invalidates cache
            assertUpdate("ALTER TABLE test_select_snapshots RENAME TO test_select_snapshots_renamed");
            assertQueryFails("SELECT * FROM \"test_select_snapshots$history\"", ".*Table '.*test_select_snapshots.*' does not exist");
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots_renamed$history\"",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots_renamed$snapshots\"",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots_renamed");
        }
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocationsForQuery(getDistributedQueryRunner(), getSession(), query, expectedInvocations);
    }
}
