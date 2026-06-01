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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreMethod.UPDATE_TABLE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Execution(SAME_THREAD) // metastore invocation counters share mutable state so can't be run from many threads simultaneously
public abstract class BaseIcebergCachingGlueCatalogAccessOperationsTest
        extends AbstractTestQueryFramework
{
    protected static final int MAX_PREFIXES_COUNT = 5;

    @Test
    void testSelectFromTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

            // First select populates the cache
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Second select is served from cache
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Insert invalidates cache
            assertUpdate("INSERT INTO test_select_from VALUES ('Alice', 30)", 1);
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Alter table invalidates cache without repopulating it
            assertUpdate("ALTER TABLE test_select_from ADD COLUMN address VARCHAR");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Comment on table invalidates cache
            assertUpdate("COMMENT ON TABLE test_select_from IS 'test comment'");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_from",
                    ImmutableMultiset.of());

            // Rename table invalidates cache
            assertUpdate("ALTER TABLE test_select_from RENAME TO test_select_from_renamed");
            assertQueryFails("SELECT * FROM test_select_from", ".*Table '.*test_select_from' does not exist");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_from_renamed",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_from_renamed",
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
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Second select is served from cache
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());

            // Insert invalidates cache
            assertUpdate("INSERT INTO test_select_view_table VALUES ('Alice', 30)", 1);
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());

            // Comment on view invalidates its cache entry
            assertUpdate("COMMENT ON VIEW test_select_view_view IS 'test comment'");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_view_view",
                    ImmutableMultiset.of());

            // Rename view invalidates cache
            assertUpdate("ALTER VIEW test_select_view_view RENAME TO test_select_view_view_renamed");
            assertQueryFails("SELECT * FROM test_select_view_view", ".*Table '.*test_select_view_view' does not exist");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_view_view_renamed",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_view_view_renamed",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_view");
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_view_renamed");
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
            assertGlueMetastoreApiInvocations("REFRESH MATERIALIZED VIEW test_select_mview_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 6)
                            .add(UPDATE_TABLE)
                            .build());

            // Select is served from cache
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.of());

            // Insert into source table invalidates its cache entry and makes MV stale
            assertUpdate("INSERT INTO test_select_mview_table VALUES ('Alice', 30)", 1);

            // Refresh repopulates the cache
            assertGlueMetastoreApiInvocations("REFRESH MATERIALIZED VIEW test_select_mview_view",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .addCopies(GET_TABLE, 6)
                            .add(UPDATE_TABLE)
                            .build());

            // Select after refresh is served from cache
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.of());

            // Rename MV invalidates cache
            assertUpdate("ALTER MATERIALIZED VIEW test_select_mview_view RENAME TO test_select_mview_view_renamed");
            assertQueryFails("SELECT * FROM test_select_mview_view", ".*Table '.*test_select_mview_view' does not exist");
            assertGlueMetastoreApiInvocations("SELECT * FROM test_select_mview_view_renamed",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM test_select_mview_view_renamed",
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
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Other system tables are served from cache
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.of());

            // Insert invalidates cache
            assertUpdate("INSERT INTO test_select_snapshots VALUES (3)", 1);
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.of());

            // Rename table invalidates cache
            assertUpdate("ALTER TABLE test_select_snapshots RENAME TO test_select_snapshots_renamed");
            assertQueryFails("SELECT * FROM \"test_select_snapshots$history\"", ".*Table '.*test_select_snapshots.*' does not exist");
            assertGlueMetastoreApiInvocations("SELECT * FROM \"test_select_snapshots_renamed$history\"",
                    ImmutableMultiset.<GlueMetastoreMethod>builder()
                            .add(GET_TABLE)
                            .build());

            // Cache is populated again
            assertGlueMetastoreApiInvocations(
                    "SELECT * FROM \"test_select_snapshots_renamed$snapshots\"",
                    ImmutableMultiset.of());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots_renamed");
        }
    }

    protected abstract List<GlueMetastoreStats> getGlueStats(QueryRunner queryRunner);

    private void assertGlueMetastoreApiInvocations(@Language("SQL") String query, Multiset<GlueMetastoreMethod> expectedInvocations)
    {
        Map<GlueMetastoreMethod, Integer> countsBefore = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), this::getInvocationCount));

        getQueryRunner().execute(getSession(), query);

        Map<GlueMetastoreMethod, Integer> countsAfter = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMap(Function.identity(), this::getInvocationCount));

        Multiset<GlueMetastoreMethod> actualGlueInvocations = Arrays.stream(GlueMetastoreMethod.values())
                .collect(toImmutableMultiset(Function.identity(), method -> requireNonNull(countsAfter.get(method)) - requireNonNull(countsBefore.get(method))));

        assertMultisetsEqual(actualGlueInvocations, expectedInvocations);
    }

    private int getInvocationCount(GlueMetastoreMethod method)
    {
        return getGlueStats(getQueryRunner()).stream()
                .map(method::getInvocationCount)
                .mapToInt(value -> value)
                .sum();
    }
}
