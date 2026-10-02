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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HivePlugin;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHiveSchemaMappingRules
        extends BaseSchemaMappingRulesTest
{
    public TestHiveSchemaMappingRules()
    {
        super("hive", new HivePlugin(), ImmutableMap.<String, String>builder()
                .put("hive.metastore", "glue")
                .put("hive.security", "allow-all")
                .put("hive.hive-views.enabled", "true")
                .buildOrThrow());
    }

    @Test
    void testExplain()
    {
        String table = "explain_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertQuerySucceeds("EXPLAIN SELECT * FROM " + prefixedTable(table));
        assertQuerySucceeds("EXPLAIN (TYPE VALIDATE) SELECT * FROM " + prefixedTable(table));

        assertThat((String) computeScalar("EXPLAIN SELECT * FROM " + prefixedTable(table)))
                .contains("%s:%s:%s".formatted(CATALOG, REAL_SCHEMA, table));

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testSyncPartitionMetadataProcedureThroughPrefix()
    {
        String table = "sync_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, part VARCHAR) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES (1, 'a'), (2, 'b')", 2);

        assertUpdate("CALL system.sync_partition_metadata(" +
                "schema_name => '" + prefixedSchemaName(realSchemaName()) + "', table_name => '" + table + "', mode => 'FULL')");

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testFlushMetadataCacheProcedureThroughPrefix()
    {
        String table = "flush_cache_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");

        assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + prefixedSchemaName(realSchemaName()) + "', table_name => '" + table + "')");

        assertThat(getQueryRunner().tableExists(getSession(), table)).isTrue();

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testDropStatsProcedureThroughPrefix()
    {
        String table = "drop_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT, part VARCHAR) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES (1, 'a'), (2, 'b')", 2);

        assertUpdate("CALL system.drop_stats(schema_name => '" + prefixedSchemaName(realSchemaName()) + "', table_name => '" + table + "')");

        assertUpdate("DROP TABLE " + realTable(table));
    }

    @Test
    void testErrorMessageForNonExistentSchemaIsNotWrapped()
    {
        // A prefixed name whose underlying real schema does not exist must still be reported as "does not
        // exist" the same way an unmapped nonexistent schema is: schema mapping must not turn this into a
        // wrapped/internal error, nor silently succeed.
        assertThatThrownBy(() -> computeActual("SHOW TABLES FROM " + prefixedSchema("zzz_no_such_schema_" + randomNameSuffix())))
                .hasMessageContaining("does not exist");
    }
}
