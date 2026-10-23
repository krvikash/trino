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
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

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
    void testNativeHiveViewBodyReferencingOtherSchemaByRealName()
    {
        String otherSchema = "other_" + randomNameSuffix();
        String otherTable = "native_hive_view_source_" + randomNameSuffix();
        String view = "native_hive_view_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + catalog() + "." + otherSchema);
        assertUpdate("CREATE TABLE " + table(otherSchema, otherTable) + " (id INT)");
        assertUpdate("INSERT INTO " + table(otherSchema, otherTable) + " VALUES 1, 2", 2);

        createNativeHiveView(realSchemaName(), view, "SELECT id FROM " + otherSchema + "." + otherTable);

        try {
            assertThat(computeActual("SELECT id FROM " + prefixedTable(view)).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder(1, 2);

            assertThat((String) computeScalar("SHOW CREATE VIEW " + prefixedTable(view)))
                    .isEqualTo(
                            """
                            CREATE VIEW %s SECURITY DEFINER AS
                            SELECT *
                            FROM
                              "%s"."%s" "%s\""""
                                    .formatted(prefixedTable(view), prefixedSchemaName(otherSchema), otherTable, otherTable));
        }
        finally {
            assertUpdate("DROP VIEW " + realTable(view));
            assertUpdate("DROP TABLE " + table(otherSchema, otherTable));
            assertUpdate("DROP SCHEMA " + catalog() + "." + otherSchema);
        }
    }

    @Test
    void testNativeHiveViewInUnmappedSchemaReferencingMappedSchema()
    {
        // View's own schema is unmapped (wrap() won't wrap it), but its body references a mapped schema.
        String unmappedSchema = "unmapped_" + randomNameSuffix();
        String table = "nested_view_source_" + randomNameSuffix();
        String view = "nested_view_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + catalog() + "." + unmappedSchema);
        assertUpdate("CREATE TABLE " + realTable(table) + " (id INT)");
        assertUpdate("INSERT INTO " + realTable(table) + " VALUES 1, 2", 2);

        createNativeHiveView(unmappedSchema, view, "SELECT id FROM " + realSchemaName() + "." + table);

        try {
            assertThat(computeActual("SELECT id FROM " + table(unmappedSchema, view)).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder(1, 2);
        }
        finally {
            assertUpdate("DROP VIEW " + table(unmappedSchema, view));
            assertUpdate("DROP TABLE " + realTable(table));
            assertUpdate("DROP SCHEMA " + catalog() + "." + unmappedSchema);
        }
    }

    private void createNativeHiveView(String databaseName, String viewName, String hiveSql)
    {
        TableInput viewInput = TableInput.builder()
                .name(viewName)
                .owner("hive")
                .tableType("VIRTUAL_VIEW")
                .viewOriginalText(hiveSql)
                .viewExpandedText(hiveSql)
                .storageDescriptor(StorageDescriptor.builder()
                        .columns(Column.builder().name("id").type("int").build())
                        .serdeInfo(SerDeInfo.builder().build())
                        .build())
                .build();

        try (GlueClient glueClient = floci.createGlueClient()) {
            glueClient.createTable(CreateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(viewInput)
                    .build());
        }
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
