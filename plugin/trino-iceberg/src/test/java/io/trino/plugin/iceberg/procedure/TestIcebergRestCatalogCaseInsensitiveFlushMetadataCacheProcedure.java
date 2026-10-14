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
package io.trino.plugin.iceberg.procedure;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergRestCatalogCaseInsensitiveFlushMetadataCacheProcedure
        extends BaseTestIcebergRestCatalogFlushMetadataCacheProcedure
{
    TestIcebergRestCatalogCaseInsensitiveFlushMetadataCacheProcedure()
    {
        super(true);
    }

    @Test
    void testFlushCaseInsensitiveTableMappingCache()
    {
        String schemaName = "test_case_insensitive_flush_" + randomNameSuffix();
        String tableName = "table_name";

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + getSchemaLocation() + "/" + schemaName + "')");
        assertUpdate("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        // populate cache
        assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 25");

        renameTableOutsideTrino(schemaName, tableName, tableName.toUpperCase(ENGLISH));

        assertUpdate("CALL system.flush_metadata_cache()");

        // Use a modifying operation to verify the cache was flushed.
        // A SELECT would succeed even with stale cache since the REST catalog resolves names case-insensitively.
        assertUpdate("INSERT INTO " + schemaName + "." + tableName + " SELECT * FROM tpch.tiny.nation", 25);

        assertUpdate("DROP TABLE " + schemaName + "." + tableName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    void testFlushTableCaseInsensitiveTableMappingCache()
    {
        String schemaName = "test_case_insensitive_table_flush_" + randomNameSuffix();
        String tableName = "table_name";

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + getSchemaLocation() + "/" + schemaName + "')");
        assertUpdate("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        // populate cache
        assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 25");

        renameTableOutsideTrino(schemaName, tableName, tableName.toUpperCase(ENGLISH));

        assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + tableName + "')");

        // Use a modifying operation to verify the cache was flushed.
        // A SELECT would succeed even with stale cache since the REST catalog resolves names case-insensitively.
        assertUpdate("INSERT INTO " + schemaName + "." + tableName + " SELECT * FROM tpch.tiny.nation", 25);

        assertUpdate("DROP TABLE " + schemaName + "." + tableName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    void testFlushTableWithMixedCaseRemoteName()
    {
        String suffix = randomNameSuffix();
        Namespace remoteNamespace = Namespace.of("MixedCaseSchema_" + suffix);
        TableIdentifier remoteTable = TableIdentifier.of(remoteNamespace, "MixedCaseTable");
        String schemaName = remoteNamespace.level(0).toLowerCase(ENGLISH);
        String tableName = schemaName + "." + remoteTable.name().toLowerCase(ENGLISH);

        backend.createNamespace(remoteNamespace);
        backend.createTable(remoteTable, new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        try {
            // Populate cache under the remote, mixed-case identifier
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");

            backend.loadTable(remoteTable).updateSchema()
                    .addColumn("new_column", Types.IntegerType.get())
                    .commit();

            // The cached response does not have the new column yet
            assertQueryFails("SELECT new_column FROM " + tableName, ".*Column 'new_column' cannot be resolved");

            assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + remoteTable.name().toLowerCase(ENGLISH) + "')");

            assertQuery("SELECT count(new_column) FROM " + tableName, "VALUES 0");
        }
        finally {
            backend.dropTable(remoteTable);
            backend.dropNamespace(remoteNamespace);
        }
    }

    @Test
    void testFlushTableWithStaleNamespaceMapping()
    {
        String suffix = randomNameSuffix();
        Namespace remoteNamespace = Namespace.of("MixedCaseSchema_" + suffix);
        TableIdentifier remoteTable = TableIdentifier.of(remoteNamespace, "MixedCaseTable");
        String schemaName = remoteNamespace.level(0).toLowerCase(ENGLISH);
        String tableName = remoteTable.name().toLowerCase(ENGLISH);

        // Caches the namespace mapping before the remote namespace exists
        assertThatThrownBy(() -> computeActual("SELECT * FROM " + schemaName + "." + tableName))
                .hasMessageContaining("Namespace does not exist");

        backend.createNamespace(remoteNamespace);
        backend.createTable(remoteTable, new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        try {
            assertUpdate("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + tableName + "')");

            assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "VALUES 0");
        }
        finally {
            backend.dropTable(remoteTable);
            backend.dropNamespace(remoteNamespace);
        }
    }
}
