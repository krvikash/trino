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
package io.trino.plugin.sas;

import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.connector.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.sas.SasConfig.MappingType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSasMetadata
{
    @Test
    void testNasClient()
    {
        Path file = Path.of("src/test/resources").toAbsolutePath();
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(file));

        List<String> schemas = client.getSchemaNames(identity);
        assertThat(schemas).isNotEmpty();

        for (String schema : List.of("schema1", "schema2")) {
            Set<String> tables = client.getTableNames(schema, identity);
            assertThat(tables).isNotEmpty();

            for (String table : tables) {
                assertThat(client.getTable(schema, table, identity)).isPresent();
            }
        }

        SasTable sasTable = client.getTable("schema1", "colon", identity).orElseThrow();
        assertThat(sasTable.lineCount()).isEqualTo(15564L);
        assertThat(sasTable.pageCount()).isEqualTo(7L);
    }

    @Test
    void testNasJsonClient()
    {
        String path = "src/test/resources";

        Path file = Path.of(path).toAbsolutePath();
        SasConfig config = new SasConfig();
        config.setDataDirectory(file.toUri());
        config.setMappingType(JSON);
        config.setJsonFile(file.resolve("mapping_sas.json").toFile());
        config.setMinPagePerSplit(1000);

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        SasNasJsonClient jsonClient = new SasNasJsonClient(Location.of("local:///"), config, new LocalFileSystemFactory(file));

        List<String> schemas = jsonClient.getSchemaNames(identity);
        assertThat(schemas).isNotEmpty();

        for (String schema : schemas) {
            Set<String> tables = jsonClient.getTableNames(schema, identity);
            assertThat(tables).isNotEmpty();

            for (String table : tables) {
                assertThat(jsonClient.getTable(schema, table, identity)).isPresent();
            }
        }

        SasTable sasTable = jsonClient.getTable("mapping_table", "colon", identity).orElseThrow();
        assertThat(sasTable.lineCount()).isEqualTo(15564L);
        assertThat(sasTable.pageCount()).isEqualTo(7L);

        sasTable = jsonClient.getTable("mapping_schema", "colon", identity).orElseThrow();
        assertThat(sasTable.lineCount()).isEqualTo(15564L);
        assertThat(sasTable.pageCount()).isEqualTo(7L);
    }

    @Test
    void testMetadataAndRecordReading()
    {
        Path file = Path.of("src/test/resources").toAbsolutePath();
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(file));
        SasMetadata meta = new SasMetadata(client);
        List<String> schemas = meta.listSchemaNames(TestingConnectorSession.SESSION);
        assertThat(schemas).contains("schema1", "schema2");

        for (String schema : List.of("schema1", "schema2")) {
            List<SchemaTableName> tables = meta.listTables(TestingConnectorSession.SESSION, Optional.of(schema));
            assertThat(tables).isNotEmpty();

            for (SchemaTableName table : tables) {
                SasTableHandle tabH = meta.getTableHandle(TestingConnectorSession.SESSION, table, Optional.empty(), Optional.empty());
                meta.getTableMetadata(TestingConnectorSession.SESSION, tabH);

                Collection<ColumnHandle> coll = meta.getColumnHandles(TestingConnectorSession.SESSION, tabH).values();
                assertThat(coll).hasSize(13);

                List<SasColumnHandle> columnHandles = coll.stream()
                        .map(SasColumnHandle.class::cast)
                        .collect(toImmutableList());

                SasTable sasTable = client.getTable(table.getSchemaName(), table.getTableName(), identity).orElseThrow();
                LocalFileSystemFactory fsFactory = new LocalFileSystemFactory(file);
                int count = 0;
                try (SasRecordCursor record = new SasRecordCursor(columnHandles, sasTable.source(), 0, 100, client, fsFactory.create(identity))) {
                    while (record.advanceNextPosition()) {
                        int c = 0;
                        for (SasColumnHandle h : columnHandles) {
                            assertThat(h.columnName()).isNotNull();
                            assertThat(h.columnType()).isNotNull();
                            if (record.getType(c) instanceof VarcharType) {
                                assertThat(record.getSlice(c).toStringAscii()).isNotNull();
                            }
                            else if (record.getType(c) instanceof DoubleType) {
                                assertThat(record.getDouble(c)).isGreaterThan(-10_000_000.0);
                            }
                            else if (record.getType(c) instanceof TimestampType) {
                                assertThat(record.getLong(c)).isGreaterThan(Long.MIN_VALUE);
                            }
                            else if (record.getType(c) instanceof TimeType) {
                                assertThat(record.getLong(c)).isGreaterThan(Long.MIN_VALUE);
                            }
                            else if (record.getType(c) instanceof IntegerType) {
                                assertThat(record.getLong(c)).isGreaterThan(Long.MIN_VALUE);
                            }

                            c++;
                        }
                        count++;
                    }
                }
                assertThat(count).isEqualTo(15564);
            }
        }
    }
}
