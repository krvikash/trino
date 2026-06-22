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
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestSasFsModeCase
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");
    private SasNasClient client;

    @BeforeEach
    void setUp()
    {
        Path root = Path.of("src/test/resources").toAbsolutePath();
        client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(root));
    }

    @Test
    void testSchemaNameIsLowercase()
    {
        List<String> schemas = client.getSchemaNames(TEST_IDENTITY);
        for (String schema : schemas) {
            assertThat(schema).isEqualTo(schema.toLowerCase(Locale.ENGLISH));
        }
    }

    @Test
    void testTableNameIsLowercaseAndHasNoExtension()
    {
        Set<String> tables = client.getTableNames("schema1", TEST_IDENTITY);
        assertThat(tables).isNotEmpty();
        for (String table : tables) {
            assertThat(table).isEqualTo(table.toLowerCase(Locale.ENGLISH));
            assertThat(table).doesNotEndWith(".sas7bdat");
        }
    }

    @Test
    void testGetTableWithUppercaseSchemaInput()
    {
        // engine passes lowercased names in practice, but lookup must survive uppercase
        SasTable table = client.getTable("SCHEMA1", "colon", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    @Test
    void testGetTableWithUppercaseTableInput()
    {
        SasTable table = client.getTable("schema1", "COLON", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    // --- uppercase names on the actual filesystem (SCHEMA3/UPPER.sas7bdat) ---

    @Test
    void testUppercaseFsDirectoryIsReturnedLowercase()
    {
        List<String> schemas = client.getSchemaNames(TEST_IDENTITY);
        assertThat(schemas).contains("schema3");
    }

    @Test
    void testUppercaseFsFileIsReturnedLowercase()
    {
        Set<String> tables = client.getTableNames("schema3", TEST_IDENTITY);
        assertThat(tables).contains("upper");
    }

    @Test
    void testGetTableFromUppercaseFsNames()
    {
        // query with lowercase names — connector must resolve SCHEMA3/UPPER.sas7bdat case-insensitively
        SasTable table = client.getTable("schema3", "upper", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    @Test
    void testGetTableFromUppercaseFsNamesWithUppercaseInput()
    {
        // both the input and the filesystem names are uppercase
        SasTable table = client.getTable("SCHEMA3", "UPPER", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }
}
