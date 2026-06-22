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

import static io.trino.plugin.sas.SasConfig.MappingType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

class TestSasJsonModeCase
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");
    private SasNasJsonClient client;

    @BeforeEach
    void setUp()
    {
        Path root = Path.of("src/test/resources").toAbsolutePath();
        SasConfig config = new SasConfig();
        config.setDataDirectory(root.toUri());
        config.setMappingType(JSON);
        config.setJsonFile(root.resolve("mapping_case_test.json").toFile());
        config.setMinPagePerSplit(1000);
        client = new SasNasJsonClient(Location.of("local:///"), config, new LocalFileSystemFactory(root));
    }

    // --- explicit table list ---

    @Test
    void testExplicitUppercaseSchemaNameIsLowercased()
    {
        List<String> schemas = client.getSchemaNames(TEST_IDENTITY);
        assertThat(schemas).contains("explicit_schema");
    }

    @Test
    void testExplicitUppercaseTableNameIsLowercased()
    {
        Set<String> tables = client.getTableNames("explicit_schema", TEST_IDENTITY);
        assertThat(tables).contains("explicit_table");
    }

    @Test
    void testExplicitGetTableWithLowercasedName()
    {
        SasTable table = client.getTable("explicit_schema", "explicit_table", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    @Test
    void testExplicitGetTableWithUppercaseInput()
    {
        // lookup must still work regardless of input casing
        SasTable table = client.getTable("EXPLICIT_SCHEMA", "EXPLICIT_TABLE", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    // --- path shorthand ---

    @Test
    void testPathSchemaNameIsLowercased()
    {
        List<String> schemas = client.getSchemaNames(TEST_IDENTITY);
        assertThat(schemas).contains("path_schema");
    }

    @Test
    void testPathTableNameIsLowercaseAndHasNoExtension()
    {
        Set<String> tables = client.getTableNames("path_schema", TEST_IDENTITY);
        assertThat(tables).isNotEmpty();
        for (String table : tables) {
            assertThat(table).isEqualTo(table.toLowerCase(Locale.ENGLISH));
            assertThat(table).doesNotEndWith(".sas7bdat");
        }
    }

    @Test
    void testPathGetTableWithLowercaseNameNoExtension()
    {
        SasTable table = client.getTable("path_schema", "colon", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    @Test
    void testPathGetTableWithUppercaseInput()
    {
        SasTable table = client.getTable("PATH_SCHEMA", "COLON", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }

    // --- path shorthand pointing at an uppercase filesystem directory (SCHEMA3/UPPER.sas7bdat) ---

    @Test
    void testUppercaseFsPathTableIsReturnedLowercase()
    {
        Set<String> tables = client.getTableNames("upper_path_schema", TEST_IDENTITY);
        assertThat(tables).contains("upper");
    }

    @Test
    void testUppercaseFsPathTableHasNoExtension()
    {
        Set<String> tables = client.getTableNames("upper_path_schema", TEST_IDENTITY);
        for (String table : tables) {
            assertThat(table).doesNotEndWith(".sas7bdat");
        }
    }

    @Test
    void testGetTableFromUppercaseFsPath()
    {
        // JSON path points to SCHEMA3/; file is UPPER.sas7bdat — must resolve case-insensitively
        SasTable table = client.getTable("upper_path_schema", "upper", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
    }
}
