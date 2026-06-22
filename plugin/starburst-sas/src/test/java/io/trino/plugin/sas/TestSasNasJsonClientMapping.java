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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.trino.plugin.sas.SasConfig.MappingType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that JSON mapping fully decouples logical names from the physical layout:
 * schema name != directory name, table name != filename, file 3 levels deep.
 *
 * Physical layout:
 *   src/test/resources/
 *     data/year2024/reports/sales_report.sas7bdat
 *
 * JSON mapping (nested_mapping.json):
 *   schema "analytics"  ->  directory does not exist / does not need to match
 *   table  "quarterly_sales"  ->  file data/year2024/reports/sales_report.sas7bdat
 */
public class TestSasNasJsonClientMapping
{
    @Test
    public void testSchemaAndTableNamesDecoupledFromFilesystem()
    {
        Path resourcesPath = Path.of("src/test/resources").toAbsolutePath();

        SasConfig config = new SasConfig();
        config.setDataDirectory(resourcesPath.toUri());
        config.setMappingType(JSON);
        config.setJsonFile(resourcesPath.resolve("nested_mapping.json").toFile());
        config.setMinPagePerSplit(1000);

        SasNasJsonClient client = new SasNasJsonClient(Location.of("local:///"), config, new LocalFileSystemFactory(resourcesPath));
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        // Schema name "analytics" — no directory called "analytics" exists under resources
        List<String> schemas = client.getSchemaNames(identity);
        assertThat(schemas).containsExactly("analytics");

        // Table name "quarterly_sales" — no file called "quarterly_sales.sas7bdat" exists
        Set<String> tables = client.getTableNames("analytics", identity);
        assertThat(tables).containsExactly("quarterly_sales");

        // The actual file is 3 levels deep at data/year2024/reports/sales_report.sas7bdat
        SasTable table = client.getTable("analytics", "quarterly_sales", identity).orElseThrow();
        assertThat(table).isNotNull();
        assertThat(table.lineCount()).isEqualTo(15564);
        assertThat(table.pageCount()).isEqualTo(7);
        assertThat(table.columns()).hasSize(13);
        // source location points to the real file, not the logical table name
        assertThat(table.source()).contains("sales_report.sas7bdat");
        assertThat(table.source()).contains("data/year2024/reports");
    }
}
