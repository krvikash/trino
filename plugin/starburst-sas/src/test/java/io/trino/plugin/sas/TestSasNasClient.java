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
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSasNasClient
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");

    @Test
    void testNestedTableRoundTrip()
    {
        Path basePath = Path.of("src/test/resources").toAbsolutePath();
        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(basePath));

        // data/year2024/reports/sales_report.sas7bdat should surface as a nested table name
        Set<String> tables = client.getTableNames("data", TEST_IDENTITY);
        assertThat(tables).contains("year2024/reports/sales_report");

        // getTable must succeed for a name containing '/'
        SasTable table = client.getTable("data", "year2024/reports/sales_report", TEST_IDENTITY).orElseThrow();
        assertThat(table.source()).contains("sales_report.sas7bdat");
    }

    @Test
    void testCheckFile()
    {
        Path basePath = Path.of("src/test/resources").toAbsolutePath();
        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(basePath));

        // valid schema name — must not throw
        client.checkFileSchema("schema1");

        // schema name contains ".."
        assertThatThrownBy(() -> client.checkFileSchema("../schema1"))
                .isExactlyInstanceOf(SchemaNotFoundException.class);

        // schema name contains "/"
        assertThatThrownBy(() -> client.checkFileSchema("foo/bar"))
                .isExactlyInstanceOf(SchemaNotFoundException.class);

        // schema name contains "\"
        assertThatThrownBy(() -> client.checkFileSchema("foo\\bar"))
                .isExactlyInstanceOf(SchemaNotFoundException.class);
    }
}
