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

import com.epam.parso.Column;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.scharp.sas7bdat.Sas7bdatExporter;
import org.scharp.sas7bdat.Sas7bdatMetadata;
import org.scharp.sas7bdat.Variable;
import org.scharp.sas7bdat.VariableType;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class TestSasFileGenerator
{
    @Test
    void testWriteAndReadRoundTrip(@TempDir Path tempDir)
            throws Exception
    {
        Path file = tempDir.resolve("test.sas7bdat");

        Sas7bdatMetadata metadata = Sas7bdatMetadata.builder()
                .datasetName("TEST")
                .variables(List.of(
                        Variable.builder().name("NAME").type(VariableType.CHARACTER).length(20).build(),
                        Variable.builder().name("AGE").type(VariableType.NUMERIC).length(8).build(),
                        Variable.builder().name("SCORE").type(VariableType.NUMERIC).length(8).build()))
                .build();

        List<List<Object>> rows = List.of(
                List.of("Alice", 30.0, 95.5),
                List.of("Bob", 25.0, 87.0),
                List.of("Charlie", 35.0, 91.0),
                List.of("Diana", 28.0, 99.0),
                List.of("Eve", 22.0, 78.5));

        Sas7bdatExporter.exportDataset(file, metadata, rows);

        assertThat(file).exists();

        try (InputStream fis = Files.newInputStream(file)) {
            SasFileReader reader = new SasFileReaderImpl(fis);

            List<Column> columns = reader.getColumns();
            assertThat(columns).hasSize(3);
            assertThat(columns.get(0).getName()).isEqualTo("NAME");
            assertThat(columns.get(1).getName()).isEqualTo("AGE");
            assertThat(columns.get(2).getName()).isEqualTo("SCORE");

            assertThat(reader.getSasFileProperties().getRowCount()).isEqualTo(5);

            Object[] row0 = reader.readNext();
            assertThat(row0).isNotNull();
            assertThat(((String) row0[0]).trim()).isEqualTo("Alice");
            assertThat(((Number) row0[1]).doubleValue()).isEqualTo(30.0);
            assertThat(((Number) row0[2]).doubleValue()).isEqualTo(95.5);

            int remaining = 0;
            while (reader.readNext() != null) {
                remaining++;
            }
            assertThat(remaining).isEqualTo(4);
        }
    }
}
