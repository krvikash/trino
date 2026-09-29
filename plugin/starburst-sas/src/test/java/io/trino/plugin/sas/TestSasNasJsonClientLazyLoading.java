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
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static io.trino.plugin.sas.SasConfig.MappingType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code sas.json-file} is only used by {@link SasMetadata} (schema/table discovery), which the
 * engine always runs on the coordinator, never by the split-reading path. {@link SasNasJsonClient}
 * defers reading and parsing the mapping file until it is actually needed, so a worker that never
 * calls those metadata methods never has to have the file present at all.
 */
final class TestSasNasJsonClientLazyLoading
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");

    @Test
    void testColumnReadingDoesNotNeedTheMappingFile()
    {
        Path resourcesPath = Path.of("src/test/resources").toAbsolutePath();
        SasNasJsonClient client = clientWithMissingMappingFile(resourcesPath);

        // the split-reading path only resolves a file it was already given the path to; it never
        // consults the JSON mapping, so a missing mapping file must not matter here
        assertThat(client.listColumnsFromFile("local:///schema1/colon.sas7bdat", new LocalFileSystemFactory(resourcesPath).create(TEST_IDENTITY)))
                .isNotEmpty();
    }

    @Test
    void testSchemaDiscoveryFailsOnlyWhenActuallyCalled()
    {
        Path resourcesPath = Path.of("src/test/resources").toAbsolutePath();
        SasNasJsonClient client = clientWithMissingMappingFile(resourcesPath);

        // only once a ConnectorMetadata method is actually invoked does the missing file surface
        assertThatThrownBy(() -> client.getSchemaNames(TEST_IDENTITY))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Error reading JSON mapping file");
    }

    private static SasNasJsonClient clientWithMissingMappingFile(Path resourcesPath)
    {
        SasConfig config = new SasConfig();
        config.setDataDirectory(resourcesPath.toUri());
        config.setMappingType(JSON);
        config.setJsonFile(new File(resourcesPath.toFile(), "no-such-mapping-file.json"));
        return new SasNasJsonClient(Location.of("local:///"), config, new LocalFileSystemFactory(resourcesPath));
    }
}
