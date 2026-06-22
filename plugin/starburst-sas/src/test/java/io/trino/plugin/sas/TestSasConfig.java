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

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.trino.plugin.sas.SasConfig.MappingType.FS;
import static io.trino.plugin.sas.SasConfig.MappingType.JSON;

final class TestSasConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SasConfig.class)
                .setDataDirectory(null)
                .setSplitCount(1)
                .setMinPagePerSplit(2000)
                .setMappingType(FS)
                .setJsonFile(null));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path jsonFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sas.data-directory", "file:///data/sas")
                .put("sas.split-count", "4")
                .put("sas.min-page-per-split", "500")
                .put("sas.mapping-type", "JSON")
                .put("sas.json-file", jsonFile.toString())
                .buildOrThrow();

        SasConfig expected = new SasConfig()
                .setDataDirectory(URI.create("file:///data/sas"))
                .setSplitCount(4)
                .setMinPagePerSplit(500)
                .setMappingType(JSON)
                .setJsonFile(jsonFile.toFile());

        assertFullMapping(properties, expected);
    }

    @Test
    void testJsonFileMustBeSetWhenMappingTypeIsJson()
            throws IOException
    {
        assertFailsValidation(
                new SasConfig().setMappingType(JSON),
                "jsonFileSetWhenMappingTypeIsJson",
                "sas.json-file is required when mapping-type is JSON",
                AssertTrue.class);

        Path jsonFile = Files.createTempFile(null, null);
        assertValidates(new SasConfig()
                .setDataDirectory(URI.create("file:///data/sas"))
                .setMappingType(JSON)
                .setJsonFile(jsonFile.toFile()));
        assertValidates(new SasConfig().setDataDirectory(URI.create("file:///data/sas")));
    }
}
