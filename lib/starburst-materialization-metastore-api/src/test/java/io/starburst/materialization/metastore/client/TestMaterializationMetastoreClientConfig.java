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
package io.starburst.materialization.metastore.client;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

public class TestMaterializationMetastoreClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(MaterializationMetastoreClientConfig.class)
                .setBaseUri(null)
                .setMetastoreId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("materialization.metastore.base-uri", "https://mv-metastore.internal:8443")
                .put("materialization.metastore.id", "ms-1")
                .buildOrThrow();
        MaterializationMetastoreClientConfig expected = new MaterializationMetastoreClientConfig()
                .setBaseUri(URI.create("https://mv-metastore.internal:8443"))
                .setMetastoreId("ms-1");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
