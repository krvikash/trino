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
package io.trino.plugin.warp;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class WarpPluginTest
{
    @Test
    public void testPlugin()
    {
        WarpPlugin warpPlugin = new WarpPlugin();
        assertThat(warpPlugin.getConnectorFactories()).hasOnlyElementsOfType(WarpConnectorFactory.class);
    }

    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        WarpPlugin warpPlugin = new WarpPlugin().withStorageEngineModule(new WarpStubsStorageEngineModule());
        ConnectorFactory factory = getOnlyElement(warpPlugin.getConnectorFactories());
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("non-existent-property", "value")
                .put("warp-speed.non-existent-property", "value")
                .put("warp-speed.cluster-uuid", "1234")
                .put("warp-speed.config.internal-communication.shared-secret", "secret")
                .put("warp-speed.config.extensions.enabled", "true")
                .put("warp-speed.proxied-connector", "hive")
                .put("fs.hadoop.enabled", "true")
                .put("hive.azure.abfs.oauth.client-id", "test-client-id") // security-sensitive property from trino-hdfs
                .put("hive.metastore.uri", "thrift://foo:1234")
                .put("hive.metastore.thrift.client.ssl.key-password", "password")
                .buildOrThrow();

        Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

        assertThat(sensitiveProperties)
                .containsExactlyInAnyOrder(
                        "non-existent-property",
                        "warp-speed.config.internal-communication.shared-secret",
                        "hive.azure.abfs.oauth.client-id",
                        "hive.metastore.thrift.client.ssl.key-password");
    }
}
