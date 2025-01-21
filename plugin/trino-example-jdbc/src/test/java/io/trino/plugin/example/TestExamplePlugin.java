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
package io.trino.plugin.example;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

final class TestExamplePlugin
{
    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        Plugin plugin = new ExamplePlugin();
        ConnectorFactory factory = plugin.getConnectorFactories().iterator().next();
        Map<String, String> config = Map.of(
                "non-existent-property", "value",
                "connection-url", "jdbc:h2:mem:test",
                "credential-provider.type", "inline",
                "connection-user", "user",
                "connection-password", "password");

        Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

        assertThat(sensitiveProperties)
                .containsExactlyInAnyOrder("non-existent-property", "connection-password");
    }
}
