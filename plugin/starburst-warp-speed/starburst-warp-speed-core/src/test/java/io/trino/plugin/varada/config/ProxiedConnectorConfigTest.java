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
package io.trino.plugin.varada.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxiedConnectorConfigTest
{
    @Test
    public void testPassThroughDispatcherSet()
    {
        ProxiedConnectorConfig proxiedConnectorConfig = new ProxiedConnectorConfig();

        proxiedConnectorConfig.setPassThroughDispatcherSet(
                String.format(" %s , %s ", ProxiedConnectorConfig.HUDI_CONNECTOR_NAME, ProxiedConnectorConfig.HIVE_CONNECTOR_NAME));
        assertThat(proxiedConnectorConfig.getPassThroughDispatcherSet())
                .containsExactlyInAnyOrder(ProxiedConnectorConfig.HUDI_CONNECTOR_NAME, ProxiedConnectorConfig.HIVE_CONNECTOR_NAME);

        proxiedConnectorConfig.setPassThroughDispatcherSet(
                String.format(" %s,%s, %s ",
                        ProxiedConnectorConfig.HUDI_CONNECTOR_NAME,
                        ProxiedConnectorConfig.HIVE_CONNECTOR_NAME,
                        ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME));
        assertThat(proxiedConnectorConfig.getPassThroughDispatcherSet())
                .containsExactlyInAnyOrder(
                        ProxiedConnectorConfig.HUDI_CONNECTOR_NAME,
                        ProxiedConnectorConfig.HIVE_CONNECTOR_NAME,
                        ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME);

        assertThatThrownBy(() -> proxiedConnectorConfig.setPassThroughDispatcherSet(" hive11,hudi, iceberg "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("enable.passthrough config only supports");
    }
}
