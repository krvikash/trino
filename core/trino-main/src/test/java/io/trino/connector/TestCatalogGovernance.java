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
package io.trino.connector;

import com.google.common.collect.ImmutableMap;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.catalog.CatalogGovernance;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.catalog.CatalogGovernance.DELEGATED;
import static io.trino.spi.catalog.CatalogGovernance.GOVERNED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCatalogGovernance
{
    @Test
    void testCatalogPropertiesDefaultsToGoverned()
    {
        assertThat(new CatalogProperties(new CatalogName("test"), new CatalogVersion("1"), new ConnectorName("mock"), ImmutableMap.of()).governance())
                .isEqualTo(GOVERNED);

        assertThatThrownBy(() -> new CatalogProperties(new CatalogName("test"), new CatalogVersion("1"), new ConnectorName("mock"), ImmutableMap.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("governance is null");
    }

    @Test
    void testCatalogCarriesGovernanceFromCatalogProperties()
    {
        assertThat(createCatalogConnector(Optional.of(catalogProperties(DELEGATED))).getCatalog().getGovernance())
                .isEqualTo(DELEGATED);

        assertThat(createCatalogConnector(Optional.of(catalogProperties(GOVERNED))).getCatalog().getGovernance())
                .isEqualTo(GOVERNED);
    }

    @Test
    void testCatalogWithoutCatalogPropertiesIsGoverned()
    {
        assertThat(createCatalogConnector(Optional.empty()).getCatalog().getGovernance())
                .isEqualTo(GOVERNED);
    }

    private static CatalogProperties catalogProperties(CatalogGovernance governance)
    {
        return new CatalogProperties(new CatalogName("test"), new CatalogVersion("1"), new ConnectorName("mock"), ImmutableMap.of(), governance);
    }

    private static CatalogConnector createCatalogConnector(Optional<CatalogProperties> catalogProperties)
    {
        CatalogName catalogName = new CatalogName("test");
        CatalogHandle catalogHandle = createRootCatalogHandle(catalogName, new CatalogVersion("1"));
        Connector connector = MockConnectorFactory.create().create(catalogName.toString(), ImmutableMap.of(), new TestingConnectorContext());
        ConnectorServices connectorServices = new ConnectorServices(noopTracer(), catalogHandle, connector);
        return new CatalogConnector(
                catalogHandle,
                new ConnectorName("mock"),
                connectorServices,
                connectorServices,
                connectorServices,
                new LocalMemoryManager(new NodeMemoryConfig()),
                catalogProperties);
    }
}
