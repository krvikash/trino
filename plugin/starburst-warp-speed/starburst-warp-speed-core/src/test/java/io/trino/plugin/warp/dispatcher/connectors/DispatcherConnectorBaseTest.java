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
package io.trino.plugin.warp.dispatcher.connectors;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class DispatcherConnectorBaseTest
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(
                Connector.class,
                DispatcherConnectorBase.class,
                Set.of(
                        // Declared by the subclasses that serve each role
                        Connector.class.getMethod("getMetadata", ConnectorSession.class, ConnectorTransactionHandle.class),
                        Connector.class.getMethod("commit", ConnectorTransactionHandle.class),
                        Connector.class.getMethod("rollback", ConnectorTransactionHandle.class),
                        Connector.class.getMethod("getSplitManager"),
                        Connector.class.getMethod("getNodePartitioningProvider"),
                        Connector.class.getMethod("getPageSourceProviderFactory"),
                        // Warp does not expose cache metadata or sub-plan alternatives
                        Connector.class.getMethod("getCacheMetadata"),
                        Connector.class.getMethod("getAlternativeChooser"),
                        // Page sources are created through getPageSourceProviderFactory
                        Connector.class.getMethod("getPageSourceProvider")));
    }
}
