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

import io.trino.spi.connector.Connector;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class StarburstWarpConnectorTest
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(
                Connector.class,
                StarburstWarpConnector.class,
                Set.of(
                        // Warp does not expose cache metadata
                        Connector.class.getMethod("getCacheMetadata"),
                        // Page sources are created through getPageSourceProviderFactory
                        Connector.class.getMethod("getPageSourceProvider")));
    }
}
