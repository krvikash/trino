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
package io.trino.plugin.warp.dispatcher;

import io.trino.spi.connector.ConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class WarpCacheMgrConnectorContextTest
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(
                ConnectorContext.class,
                WarpCacheMgrConnectorContext.class,
                Set.of(
                        ConnectorContext.class.getMethod("getTracer"),
                        ConnectorContext.class.getMethod("getSpiVersion"),
                        ConnectorContext.class.getMethod("getVersionEmbedder"),
                        ConnectorContext.class.getMethod("getMetadataProvider"),
                        ConnectorContext.class.getMethod("getTypeManager"),
                        ConnectorContext.class.getMethod("getPageIndexerFactory"),
                        ConnectorContext.class.getMethod("getAiModelAccessControl"),
                        ConnectorContext.class.getMethod("getModelConnectionSpecsLoader"),
                        ConnectorContext.class.getMethod("getPageSorter"),
                        ConnectorContext.class.getMethod("getPageStreamFactory"),
                        ConnectorContext.class.getMethod("getCatalogVersion"),
                        ConnectorContext.class.getMethod("getNodeEnvironment"),
                        ConnectorContext.class.getMethod("getFunctionBundleFactory")));

        assertAllMethodsOverridden(
                WarpContext.class,
                WarpCacheMgrConnectorContext.class);
    }
}
