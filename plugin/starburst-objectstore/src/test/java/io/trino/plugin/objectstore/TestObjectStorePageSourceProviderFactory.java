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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.IoExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

class TestObjectStorePageSourceProviderFactory
{
    @Test
    void testEverythingImplemented()
            throws Exception
    {
        assertAllMethodsOverridden(ConnectorPageSourceProviderFactory.class, ObjectStorePageSourceProviderFactory.class, ImmutableSet.<Method>builder()
                // TODO (https://starburstdata.atlassian.net/browse/ENG-22306) support GPU Table Scan via ObjectStore connector
                .add(ConnectorPageSourceProviderFactory.class.getMethod("getGpuPageSourceSupport", ConnectorTableHandle.class, List.class))
                .build());

        assertAllMethodsOverridden(ConnectorPageSourceProvider.class, ObjectStorePageSourceProviderFactory.ObjectStorePageSourceProvider.class, ImmutableSet.<Method>builder()
                // TODO (https://starburstdata.atlassian.net/browse/ENG-22306) support GPU Table Scan via ObjectStore connector
                .add(ObjectStorePageSourceProviderFactory.ObjectStorePageSourceProvider.class.getMethod(
                        "createGpuPageSource",
                        ConnectorTransactionHandle.class,
                        ConnectorSession.class,
                        ConnectorSplit.class,
                        ConnectorTableHandle.class,
                        Optional.class,
                        List.class,
                        DynamicFilter.class,
                        ConnectorGpuMemoryContext.class,
                        IoExecutor.class))
                .build());
    }
}
