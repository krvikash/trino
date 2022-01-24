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
package io.trino.plugin.objectstore.functions.unload;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.objectstore.ForHive;
import io.trino.spi.connector.Connector;
import io.trino.spi.function.table.ConnectorTableFunction;

import static java.util.Objects.requireNonNull;

public class OjbectStoreUnloadFunctionProvider
        implements Provider<ConnectorTableFunction>
{
    private final Connector hiveConnector;

    @Inject
    public OjbectStoreUnloadFunctionProvider(@ForHive Connector hiveConnector)
    {
        this.hiveConnector = requireNonNull(hiveConnector, "hiveConnector is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(
                new ObjectStoreUnloadFunction(hiveConnector),
                getClass().getClassLoader());
    }
}
