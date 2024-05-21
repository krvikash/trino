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
package io.trino.plugin.warp.extension.di;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.plugin.warp.di.WarmupCloudFetcherModule;
import io.trino.plugin.warp.dispatcher.connectors.ConnectorTaskExecutor;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class WarpEmptyExtensionModule
        extends AbstractModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext connectorContext;
    private String catalogName;

    @SuppressWarnings("unused")
    public WarpEmptyExtensionModule() {}

    @SuppressWarnings("unused")
    public WarpEmptyExtensionModule(
            Map<String, String> config,
            ConnectorContext connectorContext,
            String catalogName)
    {
        this.config = requireNonNull(config);
        this.connectorContext = requireNonNull(connectorContext);
        this.catalogName = requireNonNull(catalogName);
    }

    @Override
    public void configure()
    {
        binder().bind(ConnectorTaskExecutor.class).to(EmptyTaskExecutor.class);

        binder().install(new WarmupCloudFetcherModule(config, connectorContext, catalogName));
    }

    @Override
    public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        return new WarpEmptyExtensionModule(config, connectorContext, catalogName);
    }

    static class EmptyTaskExecutor
            implements ConnectorTaskExecutor
    {
        @Override
        public Object executeTask(String taskName, String dataStr, String httpMethod)
        {
            return null;
        }
    }
}
