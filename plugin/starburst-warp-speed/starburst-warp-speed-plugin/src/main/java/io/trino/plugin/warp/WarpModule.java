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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.warp.di.DefaultFakeConnectorSessionProvider;
import io.trino.plugin.warp.di.FakeConnectorSessionProvider;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.extension.di.WarpEmptyExtensionModule;
import io.trino.plugin.warp.extension.di.WarpExtensionModule;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class WarpModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext connectorContext;
    private String catalogName;

    public WarpModule() {}

    public WarpModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        this.config = requireNonNull(config);
        this.connectorContext = requireNonNull(connectorContext);
        this.catalogName = requireNonNull(catalogName);
    }

    @Override
    public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        return new WarpModule(config, connectorContext, catalogName);
    }

    @Override
    public void configure(Binder binder)
    {
        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        WarpExtensionConfig warpExtensionConfig = configFactory.build(WarpExtensionConfig.class);

        if (warpExtensionConfig.isEnabled()) {
            binder.install(new WarpExtensionModule(config, connectorContext, catalogName));
        }
        else {
            binder.install(new WarpEmptyExtensionModule(config, connectorContext, catalogName));
        }

        binder.bind(FakeConnectorSessionProvider.class).to(DefaultFakeConnectorSessionProvider.class);
    }
}
