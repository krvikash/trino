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
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.di.DefaultFakeConnectorSessionProvider;
import io.trino.plugin.warp.di.FakeConnectorSessionProvider;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.execution.VaradaClient;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.extension.di.WarpEmptyExtensionModule;
import io.trino.plugin.warp.extension.di.WarpExtensionModule;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorInitializer;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StarburstWarpConnectorFactory
        implements ConnectorFactory, CacheManagerFactory
{
    private final DispatcherConnectorFactory dispatcherConnectorFactory;
    private final LicenseManager licenseManager;
    private final List<Class<? extends InitializationModule>> extraModules;

    public StarburstWarpConnectorFactory(
            DispatcherConnectorFactory dispatcherConnectorFactory,
            LicenseManager licenseManager,
            List<Class<? extends InitializationModule>> extraModules)
    {
        this.dispatcherConnectorFactory = requireNonNull(dispatcherConnectorFactory);
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
        this.extraModules = requireNonNull(extraModules, "extraModules is null");
    }

    @Override
    public String getName()
    {
        return DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        Map<String, String> configMap = new HashMap<>(config);

        if (!Boolean.parseBoolean(configMap.getOrDefault(WarpExtensionConfig.USE_HTTP_SERVER_PORT, Boolean.TRUE.toString()))) {
            String httpRestPortStr = VaradaClient.getRestHttpPortStr(configMap,
                    UriUtils.getHttpUri(context.getNodeManager().getCurrentNode()).getPort());

            configMap.put("http-server.http.port", httpRestPortStr);
            if (!configMap.containsKey(WarpExtensionConfig.HTTP_REST_PORT)) {
                configMap.put(WarpExtensionConfig.HTTP_REST_PORT, httpRestPortStr);
            }
        }

        List<Class<? extends InitializationModule>> extraModules = !this.extraModules.isEmpty() ?
                new ArrayList<>(this.extraModules) :
                new ArrayList<>(List.of(WarpCorkModule.class));
        return new StarburstWarpConnector(dispatcherConnectorFactory.create(
                catalogName,
                configMap,
                context,
                Optional.of(extraModules),
                Map.of(ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME, DeltaLakeProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfig.HIVE_CONNECTOR_NAME, HiveProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME, IcebergProxiedConnectorInitializer.class.getName())));
    }

    @Override
    public CacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        return dispatcherConnectorFactory.getCacheManager();
    }

    public static class WarpCorkModule
            implements InitializationModule
    {
        private Map<String, String> config;
        private ConnectorContext connectorContext;
        private String catalogName;

        @SuppressWarnings("unused")
        public WarpCorkModule() {}

        public WarpCorkModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
        {
            this.config = requireNonNull(config);
            this.connectorContext = requireNonNull(connectorContext);
            this.catalogName = requireNonNull(catalogName);
        }

        @Override
        public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
        {
            return new WarpCorkModule(config, connectorContext, catalogName);
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
}
