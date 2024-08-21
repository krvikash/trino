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

import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.warp.config.ProxiedConnectorConfig;
import io.trino.plugin.warp.di.InitializationModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.plugin.warp.extension.config.WarpExtensionConfig;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorInitializer;
import io.trino.plugin.warp.util.UriUtils;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class WarpConnectorFactory
        implements ConnectorFactory
{
    private final DispatcherConnectorFactory dispatcherConnectorFactory;
    private final LicenseManager licenseManager;
    private final List<Class<? extends InitializationModule>> extraModules;

    public WarpConnectorFactory(
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

        ConfigurationFactory configFactory = new ConfigurationFactory(config);
        WarpExtensionConfig warpExtensionConfig = configFactory.build(WarpExtensionConfig.class);

        if (!warpExtensionConfig.isUseHttpServerPort()) {
            String httpRestPortStr = WarpClient.getRestHttpPortStr(
                    warpExtensionConfig,
                    UriUtils.getHttpUri(context.getNodeManager().getCurrentNode()).getPort());

            configMap.put("http-server.http.port", httpRestPortStr);
            if (!configMap.containsKey(WarpExtensionConfig.HTTP_REST_PORT)) {
                configMap.put(WarpExtensionConfig.HTTP_REST_PORT, httpRestPortStr);
            }
        }

        List<Class<? extends InitializationModule>> extraModules = !this.extraModules.isEmpty() ?
                this.extraModules :
                List.of(WarpModule.class);
        return new StarburstWarpConnector(dispatcherConnectorFactory.create(
                catalogName,
                configMap,
                context,
                Optional.of(extraModules),
                Map.of(ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME, DeltaLakeProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfig.HIVE_CONNECTOR_NAME, HiveProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME, IcebergProxiedConnectorInitializer.class.getName())));
    }
}
