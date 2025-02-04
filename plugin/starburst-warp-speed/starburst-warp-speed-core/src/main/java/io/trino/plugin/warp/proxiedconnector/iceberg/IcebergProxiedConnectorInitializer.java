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
package io.trino.plugin.warp.proxiedconnector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.ProxiedConnectorInitializer;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.warp.proxiedconnector.utils.ConfigurationUtils.getIcebergFilteredConfig;

public class IcebergProxiedConnectorInitializer
        implements ProxiedConnectorInitializer
{
    private static final Module DEFAULT_ADDITIONAL_MODULE = EMPTY_MODULE;
    private static final Optional<Module> DEFAULT_ICEBERG_CATALOG_MODULE = Optional.empty();

    @Override
    public Supplier<List<Module>> getModules(ConnectorContext context)
    {
        return () -> List.of(
                new ConnectorObjectNameGeneratorModule(
                        "io.trino.plugin.iceberg",
                        "presto.plugin.iceberg"),
                binder -> binder.bind(DispatcherProxiedConnectorTransformer.class).to(IcebergProxiedConnectorTransformer.class));
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context, Optional<Module> optionalProxyModule)
    {
        try {
            Map<String, String> icebergConfig = getIcebergFilteredConfig(config);
            return IcebergConnectorFactory.createConnector(
                    catalogName,
                    icebergConfig,
                    context,
                    DEFAULT_ADDITIONAL_MODULE,
                    DEFAULT_ICEBERG_CATALOG_MODULE);
        }
        catch (Exception e) {
            throw new RuntimeException("cant create iceberg connector", e);
        }
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context, Optional<Module> optionalProxyModule)
    {
        Map<String, String> icebergConfig = getIcebergFilteredConfig(config);

        ClassLoader classLoader = IcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = IcebergConnectorFactory.createBootstrap(catalogName, icebergConfig, ImmutableMap.of(), context, DEFAULT_ADDITIONAL_MODULE, DEFAULT_ICEBERG_CATALOG_MODULE, true);

            Set<ConfigPropertyMetadata> usedProperties = app.configure();

            return ConfigUtils.getSecuritySensitivePropertyNames(icebergConfig, usedProperties);
        }
    }
}
