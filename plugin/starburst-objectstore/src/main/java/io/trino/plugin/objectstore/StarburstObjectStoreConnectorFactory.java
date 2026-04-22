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

import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.objectstore.InternalStarburstObjectStoreConnectorFactory.buildConfig;
import static io.trino.plugin.objectstore.InternalStarburstObjectStoreConnectorFactory.createBootstrap;
import static io.trino.plugin.objectstore.InternalStarburstObjectStoreConnectorFactory.createConnector;

public class StarburstObjectStoreConnectorFactory
        implements ConnectorFactory
{
    public static final String STARBURST_OBJECTSTORE = "great_lakes";

    @Override
    public String getName()
    {
        return STARBURST_OBJECTSTORE;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(
                catalogName,
                config,
                Optional.empty(),
                EMPTY_MODULE,
                Optional.empty(),
                Optional.empty(),
                EMPTY_MODULE,
                EMPTY_MODULE,
                context,
                false);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Map<String, String> objectStoreConfig = buildConfig(config);
        ClassLoader classLoader = StarburstObjectStoreConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(
                    catalogName,
                    Optional.empty(),
                    EMPTY_MODULE,
                    Optional.empty(),
                    Optional.empty(),
                    EMPTY_MODULE,
                    EMPTY_MODULE,
                    objectStoreConfig,
                    _ -> {},
                    context,
                    true);

            Set<ConfigPropertyMetadata> usedProperties = app.configure();

            return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
        }
    }
}
