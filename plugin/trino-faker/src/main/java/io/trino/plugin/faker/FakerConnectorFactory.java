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

package io.trino.plugin.faker;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class FakerConnectorFactory
        implements ConnectorFactory
{
    private final String name;

    public FakerConnectorFactory(String name)
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireNonNull(context, "context is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = createBootstrap(catalogName, requiredConfig, context);

        Injector injector = app.initialize();

        return injector.getInstance(FakerConnector.class);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = createBootstrap(catalogName, config, context);

        Set<ConfigPropertyMetadata> usedProperties = app
                .quiet()
                .skipErrorReporting()
                .configure();

        return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
    }

    private static Bootstrap createBootstrap(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new ConnectorContextModule(catalogName, context),
                new FakerModule(context.getFunctionBundleFactory()));

        return app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config);
    }
}
