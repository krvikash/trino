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
package com.starburstdata.trino.plugin.internaltesting;

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

public class InternalTestingConnectorFactory
        implements ConnectorFactory
{
    private static final String NAME = "starburst_internal_testing";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = createBootstrap(catalogName, config, context);

        Injector injector = app
                .initialize();

        return injector.getInstance(InternalTestingConnector.class);
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
                new InternalTestingModule());

        return app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config);
    }
}
