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
package io.trino.plugin.elasticsearch;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.elasticsearch.client.DefaultElasticsearchClientFactory;
import io.trino.plugin.elasticsearch.client.ElasticsearchClientFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class ElasticsearchConnectorFactory
        implements ConnectorFactory
{
    ElasticsearchConnectorFactory() {}

    @Override
    public String getName()
    {
        return "elasticsearch";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = createBootstrap(catalogName, config, context);

        Injector injector = app.initialize();

        return injector.getInstance(ElasticsearchConnector.class);
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
                new MBeanModule(),
                new MBeanServerModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.elasticsearch", "trino.plugin.elasticsearch"),
                new JsonModule(),
                new TypeDeserializerModule(),
                new ElasticsearchConnectorModule(),
                binder -> {
                    binder.bind(ElasticsearchClientFactory.class).to(DefaultElasticsearchClientFactory.class).in(Scopes.SINGLETON);
                },
                new ConnectorContextModule(catalogName, context));

        return app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config);
    }
}
