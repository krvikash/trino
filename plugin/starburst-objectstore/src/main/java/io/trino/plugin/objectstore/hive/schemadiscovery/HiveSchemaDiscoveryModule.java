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
package io.trino.plugin.objectstore.hive.schemadiscovery;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.schema.discovery.models.IdentifierConstraint;
import io.trino.plugin.base.connector.SystemTableProvider;
import io.trino.spi.catalog.CatalogName;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class HiveSchemaDiscoveryModule
        extends AbstractConfigurationAwareModule
{
    private final IdentifierConstraint schemaDiscoveryIdentifierConstraint;

    public HiveSchemaDiscoveryModule(IdentifierConstraint schemaDiscoveryIdentifierConstraint)
    {
        this.schemaDiscoveryIdentifierConstraint = requireNonNull(schemaDiscoveryIdentifierConstraint, "schemaDiscoveryIdentifierConstraint is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SchemaDiscoveryConfig.class);
        binder.bind(IdentifierConstraint.class).toInstance(schemaDiscoveryIdentifierConstraint);

        Multibinder<SystemTableProvider> systemTableProviders = newSetBinder(binder, SystemTableProvider.class);
        systemTableProviders.addBinding().to(SchemaDiscoverySystemTableProvider.class).in(Scopes.SINGLETON);
    }

    @ForSchemaDiscovery
    @Singleton
    @Provides
    public ExecutorService createSchemaDiscoveryExecutor(CatalogName catalogName, SchemaDiscoveryConfig config)
    {
        return newFixedThreadPool(
                config.getExecutorThreadCount(),
                daemonThreadsNamed("hive-schema-discovery-" + catalogName + "-%s"));
    }
}
