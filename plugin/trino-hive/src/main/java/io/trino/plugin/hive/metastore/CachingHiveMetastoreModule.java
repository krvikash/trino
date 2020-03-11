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
package io.trino.plugin.hive.metastore;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.MeasuredHiveMetastore.MeasuredMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.metastore.cache.ImpersonationCachingConfig;
import io.trino.metastore.cache.SharedHiveMetastoreCache;
import io.trino.metastore.cache.SharedHiveMetastoreCache.CachingHiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class CachingHiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, HiveMetastoreDecorator.class);
        configBinder(binder).bindConfig(CachingHiveMetastoreConfig.class);
        // TODO this should only be bound when impersonation is actually enabled
        configBinder(binder).bindConfig(ImpersonationCachingConfig.class);
        binder.bind(SharedHiveMetastoreCache.class).in(Scopes.SINGLETON);
        // export under the old name, for backwards compatibility
        newExporter(binder).export(HiveMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof CachingHiveMetastoreModule;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Provides
    @Singleton
    public static HiveMetastoreFactory createHiveMetastore(
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            Set<HiveMetastoreDecorator> decorators,
            SharedHiveMetastoreCache sharedHiveMetastoreCache)
    {
        metastoreFactory = new DecoratingHiveMetastoreFactory(metastoreFactory, decorators);

        // cross TX metastore cache is enabled wrapper with caching metastore
        return sharedHiveMetastoreCache.createCachingHiveMetastoreFactory(new MeasuredMetastoreFactory(metastoreFactory));
    }

    private static class DecoratingHiveMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastoreFactory delegate;
        private final List<HiveMetastoreDecorator> sortedDecorators;

        public DecoratingHiveMetastoreFactory(HiveMetastoreFactory delegate, Set<HiveMetastoreDecorator> decorators)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");

            this.sortedDecorators = decorators.stream()
                    .sorted(comparing(HiveMetastoreDecorator::getPriority))
                    .collect(toImmutableList());
        }

        @Override
        public boolean hasBuiltInCaching()
        {
            return delegate.hasBuiltInCaching();
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return delegate.isImpersonationEnabled();
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            HiveMetastore metastore = delegate.createMetastore(identity);
            for (HiveMetastoreDecorator decorator : sortedDecorators) {
                metastore = decorator.decorate(metastore);
            }
            return metastore;
        }
    }

    @Provides
    @Singleton
    public static Optional<CachingHiveMetastore> createHiveMetastore(HiveMetastoreFactory metastoreFactory)
    {
        if (metastoreFactory instanceof CachingHiveMetastoreFactory cachingHiveMetastoreFactory) {
            return Optional.of(cachingHiveMetastoreFactory.getMetastore());
        }
        return Optional.empty();
    }
}
