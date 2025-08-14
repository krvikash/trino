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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
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
import io.trino.plugin.base.Decorator;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class CachingHiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, new TypeLiteral<Decorator<HiveMetastore>>() {});
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
            Set<Decorator<HiveMetastore>> decorators,
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
        private final Set<Decorator<HiveMetastore>> decorators;

        public DecoratingHiveMetastoreFactory(HiveMetastoreFactory delegate, Set<Decorator<HiveMetastore>> decorators)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.decorators = ImmutableSet.copyOf(requireNonNull(decorators, "decorators is null"));
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
            return Decorator.combine(() -> delegate.createMetastore(identity), decorators);
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
