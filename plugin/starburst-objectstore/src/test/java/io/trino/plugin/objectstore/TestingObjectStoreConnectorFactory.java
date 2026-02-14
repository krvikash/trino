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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.TestingDeltaLakeExtensionsModule;
import io.trino.plugin.deltalake.metastore.NoOpVendedCredentialsProvider;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.deltalake.transactionlog.writer.TestingLocalTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;
import io.trino.plugin.hive.metastore.MetastoreTypeConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalogFactory;
import io.trino.plugin.iceberg.procedure.MigrateProcedure;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.procedure.Procedure;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static java.util.Objects.requireNonNull;

public class TestingObjectStoreConnectorFactory
        implements ConnectorFactory
{
    private final String connectorName;
    private final Optional<HiveMetastore> metastore;
    private final Optional<Module> hiveModule;
    private final Optional<TrinoFileSystemFactory> localFileSystemFactory;
    private final Optional<TransactionLogSynchronizer> localTransactionLogSynchronizer;

    public TestingObjectStoreConnectorFactory(String connectorName, Optional<HiveMetastore> metastore, Optional<Module> hiveModule, Optional<Path> localFileSystemRootPath)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hiveModule = requireNonNull(hiveModule, "hiveModule is null");
        requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        this.localFileSystemFactory = localFileSystemRootPath.map(LocalFileSystemFactory::new);
        this.localTransactionLogSynchronizer = localFileSystemFactory.map(factory -> new TestingLocalTransactionLogSynchronizer(new DefaultDeltaLakeFileSystemFactory(factory, new NoOpVendedCredentialsProvider())));
    }

    @Override
    public String getName()
    {
        return connectorName;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return switch (connectorName) {
            case STARBURST_OBJECTSTORE -> InternalStarburstObjectStoreConnectorFactory.createConnector(
                    catalogName,
                    config,
                    metastore,
                    hiveModule.orElse(EMPTY_MODULE),
                    metastore.map(TestingIcebergFileMetastoreCatalogModule::new),
                    metastore.map(TestingDeltaLakeMetastoreModule::new),
                    binder -> {
                        binder.install(new TestingDeltaLakeExtensionsModule());
                        localFileSystemFactory.ifPresent(filesystemFactory -> newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                                .addBinding("local").toInstance(filesystemFactory));
                        localTransactionLogSynchronizer.ifPresent(logSynchronizer -> newMapBinder(binder, String.class, TransactionLogSynchronizer.class)
                                .addBinding("local").toInstance(logSynchronizer));

                        configBinder(binder).bindConfig(MetastoreTypeConfig.class);
                    },
                    context);
            default -> throw new IllegalArgumentException("Unknown connector: " + connectorName);
        };
    }

    private static class TestingIcebergFileMetastoreCatalogModule
            extends AbstractConfigurationAwareModule
    {
        private final HiveMetastore metastore;

        private TestingIcebergFileMetastoreCatalogModule(HiveMetastore metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
        }

        @Override
        protected void setup(Binder binder)
        {
            binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(HiveMetastoreFactory.ofInstance(metastore, false));
            install(new CachingHiveMetastoreModule());
            binder.bind(IcebergTableOperationsProvider.class).to(FileMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
            binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);

            configBinder(binder).bindConfigDefaults(CachingHiveMetastoreConfig.class, config -> {
                // caching metastore wrapper isn't created by default
                config.setStatsCacheTtl(new Duration(0, TimeUnit.SECONDS));
            });

            Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
            procedures.addBinding().toProvider(MigrateProcedure.class).in(Scopes.SINGLETON);
        }
    }
}
