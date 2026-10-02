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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates.SchemaMappingRule;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.iceberg.ForIcebergMetadata;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergIncrementalMvRefreshConfig;
import io.trino.plugin.iceberg.IcebergScheduledMvRefreshConfig;
import io.trino.plugin.iceberg.WorkScheduler;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.Node;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Serves the namespaces of the default Glue account together with the prefixed namespaces of the mapped accounts.
 */
public class SchemaMappingTrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final GlueMetastoreStats stats;
    private final TrinoGlueCatalogFactory defaultFactory;
    private final Map<String, TrinoCatalogFactory> factoriesByPrefix;
    private final List<GlueClient> ownedGlueClients;

    @Inject
    public SchemaMappingTrinoGlueCatalogFactory(
            CatalogName catalogName,
            WorkScheduler workScheduler,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            Node currentNode,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig,
            IcebergScheduledMvRefreshConfig icebergScheduledMvRefreshConfig,
            IcebergIncrementalMvRefreshConfig icebergIncrementalMvRefreshConfig,
            IcebergGlueCatalogConfig catalogConfig,
            @UsingSystemSecurity boolean usingSystemSecurity,
            GlueMetastoreStats stats,
            GlueClientProvider glueClientProvider,
            @ForIcebergMetadata ExecutorService metadataExecutorService,
            @ForIcebergSplitManager ExecutorService icebergScanExecutor,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.stats = requireNonNull(stats, "stats is null");
        defaultFactory = new TrinoGlueCatalogFactory(
                catalogName,
                workScheduler,
                fileSystemFactory,
                fileIoFactory,
                typeManager,
                tableOperationsProvider,
                nodeVersion,
                glueConfig,
                icebergConfig,
                icebergScheduledMvRefreshConfig,
                icebergIncrementalMvRefreshConfig,
                catalogConfig,
                usingSystemSecurity,
                stats,
                glueClientProvider,
                metadataExecutorService,
                icebergScanExecutor);

        ImmutableList.Builder<GlueClient> ownedGlueClients = ImmutableList.builder();
        Function<SchemaMappingRule, TrinoCatalogFactory> createFactory = rule -> {
            if (rule.catalogId().isEmpty()) {
                return defaultFactory;
            }
            GlueClient ruleGlueClient = SchemaMappingDelegates.createGlueClient(glueConfig, rule.catalogId());
            ownedGlueClients.add(ruleGlueClient);
            GlueMetastoreStats ruleStats = new GlueMetastoreStats();
            DefaultGlueClientProvider ruleGlueClientProvider = new DefaultGlueClientProvider(ruleGlueClient, catalogConfig, currentNode, catalogName, ruleStats);
            return new TrinoGlueCatalogFactory(
                    catalogName,
                    workScheduler,
                    fileSystemFactory,
                    fileIoFactory,
                    typeManager,
                    new GlueIcebergTableOperationsProvider(
                            fileSystemFactory,
                            fileIoFactory,
                            typeManager,
                            catalogConfig,
                            ruleGlueClientProvider,
                            encryptionManagerFactory),
                    nodeVersion,
                    glueConfig,
                    icebergConfig,
                    icebergScheduledMvRefreshConfig,
                    icebergIncrementalMvRefreshConfig,
                    catalogConfig,
                    usingSystemSecurity,
                    ruleStats,
                    ruleGlueClientProvider,
                    metadataExecutorService,
                    icebergScanExecutor);
        };
        factoriesByPrefix = glueConfig.getSchemaMappingRules().stream()
                .collect(toImmutableMap(SchemaMappingRule::prefix, createFactory));
        this.ownedGlueClients = ownedGlueClients.build();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @PreDestroy
    public void destroy()
    {
        ownedGlueClients.forEach(GlueClient::close);
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        TrinoCatalog defaultCatalog = defaultFactory.create(identity);
        if (factoriesByPrefix.isEmpty()) {
            return defaultCatalog;
        }
        Map<String, TrinoCatalog> delegatesByPrefix = factoriesByPrefix.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            if (entry.getValue() == defaultFactory) {
                                return defaultCatalog;
                            }
                            return entry.getValue().create(identity);
                        }));
        return new SchemaMappingTrinoGlueCatalog(catalogName, defaultCatalog, delegatesByPrefix);
    }
}
