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

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
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
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final WorkScheduler workScheduler;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final Optional<String> defaultSchemaLocation;
    private final GlueClientProvider glueClientProvider;
    private final boolean isUniqueTableLocation;
    private final boolean hideMaterializedViewStorageTable;
    private final boolean scheduledMaterializedViewRefreshEnabled;
    private final boolean isIncrementalColumnMvRefreshEnabled;
    private final GlueMetastoreStats stats;
    private final boolean isUsingSystemSecurity;
    private final Executor metadataFetchingExecutor;
    private final ExecutorService icebergScanExecutor;

    @Inject
    public TrinoGlueCatalogFactory(
            CatalogName catalogName,
            WorkScheduler workScheduler,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig,
            IcebergScheduledMvRefreshConfig icebergScheduledMvRefreshConfig,
            IcebergIncrementalMvRefreshConfig icebergIncrementalMvRefreshConfig,
            IcebergGlueCatalogConfig catalogConfig,
            @UsingSystemSecurity boolean usingSystemSecurity,
            GlueMetastoreStats stats,
            GlueClientProvider glueClientProvider,
            @ForIcebergMetadata ExecutorService metadataExecutorService,
            @ForIcebergSplitManager ExecutorService icebergScanExecutor)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.workScheduler = requireNonNull(workScheduler, "workScheduler is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = nodeVersion.toString();
        this.defaultSchemaLocation = glueConfig.getDefaultWarehouseDir();
        this.glueClientProvider = requireNonNull(glueClientProvider, "glueClientProvider is null");
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.hideMaterializedViewStorageTable = icebergConfig.isHideMaterializedViewStorageTable();
        this.scheduledMaterializedViewRefreshEnabled = icebergScheduledMvRefreshConfig.isScheduledMaterializedViewRefreshEnabled();
        this.isIncrementalColumnMvRefreshEnabled = icebergIncrementalMvRefreshConfig.isMaterializedViewIncrementalColumnRefreshEnabled();
        this.stats = requireNonNull(stats, "stats is null");
        this.isUsingSystemSecurity = usingSystemSecurity;
        if (icebergConfig.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(metadataExecutorService, icebergConfig.getMetadataParallelism());
        }
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoGlueCatalog(
                catalogName,
                workScheduler,
                fileSystemFactory,
                fileIoFactory,
                typeManager,
                cacheTableMetadata,
                tableOperationsProvider,
                trinoVersion,
                glueClientProvider.get(identity),
                isUsingSystemSecurity,
                defaultSchemaLocation,
                isUniqueTableLocation,
                hideMaterializedViewStorageTable,
                scheduledMaterializedViewRefreshEnabled,
                isIncrementalColumnMvRefreshEnabled,
                metadataFetchingExecutor,
                icebergScanExecutor);
    }
}
