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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.delete.DeletionVectorWriter;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionStatisticsWriter;
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final LocationAccessControl locationAccessControl;
    private final AiModelAccessControl aiModelAccessControl;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalogFactory catalogFactory;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TableStatisticsReader tableStatisticsReader;
    private final TableStatisticsWriter tableStatisticsWriter;
    private final PartitionStatisticsWriter partitionStatisticsWriter;
    private final Optional<HiveMetastoreFactory> metastoreFactory;
    private final int maxFormatVersion;
    private final boolean addFilesProcedureEnabled;
    private final DateTimeZone dateTimeZone;
    private final Predicate<String> allowedExtraProperties;
    private final ExecutorService icebergScanExecutor;
    private final Executor metadataFetchingExecutor;
    private final ExecutorService icebergPlanningExecutor;
    private final ExecutorService icebergFileDeleteExecutor;
    private final int materializedViewRefreshMaxSnapshotsToExpire;
    private final Duration materializedViewRefreshSnapshotRetentionPeriod;
    private final DeletionVectorWriter deletionVectorWriter;

    @Inject
    public IcebergMetadataFactory(
            LocationAccessControl locationAccessControl,
            AiModelAccessControl aiModelAccessControl,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalogFactory catalogFactory,
            IcebergFileSystemFactory fileSystemFactory,
            TableStatisticsReader tableStatisticsReader,
            TableStatisticsWriter tableStatisticsWriter,
            PartitionStatisticsWriter partitionStatisticsWriter,
            DeletionVectorWriter deletionVectorWriter,
            @RawHiveMetastoreFactory Optional<HiveMetastoreFactory> metastoreFactory,
            @ForIcebergSplitManager ExecutorService icebergScanExecutor,
            @ForIcebergMetadata ExecutorService metadataExecutorService,
            @ForIcebergPlanning ExecutorService icebergPlanningExecutor,
            @ForIcebergFileDelete ExecutorService icebergFileDeleteExecutor,
            IcebergConfig config)
    {
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.aiModelAccessControl = requireNonNull(aiModelAccessControl, "aiModelAccessControl is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsReader = requireNonNull(tableStatisticsReader, "tableStatisticsReader is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
        this.partitionStatisticsWriter = requireNonNull(partitionStatisticsWriter, "partitionStatisticsWriter is null");
        this.deletionVectorWriter = requireNonNull(deletionVectorWriter, "deletionVectorWriter is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
        this.maxFormatVersion = config.getMaxFormatVersion();
        this.addFilesProcedureEnabled = config.isAddFilesProcedureEnabled();
        this.dateTimeZone = config.getDateTimeZone();
        if (config.getAllowedExtraProperties().equals(ImmutableList.of("*"))) {
            this.allowedExtraProperties = _ -> true;
        }
        else {
            this.allowedExtraProperties = ImmutableSet.copyOf(requireNonNull(config.getAllowedExtraProperties(), "allowedExtraProperties is null"))::contains;
        }

        if (config.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(metadataExecutorService, config.getMetadataParallelism());
        }
        this.icebergPlanningExecutor = requireNonNull(icebergPlanningExecutor, "icebergPlanningExecutor is null");
        this.icebergFileDeleteExecutor = requireNonNull(icebergFileDeleteExecutor, "icebergFileDeleteExecutor is null");
        this.materializedViewRefreshMaxSnapshotsToExpire = config.getMaterializedViewRefreshMaxSnapshotsToExpire();
        this.materializedViewRefreshSnapshotRetentionPeriod = config.getMaterializedViewRefreshSnapshotRetentionPeriod();
    }

    public IcebergMetadata create(ConnectorIdentity identity)
    {
        return new IcebergMetadata(
                locationAccessControl,
                aiModelAccessControl,
                typeManager,
                commitTaskCodec,
                catalogFactory.create(identity),
                fileSystemFactory,
                tableStatisticsReader,
                tableStatisticsWriter,
                partitionStatisticsWriter,
                deletionVectorWriter,
                metastoreFactory,
                maxFormatVersion,
                addFilesProcedureEnabled,
                allowedExtraProperties,
                dateTimeZone,
                icebergScanExecutor,
                metadataFetchingExecutor,
                icebergPlanningExecutor,
                icebergFileDeleteExecutor,
                materializedViewRefreshMaxSnapshotsToExpire,
                materializedViewRefreshSnapshotRetentionPeriod);
    }
}
