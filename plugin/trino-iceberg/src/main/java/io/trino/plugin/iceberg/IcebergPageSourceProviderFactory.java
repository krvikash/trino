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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.GpuParquetConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.GpuPageSourceSupport;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private static final Logger log = Logger.get(IcebergPageSourceProviderFactory.class);

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final long gpuScanMaxPageSizeBytes;
    private final DateTimeZone dateTimeZone;
    private final TypeManager typeManager;
    private final BlocksHashFactory blocksHashFactory;
    private final ParquetFooterCache parquetFooterCache;
    private final EncryptionManagerFactory encryptionManagerFactory;

    @Inject
    public IcebergPageSourceProviderFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            GpuParquetConfig gpuParquetConfig,
            IcebergConfig icebergConfig,
            TypeManager typeManager,
            BlocksHashFactory blocksHashFactory,
            ParquetFooterCache parquetFooterCache,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = orcReaderConfig.toOrcReaderOptions();
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.gpuScanMaxPageSizeBytes = gpuParquetConfig.getMaxPageSize().toBytes();
        this.dateTimeZone = icebergConfig.getDateTimeZone();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blocksHashFactory = requireNonNull(blocksHashFactory, "blocksHashFactory is null");
        this.parquetFooterCache = requireNonNull(parquetFooterCache, "parquetFooterCache is null");
        this.encryptionManagerFactory = requireNonNull(encryptionManagerFactory, "encryptionManagerFactory is null");
    }

    @Override
    public GpuPageSourceSupport getGpuPageSourceSupport(ConnectorTableHandle connectorTableHandle, List<ColumnHandle> columns)
    {
        for (ColumnHandle column : columns) {
            IcebergColumnHandle icebergColumn = (IcebergColumnHandle) column;

            if (!icebergColumn.isBaseColumn()) {
                log.debug("GPU page source not supported: column '%s' is not a base column", icebergColumn.getName());
                return GpuPageSourceSupport.unsupported("Non-primitive columns are not supported");
            }
            if (isMetadataColumnId(icebergColumn.getId()) && !isMetadataColumnSupportedForGpu(icebergColumn)) {
                log.debug("GPU page source not supported: metadata column '%s' is not supported", icebergColumn.getName());
                return GpuPageSourceSupport.unsupported("Unsupported metadata column");
            }
            if (icebergColumn.getType() instanceof TimestampWithTimeZoneType && !dateTimeZone.equals(UTC)) {
                // The CPU reader packs values in iceberg.time-zone, while the GPU representation
                // of a timestamp with time zone holds the instant alone, in UTC
                log.debug("GPU page source not supported: column '%s' is timestamp with time zone and the time zone is %s", icebergColumn.getName(), dateTimeZone);
                return GpuPageSourceSupport.unsupported("timestamp with time zone columns are not supported with a non-UTC time zone");
            }
        }
        return GpuPageSourceSupport.SUPPORTED;
    }

    private static boolean isMetadataColumnSupportedForGpu(IcebergColumnHandle column)
    {
        return column.isPathColumn() || column.isFileModifiedTimeColumn() || column.isPartitionColumn();
    }

    @Override
    public IcebergPageSourceProvider createPageSourceProvider(MemoryContext memoryContext)
    {
        return new IcebergPageSourceProvider(fileSystemFactory, fileIoFactory, fileFormatDataSourceStats, orcReaderOptions, parquetReaderOptions, dateTimeZone, typeManager, parquetFooterCache, blocksHashFactory, encryptionManagerFactory, gpuScanMaxPageSizeBytes, memoryContext);
    }
}
