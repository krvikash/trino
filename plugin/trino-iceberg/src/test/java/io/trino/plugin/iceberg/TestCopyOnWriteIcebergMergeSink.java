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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.GpuParquetConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.encryption.PlaintextEncryptionManagerFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCopyOnWriteIcebergMergeSink
{
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "value", Types.LongType.get()));
    private static final long WRITER_MEMORY_USAGE = DataSize.of(64, MEGABYTE).toBytes();

    @Test
    void testRewriteReportsReaderAndWriterMemoryUsage()
    {
        // Reader memory rounds the rewrite total up past the writer's whole megabytes
        assertThat(peakRewriteMemoryUsage(10)).isGreaterThanOrEqualTo(WRITER_MEMORY_USAGE + DataSize.of(1, MEGABYTE).toBytes());
    }

    @Test
    void testRewriteOfFullyDeletedFileReportsWriterMemoryUsage()
    {
        assertThat(peakRewriteMemoryUsage(1)).isGreaterThanOrEqualTo(WRITER_MEMORY_USAGE);
    }

    // Rewrites a data file of the given row count with its first row deleted
    private static long peakRewriteMemoryUsage(int rowCount)
    {
        MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        IcebergFileWriterFactory fileWriterFactory = new IcebergFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), new FileFormatDataSourceStats(), new IcebergConfig(), new OrcWriterConfig(), new ParquetWriterConfig())
        {
            @Override
            public IcebergFileWriter createDataFileWriter(TrinoFileSystem fileSystem, Location outputPath, Schema icebergSchema, ConnectorSession session, IcebergFileFormat fileFormat, MetricsConfig metricsConfig, Map<String, String> storageProperties)
            {
                return new FixedMemoryUsageFileWriter(super.createDataFileWriter(fileSystem, outputPath, icebergSchema, session, fileFormat, metricsConfig, storageProperties), WRITER_MEMORY_USAGE);
            }
        };

        Location dataFile = Location.of("memory:///table/data/file-1.parquet");
        IcebergFileWriter dataFileWriter = fileWriterFactory.createDataFileWriter(fileSystem, dataFile, SCHEMA, SESSION, PARQUET, MetricsConfig.getDefault(), ImmutableMap.of());
        dataFileWriter.appendRows(new Page(createLongSequenceBlock(0, rowCount)));
        dataFileWriter.commit();

        AtomicLong peakReportedBytes = new AtomicLong();
        CopyOnWriteIcebergMergeSink mergeSink = new CopyOnWriteIcebergMergeSink(
                LocationProviders.locationsFor("memory:///table", ImmutableMap.of()),
                fileWriterFactory,
                fileSystem,
                ImmutableMap.of(),
                JsonCodec.jsonCodec(CommitTaskData.class),
                SESSION,
                PARQUET,
                ImmutableMap.of(),
                SCHEMA,
                ImmutableMap.of(0, PartitionSpec.unpartitioned()),
                new NoOpPageSink(),
                Optional.empty(),
                1,
                new IcebergPageSourceProviderFactory(
                        new DefaultIcebergFileSystemFactory(fileSystemFactory),
                        FILE_IO_FACTORY,
                        new FileFormatDataSourceStats(),
                        new OrcReaderConfig(),
                        new ParquetReaderConfig(),
                        new GpuParquetConfig(),
                        new IcebergConfig(),
                        TESTING_TYPE_MANAGER,
                        BLOCKS_HASH_FACTORY,
                        ParquetFooterCache.noop(),
                        new PlaintextEncryptionManagerFactory()),
                ImmutableList.of(getColumnHandle(SCHEMA.findField("value"), TESTING_TYPE_MANAGER)),
                new IcebergTableCredentials(ImmutableMap.of(), ImmutableList.of()),
                ImmutableMap.of(dataFile.toString(), (long) rowCount),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                2,
                bytes -> peakReportedBytes.accumulateAndGet(bytes, Math::max));

        mergeSink.storeMergedRows(deletionPage(dataFile, 0));
        mergeSink.finish().join();
        return peakReportedBytes.get();
    }

    // Page with one bigint data column, the operation and case number columns and the merge row id column, deleting one row
    private static Page deletionPage(Location dataFile, long rowPosition)
    {
        Block rowIdBlock = RowBlock.fromFieldBlocks(1, new Block[] {
                createStringsBlock(dataFile.toString()),
                createLongsBlock(rowPosition),
                createIntsBlock(0),
                createStringsBlock(PartitionData.toJson(new PartitionData(new Object[0]))),
                createLongsBlock((Long) null),
        });
        return new Page(createLongsBlock(rowPosition), createTypedLongsBlock(TINYINT, (long) DELETE_OPERATION_NUMBER), createIntsBlock(0), rowIdBlock);
    }

    private static final class NoOpPageSink
            implements ConnectorPageSink
    {
        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort() {}
    }

    private static final class FixedMemoryUsageFileWriter
            implements IcebergFileWriter
    {
        private final IcebergFileWriter delegate;
        private final long memoryUsage;

        private FixedMemoryUsageFileWriter(IcebergFileWriter delegate, long memoryUsage)
        {
            this.delegate = delegate;
            this.memoryUsage = memoryUsage;
        }

        @Override
        public FileMetrics getFileMetrics()
        {
            return delegate.getFileMetrics();
        }

        @Override
        public long getWrittenBytes()
        {
            return delegate.getWrittenBytes();
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsage;
        }

        @Override
        public void appendRows(Page dataPage)
        {
            delegate.appendRows(dataPage);
        }

        @Override
        public RollbackAction commit()
        {
            return delegate.commit();
        }

        @Override
        public void rollback()
        {
            delegate.rollback();
        }

        @Override
        public long getValidationCpuNanos()
        {
            return delegate.getValidationCpuNanos();
        }
    }
}
