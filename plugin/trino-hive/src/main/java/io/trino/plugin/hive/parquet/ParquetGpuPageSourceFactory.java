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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.FileSystemReadExecutor;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;

public final class ParquetGpuPageSourceFactory
{
    private ParquetGpuPageSourceFactory() {}

    public static ConnectorGpuPageSource createGpuPageSource(
            TrinoInputFile inputFile,
            long start,
            long length,
            List<HiveColumnHandle> gpuColumns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<ColumnMapping> columnMappings,
            int domainCompactionThreshold,
            FileSystemReadExecutor fileSystemReadExecutor)
    {
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            FileFormatDataSourceStats stats = new FileFormatDataSourceStats();

            // todo; pass ParquetReaderOptions constructed from config+session from caller
            // https://starburstdata.atlassian.net/browse/ENG-13773
            // Raise the size of the max read because we are reading everything up front into an in-memory byte array.
            // The default for CPU is tailored for lazy materialization and early cut-off of page source.
            // Stay below G1's 16 MB humongous-allocation threshold (region size 32 MB); 1 KB slack for the byte[] header.
            DataSize bufferSize = DataSize.ofBytes(16L * 1024 * 1024 - 1024);
            ParquetReaderOptions options = ParquetReaderOptions.builder()
                    .withMaxBufferSize(bufferSize)
                    .withInitialBufferSize(bufferSize)
                    .build();
            // Hardcoded UTC: cuDF reads timestamps as raw UTC without applying hive.parquet.time-zone.
            // Using the configured zone for predicate pruning here would be inconsistent with the data
            // actually read (pruning on a shifted interpretation, reading raw). The upstream UTC guard
            // in HivePageSourceProvider ensures this factory is only called when parquetDateTimeZone
            // is already UTC; pinning it here as well documents the invariant at the predicate site.
            DateTimeZone timeZone = DateTimeZone.UTC;

            // todo; pass ParquetReaderOptions constructed from config+session from caller
            // https://starburstdata.atlassian.net/browse/ENG-13773
            ParquetReaderOptions parquetReaderOptions = ParquetReaderOptions.builder()
                    .withMaxFooterReadSize(options.getMaxFooterReadSize())
                    .build();

            ParquetMetadata parquetMetadata;
            List<RowGroupInfo> filteredRowGroups;
            try (ParquetDataSource footerSource = createDataSource(inputFile, OptionalLong.empty(), options, memoryContext, stats)) {
                // Read footer and get schema
                parquetMetadata = MetadataReader.readFooter(
                        footerSource,
                        parquetReaderOptions,
                        Optional.empty(),
                        Optional.empty());
                FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
                MessageType fileSchema = fileMetadata.getSchema();

                // Get requested schema (columns to read)
                boolean useColumnNames = true; // Hive uses column names, not field IDs
                Optional<MessageType> message = getParquetMessageType(gpuColumns, useColumnNames, fileSchema);
                MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

                // Build descriptors and predicates
                Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
                TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(
                        descriptorsByPath,
                        effectivePredicate,
                        fileSchema,
                        useColumnNames);
                // Use default coercion settings (no date/timestamp rebasing for minimal implementation)
                TupleDomainParquetPredicate predicate = buildPredicate(
                        requestedSchema,
                        parquetTupleDomain,
                        descriptorsByPath,
                        timeZone,
                        false, // convertDateToProleptic
                        false, // convertInt64TimestampProleptic
                        false); // convertInt96TimestampToProleptic

                filteredRowGroups = getFilteredRowGroups(
                        start,
                        length,
                        footerSource,
                        parquetMetadata,
                        ImmutableList.of(parquetTupleDomain),
                        ImmutableList.of(predicate),
                        descriptorsByPath,
                        timeZone,
                        domainCompactionThreshold,
                        options);
            }

            // Create fabricator
            ParquetFileFabricator fabricator = new ParquetFileFabricator(
                    inputFile,
                    filteredRowGroups,
                    gpuColumns,
                    new NameBasedColumnMatcher(),
                    options,
                    parquetMetadata,
                    fileSystemReadExecutor);

            return new GpuParquetPageSource(fabricator, gpuColumns, columnMappings);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Failed to create GPU Parquet page source", e);
        }
    }
}
