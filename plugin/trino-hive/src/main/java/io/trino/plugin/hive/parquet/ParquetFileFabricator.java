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

import ai.rapids.cudf.HostMemoryBuffer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.MessageTypeConverter;
import io.trino.parquet.writer.ParquetTypeConverter;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static java.lang.Math.clamp;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Fabricates optimized mini-Parquet files in memory by extracting only
 * the needed columns and row groups from the original Parquet file.
 */
public class ParquetFileFabricator
        implements RuntimeCloseable
{
    /**
     * Result of Parquet fabrication containing both the fabricated file bytes
     * and the total row count (after filtering).
     */
    public static final class FabricatedParquet
            implements RuntimeCloseable
    {
        private final @Own Optional<Buffers> data;
        private final long rowCount;

        public FabricatedParquet(Optional<Buffers> data, long rowCount)
        {
            requireNonNull(data, "data is null");
            this.data = data;
            this.rowCount = rowCount;
        }

        public @Borrow Optional<Buffers> data()
        {
            return data;
        }

        public long rowCount()
        {
            return rowCount;
        }

        @Override
        public void close()
        {
            data.ifPresent(Buffers::close);
        }
    }

    private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final int PARQUET_MAGIC_LENGTH = PARQUET_MAGIC.length;
    private static final int FOOTER_LENGTH_SIZE = 4;

    private final long splitStart;
    private final long splitLength;
    private final ParquetDataSource dataSource;
    private final MessageType requestedSchema;
    private final List<TupleDomain<ColumnDescriptor>> parquetTupleDomains;
    private final List<TupleDomainParquetPredicate> parquetPredicates;
    private final Map<List<String>, ColumnDescriptor> descriptorsByPath;
    private final DateTimeZone timeZone;
    private final int domainCompactionThreshold;
    private final AggregatedMemoryContext memoryContext;
    private final ParquetReaderOptions options;
    private final ParquetMetadata parquetMetadata;

    public ParquetFileFabricator(
            long splitStart,
            long splitLength,
            ParquetDataSource dataSource,
            MessageType requestedSchema,
            List<TupleDomain<ColumnDescriptor>> parquetTupleDomains,
            List<TupleDomainParquetPredicate> parquetPredicates,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            DateTimeZone timeZone,
            int domainCompactionThreshold,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            ParquetMetadata parquetMetadata)
    {
        this.splitStart = splitStart;
        this.splitLength = splitLength;
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.requestedSchema = requireNonNull(requestedSchema, "requestedSchema is null");
        this.parquetTupleDomains = ImmutableList.copyOf(requireNonNull(parquetTupleDomains, "parquetTupleDomains is null"));
        this.parquetPredicates = ImmutableList.copyOf(requireNonNull(parquetPredicates, "parquetPredicates is null"));
        this.descriptorsByPath = requireNonNull(descriptorsByPath, "descriptorsByPath is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.options = requireNonNull(options, "options is null");
        this.parquetMetadata = requireNonNull(parquetMetadata, "parquetMetadata is null");
    }

    @Override
    public void close()
    {
        try {
            dataSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public @Move FabricatedParquet fabricate()
            throws IOException
    {
        try {
            List<RowGroupInfo> filteredRowGroups = getFilteredRowGroups(
                    splitStart,
                    splitLength,
                    dataSource,
                    parquetMetadata,
                    parquetTupleDomains,
                    parquetPredicates,
                    descriptorsByPath,
                    timeZone,
                    domainCompactionThreshold,
                    options);

            if (filteredRowGroups.isEmpty()) {
                return new FabricatedParquet(Optional.empty(), 0);
            }

            return writeFabricatedFile(filteredRowGroups, requestedSchema, parquetMetadata.getFileMetaData());
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, format("Error fabricating Parquet file from %s: %s", dataSource.getId(), requireNonNullElse(e.getMessage(), e)), e);
        }
    }

    private @Move FabricatedParquet writeFabricatedFile(
            List<RowGroupInfo> rowGroups,
            MessageType clippedSchema,
            FileMetadata originalFileMetadata)
            throws IOException
    {
        // Collect all disk ranges for columns included in the clipped schema
        ListMultimap<Integer, DiskRange> diskRanges = ArrayListMultimap.create();
        int chunkCount = 0;
        for (RowGroupInfo rowGroupInfo : rowGroups) {
            for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                if (!isColumnInSchema(column.getPath(), clippedSchema)) {
                    continue;
                }
                diskRanges.put(chunkCount++, new DiskRange(column.getStartingPos(), column.getTotalSize()));
            }
        }

        Map<Integer, ChunkedInputStream> chunkStreams = dataSource.planRead(diskRanges, memoryContext);
        try {
            return writeFabricatedFile(rowGroups, clippedSchema, originalFileMetadata, chunkStreams, chunkCount);
        }
        finally {
            chunkStreams.values().forEach(ChunkedInputStream::close);
        }
    }

    private @Move FabricatedParquet writeFabricatedFile(
            List<RowGroupInfo> rowGroups,
            MessageType clippedSchema,
            FileMetadata originalFileMetadata,
            Map<Integer, ChunkedInputStream> chunkStreams,
            int expectedChunkCount)
            throws IOException
    {
        // Build the fabricated Parquet as a list of host buffers — one for the magic header,
        // one per column chunk, and one for the footer + footer length + trailing magic. cuDF
        // logically concatenates these in readParquet(opts, HostMemoryBuffer...).
        List<HostMemoryBuffer> buffers = new ArrayList<>();
        try {
            buffers.add(allocateAndCopy(PARQUET_MAGIC, 0, PARQUET_MAGIC_LENGTH));
            long currentOffset = PARQUET_MAGIC_LENGTH;

            List<RowGroup> fabricatedRowGroups = new ArrayList<>();
            long totalRowCount = 0;
            int chunkIndex = 0;

            for (RowGroupInfo rowGroupInfo : rowGroups) {
                List<ColumnChunk> fabricatedColumns = new ArrayList<>();
                long rowGroupStartOffset = currentOffset;
                long totalCompressedSize = 0;
                long totalUncompressedSize = 0;

                for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                    if (!isColumnInSchema(column.getPath(), clippedSchema)) {
                        continue;
                    }

                    long chunkOffset = column.getStartingPos();
                    long chunkSize = column.getTotalSize();

                    ChunkedInputStream nextChunk = chunkStreams.get(chunkIndex++);
                    buffers.add(allocateAndCopyChunk(nextChunk, toIntExact(chunkSize)));

                    long offsetAdjustment = currentOffset - chunkOffset;

                    List<org.apache.parquet.format.Encoding> encodings = new ArrayList<>();
                    for (org.apache.parquet.column.Encoding encoding : column.getEncodings()) {
                        encodings.add(org.apache.parquet.format.Encoding.valueOf(encoding.name()));
                    }

                    List<String> pathList = ImmutableList.copyOf(column.getPath().toArray());
                    ColumnMetaData columnMetaData = new ColumnMetaData(
                            ParquetTypeConverter.getType(column.getPrimitiveType().getPrimitiveTypeName()),
                            encodings,
                            pathList,
                            CompressionCodec.valueOf(column.getCodec().name()),
                            column.getValueCount(),
                            column.getTotalUncompressedSize(),
                            column.getTotalSize(),
                            currentOffset);

                    if (column.getDictionaryPageOffset() > 0) {
                        columnMetaData.setDictionary_page_offset(column.getDictionaryPageOffset() + offsetAdjustment);
                    }

                    if (column.getStatistics() != null && !column.getStatistics().isEmpty()) {
                        Statistics stats = new Statistics();
                        if (column.getStatistics().hasNonNullValue()) {
                            if (column.getStatistics().genericGetMin() != null) {
                                stats.setMin_value(column.getStatistics().getMinBytes());
                            }
                            if (column.getStatistics().genericGetMax() != null) {
                                stats.setMax_value(column.getStatistics().getMaxBytes());
                            }
                        }
                        stats.setNull_count(column.getStatistics().getNumNulls());
                        columnMetaData.setStatistics(stats);
                    }

                    ColumnChunk columnChunk = new ColumnChunk(rowGroupStartOffset);
                    columnChunk.setMeta_data(columnMetaData);
                    fabricatedColumns.add(columnChunk);

                    totalCompressedSize += column.getTotalSize();
                    totalUncompressedSize += column.getTotalUncompressedSize();
                    currentOffset += chunkSize;
                }

                long rowCount = rowGroupInfo.prunedBlockMetadata().getRowCount();
                totalRowCount += rowCount;

                RowGroup rowGroup = new RowGroup(
                        fabricatedColumns,
                        totalCompressedSize + totalUncompressedSize,
                        rowCount);
                rowGroup.setTotal_compressed_size(totalCompressedSize);
                rowGroup.setFile_offset(rowGroupStartOffset);
                fabricatedRowGroups.add(rowGroup);
            }

            verify(chunkIndex == expectedChunkCount, "Expected %s chunks but processed %s", expectedChunkCount, chunkIndex);

            DynamicSliceOutput footerThrift = new DynamicSliceOutput(parquetMetadata.getCompleteFooterSize().orElseThrow(() -> new IllegalStateException("Complete original footer size unknown")));
            writeFooter(footerThrift, fabricatedRowGroups, clippedSchema, originalFileMetadata);
            Slice footerSlice = footerThrift.slice();
            int footerSize = footerSlice.length();
            int trailerSize = footerSize + FOOTER_LENGTH_SIZE + PARQUET_MAGIC_LENGTH;

            ByteBuffer footerLengthBuffer = ByteBuffer.allocate(FOOTER_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            footerLengthBuffer.putInt(footerSize);

            try (ClosingRef<HostMemoryBuffer> trailer = ClosingRef.own(HostMemoryBuffer.allocate(trailerSize))) {
                trailer.borrow().setBytes(0, footerSlice.byteArray(), footerSlice.byteArrayOffset(), footerSize);
                trailer.borrow().setBytes(footerSize, footerLengthBuffer.array(), 0, FOOTER_LENGTH_SIZE);
                trailer.borrow().setBytes(footerSize + FOOTER_LENGTH_SIZE, PARQUET_MAGIC, 0, PARQUET_MAGIC_LENGTH);
                buffers.add(trailer.take());
            }
            return new FabricatedParquet(Optional.of(new Buffers(buffers)), totalRowCount);
        }
        catch (Throwable t) {
            closeAllSuppress(t, buffers.toArray(HostMemoryBuffer[]::new));
            throw t;
        }
    }

    private static @Own HostMemoryBuffer allocateAndCopy(byte[] source, int sourceOffset, int length)
    {
        try (ClosingRef<HostMemoryBuffer> buffer = ClosingRef.own(HostMemoryBuffer.allocate(length))) {
            buffer.borrow().setBytes(0, source, sourceOffset, length);
            return buffer.take();
        }
    }

    private static @Own HostMemoryBuffer allocateAndCopyChunk(ChunkedInputStream in, int length)
            throws IOException
    {
        try (ClosingRef<HostMemoryBuffer> buffer = ClosingRef.own(HostMemoryBuffer.allocate(length))) {
            long position = 0;
            int remaining = length;
            while (remaining > 0) {
                // Pull only what is in the current sub-slice — that read is zero-copy from the
                // underlying byte array — then copy straight into the destination buffer.
                int toRead = clamp(in.available(), 1, remaining);
                Slice slice = in.getSlice(toRead);
                buffer.borrow().setBytes(position, slice.byteArray(), slice.byteArrayOffset(), toRead);
                position += toRead;
                remaining -= toRead;
            }
            return buffer.take();
        }
    }

    private void writeFooter(
            OutputStream outputStream,
            List<RowGroup> rowGroups,
            MessageType schema,
            FileMetadata originalFileMetadata)
            throws IOException
    {
        long totalRows = rowGroups.stream()
                .mapToLong(RowGroup::getNum_rows)
                .sum();

        FileMetaData fileMetaData = new FileMetaData(
                1,
                MessageTypeConverter.toParquetSchema(schema),
                totalRows,
                rowGroups);

        fileMetaData.setCreated_by(originalFileMetadata.getCreatedBy());

        if (originalFileMetadata.getKeyValueMetaData() != null && !originalFileMetadata.getKeyValueMetaData().isEmpty()) {
            List<KeyValue> keyValueMetadata = new ArrayList<>();
            for (Entry<String, String> entry : originalFileMetadata.getKeyValueMetaData().entrySet()) {
                keyValueMetadata.add(new KeyValue(entry.getKey()).setValue(entry.getValue()));
            }
            fileMetaData.setKey_value_metadata(keyValueMetadata);
        }

        Util.writeFileMetaData(fileMetaData, outputStream);
    }

    private static boolean isColumnInSchema(ColumnPath columnPath, MessageType schema)
    {
        return schema.containsPath(columnPath.toArray());
    }
}
