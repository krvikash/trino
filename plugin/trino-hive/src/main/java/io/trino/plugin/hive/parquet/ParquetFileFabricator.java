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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.MessageTypeConverter;
import io.trino.parquet.writer.ParquetTypeConverter;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.MemoryAllocation;
import io.trino.spi.gpu.MemoryAmount;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.AbstractParquetDataSource.mergeAdjacentDiskRanges;
import static io.trino.parquet.AbstractParquetDataSource.splitLargeRange;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
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
        private final @Own MemoryAllocation allocation;
        private final @Own Optional<Buffers> data;
        private final long rowCount;

        public FabricatedParquet(MemoryAllocation allocation, Optional<Buffers> data, long rowCount)
        {
            this.allocation = requireNonNull(allocation, "allocation is null");
            this.data = requireNonNull(data, "data is null");
            checkArgument(rowCount >= 0, "rowCount must be non-negative: %s", rowCount);
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
            allocation.close();
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
    private final ConnectorGpuMemoryContext gpuMemoryContext;
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
            ConnectorGpuMemoryContext gpuMemoryContext,
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
        this.gpuMemoryContext = requireNonNull(gpuMemoryContext, "gpuMemoryContext is null");
        this.options = requireNonNull(options, "options is null");
        this.parquetMetadata = requireNonNull(parquetMetadata, "parquetMetadata is null");
    }

    @Override
    public void close()
    {
        try (var closer = Closer.create()) {
            closer.register(dataSource);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public @Move FabricatedParquet fabricate()
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
                return new FabricatedParquet(gpuMemoryContext.allocate(MemoryAmount.ZERO), Optional.empty(), 0);
            }

            // Collect all disk ranges for columns included in the requested schema
            ImmutableList.Builder<DiskRange> chunkRanges = ImmutableList.builder();
            for (RowGroupInfo rowGroupInfo : filteredRowGroups) {
                for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                    if (isColumnInSchema(column.getPath(), requestedSchema)) {
                        chunkRanges.add(new DiskRange(column.getStartingPos(), column.getTotalSize()));
                    }
                }
            }

            return writeFabricatedFile(filteredRowGroups, requestedSchema, parquetMetadata.getFileMetaData(), chunkRanges.build());
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, format("Error fabricating Parquet file from %s: %s", dataSource.getId(), requireNonNullElse(e.getMessage(), e)), e);
        }
    }

    private @Move FabricatedParquet writeFabricatedFile(
            List<RowGroupInfo> rowGroups,
            MessageType clippedSchema,
            FileMetadata originalFileMetadata,
            List<DiskRange> chunkRanges)
            throws IOException
    {
        int originalFooterSize = parquetMetadata.getCompleteFooterSize().orElseThrow(() -> new IllegalStateException("Complete original footer size unknown"));

        long estimateBuffersSize = PARQUET_MAGIC_LENGTH;
        for (RowGroupInfo rowGroupInfo : rowGroups) {
            for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                if (isColumnInSchema(column.getPath(), clippedSchema)) {
                    estimateBuffersSize += toIntExact(column.getTotalSize());
                }
            }
        }
        estimateBuffersSize += originalFooterSize + FOOTER_LENGTH_SIZE + PARQUET_MAGIC_LENGTH;

        // Build the fabricated Parquet as a list of host buffers — one for the magic header,
        // one per column chunk, and one for the footer + footer length + trailing magic. cuDF
        // logically concatenates these in readParquet(opts, HostMemoryBuffer...).
        List<HostMemoryBuffer> buffers = new ArrayList<>();
        try (ClosingRef<MemoryAllocation> allocation = ClosingRef.own(gpuMemoryContext.allocate(MemoryAmount.offHeap(estimateBuffersSize)))) {
            buffers.add(allocateAndCopy(PARQUET_MAGIC, 0, PARQUET_MAGIC_LENGTH));
            long currentOffset = PARQUET_MAGIC_LENGTH;

            buffers.addAll(readChunks(chunkRanges));

            List<RowGroup> fabricatedRowGroups = new ArrayList<>();
            long totalRowCount = 0;

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

            DynamicSliceOutput footerThrift = new DynamicSliceOutput(originalFooterSize);
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

            long totalBuffersSize = buffers.stream().mapToLong(HostMemoryBuffer::getLength).sum();
            allocation.borrow().update(MemoryAmount.offHeap(totalBuffersSize));
            return new FabricatedParquet(
                    allocation.take(),
                    Optional.of(new Buffers(buffers)),
                    totalRowCount);
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

    private @Own List<HostMemoryBuffer> readChunks(List<DiskRange> chunkRanges)
            throws IOException
    {
        return executeReadPlan(planChunkReads(chunkRanges, options));
    }

    /**
     * Maps the column chunks onto pinned-buffer reads following {@code AbstractParquetDataSource.planChunksRead}:
     * chunks within {@code initialBufferSize} coalesce with their neighbors into a shared read, larger chunks split
     * into ramped sub-ranges read individually. Each read becomes one pinned buffer carved into the fragment slices
     * it covers.
     */
    private static List<CoalescedRead> planChunkReads(List<DiskRange> chunkRanges, ParquetReaderOptions options)
    {
        long initialBytes = options.getInitialBufferSize().toBytes();
        List<DiskRange> smallRanges = new ArrayList<>();
        List<DiskRange> largeRanges = new ArrayList<>();
        for (DiskRange chunkRange : chunkRanges) {
            if (chunkRange.length() <= initialBytes) {
                smallRanges.add(chunkRange);
            }
            else {
                largeRanges.addAll(splitLargeRange(chunkRange, options.getInitialBufferSize(), options.getMaxBufferSize()));
            }
        }

        ImmutableList.Builder<CoalescedRead> reads = ImmutableList.builder();
        if (!smallRanges.isEmpty()) {
            for (DiskRange mergedRange : mergeAdjacentDiskRanges(smallRanges, options.getMaxMergeDistance(), options.getMaxBufferSize())) {
                ImmutableList.Builder<FragmentSlice> slices = ImmutableList.builder();
                for (DiskRange smallRange : smallRanges) {
                    if (mergedRange.contains(smallRange)) {
                        slices.add(new FragmentSlice(smallRange.offset() - mergedRange.offset(), smallRange.length()));
                    }
                }
                reads.add(new CoalescedRead(mergedRange.offset(), toIntExact(mergedRange.length()), slices.build()));
            }
        }
        for (DiskRange largeRange : largeRanges) {
            reads.add(new CoalescedRead(largeRange.offset(), toIntExact(largeRange.length()), ImmutableList.of(new FragmentSlice(0, largeRange.length()))));
        }
        return reads.build();
    }

    /**
     * Reads each planned read into a pinned buffer and carves its fragments as zero-copy slices, returning them in
     * ascending file offset, the order the fabricated metadata lays the column chunks out.
     */
    private @Own List<HostMemoryBuffer> executeReadPlan(List<CoalescedRead> reads)
            throws IOException
    {
        List<CoalescedRead> sequentialReads = reads.stream()
                .sorted(comparingLong(CoalescedRead::offset))
                .collect(toImmutableList());
        List<HostMemoryBuffer> fragments = new ArrayList<>();
        try {
            for (CoalescedRead read : sequentialReads) {
                try (ClosingRef<HostMemoryBuffer> buffer = ClosingRef.own(HostMemoryBuffer.allocate(read.length()));
                        // Non-native sources stage the read through an on-heap array before filling the buffer; account for it pessimistically.
                        MemoryAllocation _ = gpuMemoryContext.allocate(MemoryAmount.heap(read.length()))) {
                    dataSource.readFully(read.offset(), buffer.borrow().asByteBuffer(0, read.length()));
                    for (FragmentSlice slice : read.slices()) {
                        fragments.add(buffer.borrow().slice(slice.bufferOffset(), slice.length()));
                    }
                }
            }
        }
        catch (Throwable t) {
            closeAllSuppress(t, fragments.toArray(HostMemoryBuffer[]::new));
            throw t;
        }
        return ImmutableList.copyOf(fragments);
    }

    private record CoalescedRead(long offset, int length, List<FragmentSlice> slices)
    {
        private CoalescedRead(long offset, int length, List<FragmentSlice> slices)
        {
            this.offset = offset;
            this.length = length;
            this.slices = requireNonNull(slices, "slices is null");
        }
    }

    private record FragmentSlice(long bufferOffset, long length) {}

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
