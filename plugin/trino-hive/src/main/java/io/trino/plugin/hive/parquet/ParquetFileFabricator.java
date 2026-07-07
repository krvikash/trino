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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.MessageTypeConverter;
import io.trino.parquet.writer.ParquetTypeConverter;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.TrinoException;
import io.trino.spi.gpu.ConnectorGpuMemoryContext;
import io.trino.spi.gpu.IoExecutor;
import io.trino.spi.gpu.MemoryAllocation;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.parquet.AbstractParquetDataSource.mergeAdjacentDiskRanges;
import static io.trino.parquet.AbstractParquetDataSource.splitLargeRange;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Fabricates optimized mini-Parquet files in memory by extracting only
 * the needed columns and row groups from the original Parquet file.
 */
public class ParquetFileFabricator
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

    private final TrinoInputFile inputFile;
    private final List<RowGroupInfo> filteredRowGroups;
    private final MessageType requestedSchema;
    private final ConnectorGpuMemoryContext gpuMemoryContext;
    private final ParquetReaderOptions options;
    private final ParquetMetadata parquetMetadata;
    private final IoExecutor ioExecutor;

    private final AtomicLong completedBytes = new AtomicLong();
    private final AtomicLong readStartNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong readEndNanos = new AtomicLong(Long.MIN_VALUE);

    public ParquetFileFabricator(
            TrinoInputFile inputFile,
            List<RowGroupInfo> filteredRowGroups,
            MessageType requestedSchema,
            ConnectorGpuMemoryContext gpuMemoryContext,
            ParquetReaderOptions options,
            ParquetMetadata parquetMetadata,
            IoExecutor ioExecutor)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.filteredRowGroups = ImmutableList.copyOf(requireNonNull(filteredRowGroups, "filteredRowGroups is null"));
        this.requestedSchema = requireNonNull(requestedSchema, "requestedSchema is null");
        this.gpuMemoryContext = requireNonNull(gpuMemoryContext, "gpuMemoryContext is null");
        this.options = requireNonNull(options, "options is null");
        this.parquetMetadata = requireNonNull(parquetMetadata, "parquetMetadata is null");
        this.ioExecutor = requireNonNull(ioExecutor, "ioExecutor is null");
    }

    public long getCompletedBytes()
    {
        return completedBytes.get();
    }

    // Wall-clock span from submitting the reads to the last one completing.
    public long getReadTimeNanos()
    {
        long end = readEndNanos.get();
        if (end == Long.MIN_VALUE) {
            return 0;
        }
        return end - readStartNanos.get();
    }

    public @Move FabricatedParquet fabricate()
    {
        if (filteredRowGroups.isEmpty()) {
            return new FabricatedParquet(gpuMemoryContext.allocate(MemoryAmount.ZERO), Optional.empty(), 0);
        }
        try {
            return writeFabricatedFile(filteredRowGroups, requestedSchema, parquetMetadata.getFileMetaData());
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, format("Error fabricating Parquet file from %s: %s", inputFile.location(), requireNonNullElse(e.getMessage(), e)), e);
        }
    }

    /**
     * One {@link io.trino.parquet.DiskRange} of a column chunk whose bytes become a single host
     * buffer in the fabricated file. A column chunk maps to one fragment when it fits within
     * {@code initialBufferSize}, or to several when it is split into ramped sub-ranges.
     *
     * <p>A producer thread {@link #complete}s or {@link #fail}s the fragment; a consumer thread
     * {@link #await}s it. The {@code buffer} future is the only mutable state and provides the
     * happens-before between them.
     */
    private static final class ChunkFragment
    {
        private final DiskRange range;
        private final CompletableFuture<ClosingRef<HostMemoryBuffer>> buffer = new CompletableFuture<>();

        ChunkFragment(DiskRange range)
        {
            this.range = requireNonNull(range, "range is null");
        }

        DiskRange range()
        {
            return range;
        }

        void complete(@Move ClosingRef<HostMemoryBuffer> completed)
        {
            requireNonNull(completed, "completed is null");
            if (!buffer.complete(completed)) {
                completed.close();
            }
        }

        void fail(Throwable cause)
        {
            requireNonNull(cause, "cause is null");
            buffer.completeExceptionally(cause);
        }

        @Move
        HostMemoryBuffer await()
                throws IOException
        {
            return getFutureValue(buffer, IOException.class).take();
        }

        void closeIfPending()
        {
            if (buffer.cancel(true) || buffer.isCompletedExceptionally()) {
                return;
            }
            // The future is already complete: either the consumer has taken the buffer
            // (ClosingRef.close is a no-op after take()) or it was abandoned and we close now.
            ClosingRef<HostMemoryBuffer> ref = buffer.getNow(null);
            if (ref != null) {
                ref.close();
            }
        }
    }

    // One read of range, filling the ChunkFragments within it.
    private record CoalescedRead(DiskRange range, List<ChunkFragment> fragments) {}

    // chunkFragments indexed by column chunk in (row-group, column) order; coalescedReads to submit.
    private record ReadPlan(List<List<ChunkFragment>> chunkFragments, List<CoalescedRead> coalescedReads) {}

    private @Move FabricatedParquet writeFabricatedFile(
            List<RowGroupInfo> rowGroups,
            MessageType clippedSchema,
            FileMetadata originalFileMetadata)
            throws IOException
    {
        int originalFooterSize = parquetMetadata.getCompleteFooterSize().orElseThrow(() -> new IllegalStateException("Complete original footer size unknown"));

        long estimateBuffersSize = PARQUET_MAGIC_LENGTH;
        for (RowGroupInfo rowGroupInfo : rowGroups) {
            for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                estimateBuffersSize += toIntExact(column.getTotalSize());
            }
        }
        estimateBuffersSize += originalFooterSize + FOOTER_LENGTH_SIZE + PARQUET_MAGIC_LENGTH;

        // Build the fabricated Parquet as a list of host buffers — one for the magic header,
        // one per column chunk, and one for the footer + footer length + trailing magic. cuDF
        // logically concatenates these in readParquet(opts, HostMemoryBuffer...).
        List<HostMemoryBuffer> buffers = new ArrayList<>();
        ReadPlan readPlan = planReads(rowGroups);
        List<List<ChunkFragment>> chunkFragments = readPlan.chunkFragments();
        submitReads(readPlan);
        try (ClosingRef<MemoryAllocation> allocation = ClosingRef.own(gpuMemoryContext.allocate(MemoryAmount.offHeap(estimateBuffersSize)))) {
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
                    long chunkOffset = column.getStartingPos();
                    long chunkSize = column.getTotalSize();

                    // A column chunk may be split across multiple fragments when it exceeds maxBufferSize;
                    // appending them in order reconstitutes the chunk's bytes in the fabricated buffer list.
                    for (ChunkFragment fragment : chunkFragments.get(chunkIndex)) {
                        buffers.add(fragment.await());
                    }
                    chunkIndex++;

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

            verify(chunkIndex == chunkFragments.size(), "Planned %s chunk fragment groups but assembled %s", chunkFragments.size(), chunkIndex);

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
            // closeIfPending on each fragment is independent; a throw from one must not leak the rest.
            for (List<ChunkFragment> fragments : chunkFragments) {
                for (ChunkFragment fragment : fragments) {
                    try {
                        fragment.closeIfPending();
                    }
                    catch (Throwable closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                }
            }
            closeAllSuppress(t, buffers.toArray(HostMemoryBuffer[]::new));
            throw t;
        }
    }

    /**
     * Plans the coalesced reads for {@code rowGroups}. Each column chunk maps to one fragment when
     * within {@code initialBufferSize}, or several when split into ramped sub-ranges by
     * {@link io.trino.parquet.AbstractParquetDataSource#splitLargeRange}.
     */
    private ReadPlan planReads(List<RowGroupInfo> rowGroups)
    {
        // Split by size like AbstractParquetDataSource.planChunksRead.
        // Small chunks (<= initialBufferSize) merge with neighbors; large
        // chunks split into sub-ranges and reassemble in order at the consumer.
        long initialBytes = options.getInitialBufferSize().toBytes();
        ImmutableList.Builder<List<ChunkFragment>> perChunkFragments = ImmutableList.builder();
        ImmutableList.Builder<ChunkFragment> smallFragmentsBuilder = ImmutableList.builder();
        ImmutableList.Builder<DiskRange> smallRangesBuilder = ImmutableList.builder();
        ImmutableList.Builder<ChunkFragment> largeFragmentsBuilder = ImmutableList.builder();
        for (RowGroupInfo rowGroupInfo : rowGroups) {
            for (ColumnChunkMetadata column : rowGroupInfo.prunedBlockMetadata().getColumns()) {
                DiskRange chunkRange = new DiskRange(column.getStartingPos(), column.getTotalSize());
                ImmutableList.Builder<ChunkFragment> fragments = ImmutableList.builder();
                if (chunkRange.length() <= initialBytes) {
                    ChunkFragment fragment = new ChunkFragment(chunkRange);
                    fragments.add(fragment);
                    smallFragmentsBuilder.add(fragment);
                    smallRangesBuilder.add(chunkRange);
                }
                else {
                    for (DiskRange subRange : splitLargeRange(chunkRange, options.getInitialBufferSize(), options.getMaxBufferSize())) {
                        ChunkFragment fragment = new ChunkFragment(subRange);
                        fragments.add(fragment);
                        largeFragmentsBuilder.add(fragment);
                    }
                }
                perChunkFragments.add(fragments.build());
            }
        }
        List<List<ChunkFragment>> chunkFragments = perChunkFragments.build();
        List<ChunkFragment> smallFragments = smallFragmentsBuilder.build();
        List<DiskRange> smallRanges = smallRangesBuilder.build();
        List<ChunkFragment> largeFragments = largeFragmentsBuilder.build();

        ImmutableList.Builder<CoalescedRead> coalescedReads = ImmutableList.builder();
        for (DiskRange coalescedRange : mergeAdjacentDiskRanges(smallRanges, options.getMaxMergeDistance(), options.getMaxBufferSize())) {
            ImmutableList.Builder<ChunkFragment> contained = ImmutableList.builder();
            for (int i = 0; i < smallFragments.size(); i++) {
                if (coalescedRange.contains(smallRanges.get(i))) {
                    contained.add(smallFragments.get(i));
                }
            }
            coalescedReads.add(new CoalescedRead(coalescedRange, contained.build()));
        }
        for (ChunkFragment fragment : largeFragments) {
            coalescedReads.add(new CoalescedRead(fragment.range(), ImmutableList.of(fragment)));
        }
        return new ReadPlan(chunkFragments, coalescedReads.build());
    }

    // Submits one read task per coalesced range to the shared executor.
    private void submitReads(ReadPlan readPlan)
    {
        // Cooperative abort: the first read to fail records its cause; later reads observe it and
        // skip issuing further reads. We do not retain the task futures to cancel them because each
        // read is short and interrupting a TrinoInput mid-read is not worth the added complexity.
        readStartNanos.set(System.nanoTime());
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        for (CoalescedRead read : readPlan.coalescedReads()) {
            ioExecutor.submit(() -> {
                readCoalescedRange(read.range(), read.fragments(), firstFailure);
                return null;
            }).whenComplete((_, throwable) -> {
                if (throwable != null) {
                    // Fail fragments if the read submission is rejected (e.g. executor shutting down) so their consumers observe the failure.
                    failFragments(read.fragments(), throwable);
                }
            });
        }
    }

    private void readCoalescedRange(DiskRange range, List<ChunkFragment> fragments, AtomicReference<Throwable> firstFailure)
    {
        Throwable existing = firstFailure.get();
        if (existing != null) {
            // Fail each fragment with its own exception so a single throwable is not thrown across threads.
            failFragments(fragments, new IOException("Aborted: a concurrent column-chunk read failed", existing));
            return;
        }
        int length = toIntExact(range.length());
        try (ClosingRef<HostMemoryBuffer> coalesced = ClosingRef.own(HostMemoryBuffer.allocate(length));
                // Non-native sources stage the read through an on-heap array before filling the buffer; account for it pessimistically.
                MemoryAllocation _ = gpuMemoryContext.allocate(MemoryAmount.heap(length));
                TrinoInput input = inputFile.newInput()) {
            input.readFully(range.offset(), coalesced.borrow().asByteBuffer(0, length));
            completedBytes.addAndGet(length);
            // Set before completing fragments so the read end is visible to the consumer that awaits them.
            readEndNanos.accumulateAndGet(System.nanoTime(), Math::max);
            // Each fragment is a zero-copy slice of the coalesced buffer; slicing bumps the shared
            // refcount so the slices outlive coalesced's close at the end of this block.
            for (ChunkFragment fragment : fragments) {
                DiskRange fragmentRange = fragment.range();
                int offsetInCoalesced = toIntExact(fragmentRange.offset() - range.offset());
                fragment.complete(ClosingRef.own(coalesced.borrow().slice(offsetInCoalesced, fragmentRange.length())));
            }
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException || t instanceof InterruptedIOException) {
                Thread.currentThread().interrupt();
            }
            firstFailure.compareAndSet(null, t);
            failFragments(fragments, t);
            throwIfInstanceOf(t, Error.class);
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private static void failFragments(List<ChunkFragment> fragments, Throwable cause)
    {
        for (ChunkFragment fragment : fragments) {
            fragment.fail(cause);
        }
    }

    private static @Move HostMemoryBuffer allocateAndCopy(byte[] source, int sourceOffset, int length)
    {
        try (ClosingRef<HostMemoryBuffer> buffer = ClosingRef.own(HostMemoryBuffer.allocate(length))) {
            buffer.borrow().setBytes(0, source, sourceOffset, length);
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
}
