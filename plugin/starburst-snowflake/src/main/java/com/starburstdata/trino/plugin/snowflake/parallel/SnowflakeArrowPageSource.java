/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.starburstdata.trino.plugin.snowflake.parallel.writer.BlockWriter;
import com.starburstdata.trino.plugin.snowflake.parallel.writer.BlockWriterFactory;
import com.starburstdata.trino.plugin.snowflake.parallel.writer.ConverterFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.MergeJdbcPageSource;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.FieldVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeColumns.getPrimaryKeys;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeColumns.getScanColumns;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeParallelSessionProperties.getQuotedIdentifiersIgnoreCase;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcPageSourceProvider.buildMergeIdColumnAdaptation;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.UnaryOperator.identity;

public class SnowflakeArrowPageSource
        implements ConnectorPageSource
{
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator();
    // Rows appended between PageBuilder.isFull() checks. Matches the ORC/Parquet reader batch size so the
    // per-slice writer dispatch cost stays low; the page is still size-bounded by DEFAULT_MAX_PAGE_SIZE_IN_BYTES.
    private static final int MAX_ROWS_PER_PAGE_SLICE = 8192;
    private final BufferAllocator bufferAllocator;
    private final boolean quotedIdentifiersIgnoreCase;
    private final PageBuilder pageBuilder;
    private final List<JdbcColumnHandle> columns;
    private final StarburstDataConversionContext conversionContext;
    private final ChunkFetcher fetcher;
    private final long splitRetainedSize;
    private final List<MergeJdbcPageSource.ColumnAdaptation> columnAdaptations;
    private long completedBytes;
    private CompletableFuture<byte[]> chunkFuture;
    private boolean finished;

    // Decoding state for the chunk currently being emitted as bounded pages. A chunk is decoded into
    // one CloseableArrowBatch (a list of Arrow record batches) that is consumed across several
    // getNextSourcePage() calls, one <= 1 MB page at a time, so a single ~160 MB chunk never
    // materializes as one giant page/block.
    private CloseableArrowBatch currentBatch;
    private List<BlockWriter> currentRecordBatchWriters;
    private int currentRecordBatchIndex;
    private int currentRecordBatchRowCount;
    private int positionInRecordBatch;

    public SnowflakeArrowPageSource(
            ConnectorSession session,
            JdbcClient jdbcClient,
            JdbcTableHandle table,
            SnowflakeArrowSplit split,
            List<JdbcColumnHandle> columns,
            StarburstResultStreamProvider streamProvider)
    {
        this.splitRetainedSize = requireNonNull(split, "split is null").getRetainedSizeInBytes();

        Optional<JdbcColumnHandle> mergeRowId = columns.stream()
                .filter(column -> column.getColumnName().equals(MERGE_ROW_ID))
                .collect(toOptional());
        if (mergeRowId.isEmpty()) {
            columnAdaptations = ImmutableList.of();
            this.columns = ImmutableList.copyOf(columns);
        }
        else {
            List<JdbcColumnHandle> primaryKeys = getPrimaryKeys(session, jdbcClient, table);
            checkArgument(!primaryKeys.isEmpty(), "Primary keys must be defined for table %s", table.getRequiredNamedRelation().getRemoteTableName());

            List<JdbcColumnHandle> scanColumns = getScanColumns(columns, () -> primaryKeys);
            ImmutableList.Builder<MergeJdbcPageSource.ColumnAdaptation> columnAdaptationsBuilder = ImmutableList.builderWithExpectedSize(columns.size());
            for (JdbcColumnHandle columnHandle : columns) {
                if (columnHandle.equals(mergeRowId.get())) {
                    columnAdaptationsBuilder.add(buildMergeIdColumnAdaptation(scanColumns, primaryKeys));
                }
                else {
                    columnAdaptationsBuilder.add(new MergeJdbcPageSource.SourceColumn(scanColumns.indexOf(columnHandle)));
                }
            }
            this.columnAdaptations = columnAdaptationsBuilder.build();
            this.columns = scanColumns;
        }

        this.quotedIdentifiersIgnoreCase = getQuotedIdentifiersIgnoreCase(requireNonNull(session, "session is null"));

        this.pageBuilder = new PageBuilder(this.columns.stream()
                .map(JdbcColumnHandle::getColumnType)
                .collect(toImmutableList()));

        this.bufferAllocator = ROOT_ALLOCATOR.newChildAllocator(
                "snowflakeArrowSplit" + split.hashCode(),
                // Allocator is used sequentially, largest chunk is what it will need to hold at most at the same time
                split.getLargestChunkUncompressedBytes(),
                Long.MAX_VALUE);

        int[] decimalColumnScales = this.columns.stream()
                .map(column -> column.getJdbcTypeHandle().decimalDigits()
                        .orElse(0))
                .mapToInt(Integer::intValue)
                .toArray();

        this.conversionContext = new StarburstDataConversionContext(
                split.snowflakeSessionParameters(),
                decimalColumnScales,
                split.resultVersion());

        this.fetcher = new ChunkFetcher(requireNonNull(streamProvider, "streamProvider is null"), split.chunks());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return fetcher.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        // While a decoded chunk is still being emitted we can make progress without waiting.
        if (currentBatch != null) {
            return NOT_BLOCKED;
        }
        return requireNonNullElse(chunkFuture, NOT_BLOCKED);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Page page = doGetNextPage();
        if (page == null) {
            return null;
        }

        SourcePage sourcePage = SourcePage.create(page);
        if (columnAdaptations.isEmpty()) {
            return sourcePage;
        }

        return getColumnAdaptationsPage(sourcePage);
    }

    private SourcePage getColumnAdaptationsPage(SourcePage page)
    {
        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).getBlock(page);
        }

        return SourcePage.create(new Page(page.getPositionCount(), blocks));
    }

    private Page doGetNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");

        if (finished) {
            return null;
        }

        // getNextPage is not called concurrently hence there is no need for synchronization here
        if (currentBatch == null) {
            if (chunkFuture == null) {
                chunkFuture = fetcher.fetchNextChunk();
                if (chunkFuture == null) {
                    // No chunks left to fetch and the previous chunk has been fully consumed.
                    finished = true;
                    return null;
                }
                // Let the engine wait on the fetch via isBlocked() before we attempt to decode it.
                return null;
            }

            byte[] chunk;
            try {
                chunk = chunkFuture.join();
            }
            catch (CompletionException e) {
                throw new TrinoException(JDBC_ERROR, "Failed fetching Arrow chunk", e);
            }
            currentBatch = decodeChunk(chunk);
            currentRecordBatchIndex = 0;
            currentRecordBatchRowCount = 0;
            positionInRecordBatch = 0;
            currentRecordBatchWriters = null;
            // Prefetch the next chunk while this one is emitted as bounded pages.
            chunkFuture = fetcher.fetchNextChunk();
        }

        Page page = buildBoundedPage();
        completedBytes += page.getSizeInBytes();
        return page;
    }

    /**
     * Emits at most one ~{@link io.trino.spi.block.PageBuilderStatus#DEFAULT_MAX_PAGE_SIZE_IN_BYTES}
     * page from {@link #currentBatch}, resuming where the previous call left off. The chunk's Arrow
     * record batches are consumed in slices of {@link #MAX_ROWS_PER_PAGE_SLICE} rows so that a single
     * large record batch does not produce one oversized block.
     */
    private Page buildBoundedPage()
    {
        List<List<ValueVector>> recordBatches = currentBatch.batch();
        try {
            while (!pageBuilder.isFull() && currentRecordBatchIndex < recordBatches.size()) {
                if (currentRecordBatchWriters == null) {
                    List<ValueVector> vectors = recordBatches.get(currentRecordBatchIndex);
                    checkState(!vectors.isEmpty(), "There must be at least one vector in the batch of vectors");
                    currentRecordBatchRowCount = vectors.getFirst().getValueCount();
                    currentRecordBatchWriters = createWriters(vectors);
                    positionInRecordBatch = 0;
                }

                int sliceLength = Math.min(currentRecordBatchRowCount - positionInRecordBatch, MAX_ROWS_PER_PAGE_SLICE);
                if (sliceLength > 0) {
                    pageBuilder.declarePositions(sliceLength);
                    for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                        currentRecordBatchWriters.get(columnIndex).write(pageBuilder.getBlockBuilder(columnIndex), positionInRecordBatch, sliceLength);
                    }
                    positionInRecordBatch += sliceLength;
                }

                if (positionInRecordBatch >= currentRecordBatchRowCount) {
                    // Advance past the exhausted record batch in this same call, so a page that fills exactly
                    // at a batch boundary does not leave an exhausted batch that yields an empty page next call.
                    currentRecordBatchIndex++;
                    currentRecordBatchWriters = null;
                }
            }
        }
        catch (SFException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't write Snowflake blocks", e);
        }

        if (currentRecordBatchIndex >= recordBatches.size()) {
            // The whole chunk has been consumed; release its Arrow buffers.
            currentBatch.close();
            currentBatch = null;
            currentRecordBatchWriters = null;
            currentRecordBatchIndex = 0;
            currentRecordBatchRowCount = 0;
            positionInRecordBatch = 0;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private CloseableArrowBatch decodeChunk(byte[] chunk)
    {
        try {
            return decodeArrowInputStream(chunk);
        }
        catch (IOException e) {
            throw new TrinoException(JDBC_ERROR, "Failed reading Arrow stream", e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return bufferAllocator.getAllocatedMemory()
                + splitRetainedSize
                + pageBuilder.getRetainedSizeInBytes()
                + fetcher.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            // Registered resources are closed in reverse order, so each close() runs even if an earlier one throws.
            closer.register(bufferAllocator::close);
            closer.register(fetcher::close);
            // The Arrow batch is closed first, before the allocator, otherwise the allocator close would fail on the still-open vectors.
            if (currentBatch != null) {
                closer.register(currentBatch::close);
                currentBatch = null;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<BlockWriter> createWriters(List<ValueVector> vectors)
    {
        Map<Integer, Integer> columnToVectorOrder = buildColumnOrder(vectors);
        ImmutableList.Builder<BlockWriter> writers = ImmutableList.builderWithExpectedSize(columns.size());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ValueVector vector = vectors.get(columnToVectorOrder.get(columnIndex));
            ArrowVectorConverter converter = ConverterFactory.createSnowflakeConverter(vector, columnIndex, conversionContext);
            writers.add(BlockWriterFactory.createWriter(columns.get(columnIndex), converter));
        }
        return writers.build();
    }

    private Map<Integer, Integer> buildColumnOrder(List<ValueVector> vectors)
    {
        Map<String, Integer> vectorIndexes = IntStream.range(0, vectors.size())
                .boxed()
                // In case of a collision, e.g. MyVector and Myvector, the ImmutableMap will throw and such tables can't be queried
                // with Trino until https://github.com/trinodb/trino/issues/17
                .collect(toImmutableMap(i -> vectors.get(i).getField().getName(), identity()));

        ImmutableMap.Builder<Integer, Integer> columnToVectorOrder = ImmutableMap.builderWithExpectedSize(columns.size());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            String columnName = columns.get(columnIndex).getColumnName();
            Integer vectorIndex = vectorIndexes.get(quotedIdentifiersIgnoreCase ? columnName.toUpperCase(ENGLISH) : columnName);
            if (vectorIndex == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot find corresponding vector for column %s. Trino columns: %s, vectors: %s".formatted(
                        columnName,
                        columns,
                        vectorIndexes));
            }
            columnToVectorOrder.put(columnIndex, vectorIndex);
        }

        return columnToVectorOrder.buildOrThrow();
    }

    private CloseableArrowBatch decodeArrowInputStream(byte[] data)
            throws IOException
    {
        // Decompress lazily and stream straight into Arrow, so the uncompressed chunk is never materialized as a single byte[].
        try (InputStream input = StarburstResultStreamProvider.decompress(data);
                ArrowStreamReader reader = new ArrowStreamReader(input, bufferAllocator);
                VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
            ImmutableList.Builder<List<ValueVector>> batchBuilder = ImmutableList.builder();
            while (reader.loadNextBatch()) {
                ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builderWithExpectedSize(vectorSchemaRoot.getFieldVectors().size());
                for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
                    // transfer will not copy data but transfer ownership of memory, otherwise values will be gone
                    // once reader is gone
                    TransferPair transferPair = fieldVector.getTransferPair(bufferAllocator);
                    transferPair.transfer();
                    vectorBuilder.add(transferPair.getTo());
                }
                batchBuilder.add(vectorBuilder.build());
                vectorSchemaRoot.clear();
            }
            return new CloseableArrowBatch(batchBuilder.build());
        }
    }

    @SuppressWarnings("UnusedVariable") // error-prone false positive
    private record CloseableArrowBatch(List<List<ValueVector>> batch)
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            for (List<ValueVector> vectors : batch) {
                vectors.forEach(ValueVector::close);
            }
        }
    }
}
