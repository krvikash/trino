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
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.FieldVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
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
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeParallelSessionProperties.getQuotedIdentifiersIgnoreCase;
import static com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeSplitManager.getScanColumns;
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
            List<JdbcColumnHandle> primaryKeys = jdbcClient.getPrimaryKeys(session, table.getRequiredNamedRelation().getRemoteTableName());
            checkArgument(!primaryKeys.isEmpty(), "Primary keys must be defined for table %s", table.getRequiredNamedRelation().getRemoteTableName());

            List<JdbcColumnHandle> scanColumns = getScanColumns(columns, primaryKeys);
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
        return requireNonNullElse(chunkFuture, NOT_BLOCKED);
    }

    @Override
    public Page getNextPage()
    {
        Page page = doGetNextPage();
        if (page == null || columnAdaptations.isEmpty()) {
            return page;
        }

        return getColumnAdaptationsPage(SourcePage.create(page));
    }

    private Page getColumnAdaptationsPage(SourcePage page)
    {
        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).getBlock(page);
        }

        return new Page(page.getPositionCount(), blocks);
    }

    private Page doGetNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");

        if (finished) {
            return null;
        }

        // getNextPage is not called concurrently hence there is no need for synchronization here
        if (chunkFuture == null) {
            chunkFuture = fetcher.fetchNextChunk();
            return null;
        }

        try {
            processChunk(chunkFuture.join());
        }
        catch (CompletionException e) {
            throw new TrinoException(JDBC_ERROR, "Failed fetching Arrow chunk", e);
        }

        // fetcher might be 'done', but page source is not 'finished' until fetched result is consumed
        finished = fetcher.isDone();
        if (!finished) {
            chunkFuture = fetcher.fetchNextChunk();
        }
        Page page = pageBuilder.build();
        // A single split maps to a multiple chunk files,
        // each holding up to a certain amount of records (from a few hundred up to a few million)
        pageBuilder.reset();
        completedBytes += page.getSizeInBytes();

        return page;
    }

    private void processChunk(byte[] chunk)
    {
        try (CloseableArrowBatch batch = decodeArrowInputStream(chunk)) {
            for (List<ValueVector> vectors : batch.batch()) {
                int columnCount = columns.size();
                checkState(!vectors.isEmpty(), "There must be at least one vector in the batch of vectors");
                pageBuilder.declarePositions(vectors.get(0).getValueCount());
                Map<Integer, Integer> columnToVectorOrder = buildColumnOrder(vectors);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    BlockWriter writer = createWriter(vectors.get(columnToVectorOrder.get(columnIndex)), columnIndex);
                    writer.write(pageBuilder.getBlockBuilder(columnIndex));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(JDBC_ERROR, "Failed reading Arrow stream", e);
        }
        catch (SFException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't write Snowflake blocks", e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return bufferAllocator.getAllocatedMemory() + splitRetainedSize + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        fetcher.close();
        bufferAllocator.close();
    }

    private BlockWriter createWriter(ValueVector vector, int columnIndex)
    {
        ArrowVectorConverter converter = ConverterFactory.createSnowflakeConverter(vector, columnIndex, conversionContext);
        JdbcColumnHandle columnHandle = columns.get(columnIndex);
        int rowCount = vector.getValueCount();
        return BlockWriterFactory.createWriter(columnHandle, converter, rowCount);
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
        try (ArrowStreamReader reader = new ArrowStreamReader(wrap(data), bufferAllocator); VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
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

    private SeekableByteChannel wrap(byte[] data)
    {
        return new ByteArrayReadableSeekableByteChannel(data);
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
