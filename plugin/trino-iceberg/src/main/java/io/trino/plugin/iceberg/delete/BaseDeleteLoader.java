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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.delete.DeleteManager.DeletePageSourceProvider;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteIndexUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.delete.DeleteFile.fromIceberg;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

/**
 * Copy of {@link org.apache.iceberg.data.BaseDeleteLoader} with Trino native file readers
 */
public class BaseDeleteLoader
        implements DeleteLoader
{
    private static final Logger LOG = Logger.get(BaseDeleteLoader.class);
    private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();

    private final TypeManager typeManager;
    private final DeletePageSourceProvider deletePageSourceProvider;
    private final Function<DeleteFile, InputFile> loadInputFile;
    private final ExecutorService workerPool;

    public BaseDeleteLoader(TypeManager typeManager, DeletePageSourceProvider deletePageSourceProvider, Function<DeleteFile, InputFile> loadInputFile)
    {
        this(typeManager, deletePageSourceProvider, loadInputFile, ThreadPools.getDeleteWorkerPool());
    }

    public BaseDeleteLoader(TypeManager typeManager, DeletePageSourceProvider deletePageSourceProvider, Function<DeleteFile, InputFile> loadInputFile, ExecutorService workerPool)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletePageSourceProvider = requireNonNull(deletePageSourceProvider, "deletePageSourceProvider is null");
        this.loadInputFile = loadInputFile;
        this.workerPool = workerPool;
    }

    /**
     * Checks if the given number of bytes can be cached.
     *
     * <p>Implementations should override this method if they support caching. It is also recommended
     * to use the provided size as a guideline to decide whether the value is eligible for caching.
     * For instance, it may be beneficial to discard values that are too large to optimize the cache
     * performance and utilization.
     */
    protected boolean canCache(long size)
    {
        return false;
    }

    /**
     * Gets the cached value for the key or populates the cache with a new mapping.
     *
     * <p>If the value for the specified key is in the cache, it should be returned. If the value is
     * not in the cache, implementations should compute the value using the provided supplier, cache
     * it, and then return it.
     *
     * <p>This method will be called only if {@link #canCache(long)} returned true.
     */
    protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize)
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not support caching");
    }

    @Override
    public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection)
    {
        Iterable<Iterable<StructLike>> deletes =
                execute(deleteFiles, deleteFile -> getOrReadEqDeletes(deleteFile, projection));
        StructLikeSet deleteSet = StructLikeSet.create(projection.asStruct());
        Iterables.addAll(deleteSet, Iterables.concat(deletes));
        return deleteSet;
    }

    private Iterable<StructLike> getOrReadEqDeletes(DeleteFile deleteFile, Schema projection)
    {
        long estimatedSize = estimateEqDeletesSize(deleteFile, projection);
        if (canCache(estimatedSize)) {
            String cacheKey = deleteFile.location();
            return getOrLoad(cacheKey, () -> readEqDeletes(deleteFile, projection), estimatedSize);
        }
        else {
            return readEqDeletes(deleteFile, projection);
        }
    }

    private Iterable<StructLike> readEqDeletes(DeleteFile deleteFile, Schema projection)
    {
        CloseableIterable<Record> deletes = openDeletes(deleteFile, projection);
        CloseableIterable<Record> copiedDeletes = CloseableIterable.transform(deletes, Record::copy);
        CloseableIterable<StructLike> copiedDeletesAsStructs = toStructs(copiedDeletes, projection);
        return materialize(copiedDeletesAsStructs);
    }

    private CloseableIterable<StructLike> toStructs(
            CloseableIterable<Record> records, Schema schema)
    {
        InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
        return CloseableIterable.transform(records, wrapper::copyFor);
    }

    // materializes the iterable and releases resources so that the result can be cached
    private <T> Iterable<T> materialize(CloseableIterable<T> iterable)
    {
        try (CloseableIterable<T> closeableIterable = iterable) {
            return ImmutableList.copyOf(closeableIterable);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close iterable", e);
        }
    }

    /**
     * Loads the content of a deletion vector or position delete files for a given data file path into
     * a position index.
     *
     * <p>The deletion vector is currently loaded without caching as the existing Puffin reader
     * requires at least 3 requests to fetch the entire file. Caching a single deletion vector may
     * only be useful when multiple data file splits are processed on the same node, which is unlikely
     * as task locality is not guaranteed.
     *
     * <p>For position delete files, however, there is no efficient way to read deletes for a
     * particular data file. Therefore, caching may be more effective as such delete files potentially
     * apply to many data files, especially in unpartitioned tables and tables with deep partitions.
     * If a position delete file qualifies for caching, this method will attempt to cache a position
     * index for each referenced data file.
     *
     * @param deleteFiles a deletion vector or position delete files
     * @param filePath the data file path for which to load deletes
     * @return a position delete index for the provided data file path
     */
    @Override
    public PositionDeleteIndex loadPositionDeletes(
            Iterable<DeleteFile> deleteFiles, CharSequence filePath)
    {
        if (ContentFileUtil.containsSingleDV(deleteFiles)) {
            DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
            validateDV(dv, filePath);
            return readDV(dv);
        }
        else {
            return getOrReadPosDeletes(deleteFiles, filePath);
        }
    }

    private PositionDeleteIndex readDV(DeleteFile dv)
    {
        LOG.debug("Opening DV file %s", dv.location());
        InputFile inputFile = loadInputFile.apply(dv);
        long offset = dv.contentOffset();
        int length = dv.contentSizeInBytes().intValue();
        byte[] bytes = readBytes(inputFile, offset, length);
        return PositionDeleteIndex.deserialize(bytes, dv);
    }

    private PositionDeleteIndex getOrReadPosDeletes(
            Iterable<DeleteFile> deleteFiles, CharSequence filePath)
    {
        Iterable<PositionDeleteIndex> deletes =
                execute(deleteFiles, deleteFile -> getOrReadPosDeletes(deleteFile, filePath));
        return PositionDeleteIndexUtil.merge(deletes);
    }

    @SuppressWarnings("CollectionUndefinedEquality")
    private PositionDeleteIndex getOrReadPosDeletes(DeleteFile deleteFile, CharSequence filePath)
    {
        long estimatedSize = estimatePosDeletesSize(deleteFile);
        if (canCache(estimatedSize)) {
            String cacheKey = deleteFile.location();
            CharSequenceMap<PositionDeleteIndex> indexes =
                    getOrLoad(cacheKey, () -> readPosDeletes(deleteFile), estimatedSize);
            return indexes.getOrDefault(filePath, PositionDeleteIndex.empty());
        }
        else {
            return readPosDeletes(deleteFile, filePath);
        }
    }

    private CharSequenceMap<PositionDeleteIndex> readPosDeletes(DeleteFile deleteFile)
    {
        CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA);
        return Deletes.toPositionIndexes(deletes, deleteFile);
    }

    private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath)
    {
        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        TupleDomain<IcebergColumnHandle> filter = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, utf8Slice(filePath.toString()))));
        CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA, filter);
        return Deletes.toPositionIndex(filePath, deletes, deleteFile);
    }

    private CloseableIterable<Record> openDeletes(DeleteFile deleteFile, Schema projection)
    {
        return openDeletes(deleteFile, projection, TupleDomain.all());
    }

    private CloseableIterable<Record> openDeletes(DeleteFile deleteFile, Schema projection, TupleDomain<IcebergColumnHandle> filter)
    {
        LOG.debug("Opening delete file %s", deleteFile.location());

        try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(fromIceberg(deleteFile), getProjectedColumns(projection, typeManager), filter)) {
            return CloseableIterable.of(loadPositionDeletes(pageSource, projection));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open delete file: " + deleteFile.location(), e);
        }
    }

    private static List<Record> loadPositionDeletes(ConnectorPageSource pageSource, Schema schema)
    {
        ImmutableList.Builder<org.apache.iceberg.data.Record> deletedRows = ImmutableList.builder();
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block posBlock = page.getBlock(1);

            for (int position = 0; position < page.getPositionCount(); position++) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField(DELETE_FILE_PATH.name(), VARCHAR.getSlice(pathBlock, position).toStringUtf8());
                record.setField(DELETE_FILE_POS.name(), BIGINT.getLong(posBlock, position));
                deletedRows.add(record);
            }
        }
        return deletedRows.build();
    }

    private <I, O> Iterable<O> execute(Iterable<I> objects, Function<I, O> func)
    {
        Queue<O> output = new ConcurrentLinkedQueue<>();

        Tasks.foreach(objects)
                .executeWith(workerPool)
                .stopOnFailure()
                .onFailure((object, exc) -> LOG.error(exc, "Failed to process %s", object))
                .run(object -> output.add(func.apply(object)));

        return output;
    }

    // estimates the memory required to cache position deletes (in bytes)
    private long estimatePosDeletesSize(DeleteFile deleteFile)
    {
        // the space consumption highly depends on the nature of deleted positions (sparse vs compact)
        // testing shows Roaring bitmaps require around 8 bits (1 byte) per value on average
        return deleteFile.recordCount();
    }

    // estimates the memory required to cache equality deletes (in bytes)
    private long estimateEqDeletesSize(DeleteFile deleteFile, Schema projection)
    {
        try {
            long recordCount = deleteFile.recordCount();
            int recordSize = estimateRecordSize(projection);
            return Math.multiplyExact(recordCount, recordSize);
        }
        catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private int estimateRecordSize(Schema schema)
    {
        return schema.columns().stream().mapToInt(TypeUtil::estimateSize).sum();
    }

    private void validateDV(DeleteFile dv, CharSequence filePath)
    {
        checkArgument(
                dv.contentOffset() != null,
                "Invalid DV, offset cannot be null: %s",
                ContentFileUtil.dvDesc(dv));
        checkArgument(
                dv.contentSizeInBytes() != null,
                "Invalid DV, length is null: %s",
                ContentFileUtil.dvDesc(dv));
        checkArgument(
                dv.contentSizeInBytes() <= Integer.MAX_VALUE,
                "Can't read DV larger than 2GB: %s",
                dv.contentSizeInBytes());
        checkArgument(
                filePath.toString().equals(dv.referencedDataFile()),
                "DV is expected to reference %s, not %s",
                filePath,
                dv.referencedDataFile());
    }

    private static byte[] readBytes(InputFile inputFile, long offset, int length)
    {
        try (SeekableInputStream stream = inputFile.newStream()) {
            byte[] bytes = new byte[length];

            if (stream instanceof RangeReadable rangeReadable) {
                rangeReadable.readFully(offset, bytes);
            }
            else {
                stream.seek(offset);
                ByteStreams.readFully(stream, bytes);
            }

            return bytes;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
