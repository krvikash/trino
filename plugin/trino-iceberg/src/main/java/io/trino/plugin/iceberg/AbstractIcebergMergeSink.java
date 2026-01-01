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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.DeleteFileSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public abstract class AbstractIcebergMergeSink
        implements ConnectorMergeSink
{
    protected final LocationProvider locationProvider;
    protected final IcebergFileWriterFactory fileWriterFactory;
    protected final TrinoFileSystem fileSystem;
    protected final Map<String, DeleteFileSet> previousDeleteFiles;
    protected final JsonCodec<CommitTaskData> jsonCodec;
    protected final ConnectorSession session;
    protected final IcebergFileFormat fileFormat;
    protected final Map<String, String> storageProperties;
    protected final Schema schema;
    protected final Map<Integer, PartitionSpec> partitionsSpecs;
    protected final ConnectorPageSink insertPageSink;
    protected final Optional<ConnectorPageSink> updateInsertPageSink;
    protected final int columnCount;
    protected final IcebergPageSourceProviderFactory pageSourceProviderFactory;
    protected final List<IcebergColumnHandle> columns;
    protected final Map<String, String> fileIoProperties;
    protected final Map<String, Long> fileCounts;
    protected final Map<String, Long> dataSequenceNumbers;
    protected final Map<String, Long> firstRowIds;
    protected final Optional<String> nameMapping;
    protected final int formatVersion;
    protected final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();
    protected long writtenBytes;

    protected AbstractIcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            Map<String, DeleteFileSet> previousDeleteFiles,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            Optional<ConnectorPageSink> updateInsertPageSink,
            int columnCount,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            List<IcebergColumnHandle> columns,
            Map<String, String> fileIoProperties,
            Map<String, Long> fileCounts,
            Map<String, Long> dataSequenceNumbers,
            Map<String, Long> firstRowIds,
            Optional<String> nameMapping,
            int formatVersion)
    {
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.previousDeleteFiles = ImmutableMap.copyOf(previousDeleteFiles);
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.storageProperties = ImmutableMap.copyOf(storageProperties);
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionsSpecs = ImmutableMap.copyOf(partitionsSpecs);
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.updateInsertPageSink = requireNonNull(updateInsertPageSink, "updateInsertPageSink is null");
        this.columnCount = columnCount;
        this.columns = ImmutableList.copyOf(columns);
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.fileIoProperties = ImmutableMap.copyOf(fileIoProperties);
        this.fileCounts = ImmutableMap.copyOf(fileCounts);
        this.dataSequenceNumbers = ImmutableMap.copyOf(dataSequenceNumbers);
        this.firstRowIds = ImmutableMap.copyOf(firstRowIds);
        this.nameMapping = requireNonNull(nameMapping, "nameMapping is null");
        this.formatVersion = formatVersion;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getDeletionsPage().ifPresent(this::processRemovals);
        mergePage.getInsertionsPage().ifPresent(insertionsPage -> {
            if (formatVersion >= 3) {
                insertPageSink.appendPage(createInsertionsPageWithRowId(insertionsPage, page));
            }
            else {
                insertPageSink.appendPage(insertionsPage);
            }
        });

        writtenBytes = updateInsertPageSink.orElse(insertPageSink).getCompletedBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    void processRemovals(Page removals)
    {
        List<Block> fields = RowBlock.getRowFieldsFromBlock(removals.getBlock(removals.getChannelCount() - 1));
        Block filePathBlock = fields.get(0);
        Block rowPositionBlock = fields.get(1);
        Block partitionSpecIdBlock = fields.get(2);
        Block partitionDataBlock = fields.get(3);
        for (int position = 0; position < filePathBlock.getPositionCount(); position++) {
            Slice filePath = VarcharType.VARCHAR.getSlice(filePathBlock, position);
            long rowPosition = BIGINT.getLong(rowPositionBlock, position);

            int index = position;
            FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, _ -> {
                int partitionSpecId = INTEGER.getInt(partitionSpecIdBlock, index);
                String partitionData = VarcharType.VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                return new FileDeletion(partitionSpecId, partitionData);
            });

            deletion.rowsToDelete().add(rowPosition);
        }
    }

    protected static List<IcebergColumnHandle> withRowLineageColumns(List<IcebergColumnHandle> columns)
    {
        return ImmutableList.<IcebergColumnHandle>builder()
                .addAll(columns)
                .add(IcebergColumnHandle.rowIdColumnHandle())
                .add(IcebergColumnHandle.lastUpdatedSequenceNumberColumnHandle())
                .build();
    }

    Page createInsertionsPageWithRowId(Page insertionsPage, Page inputPage)
    {
        Block[] blocks = new Block[columnCount + 1];
        for (int channel = 0; channel < columnCount; channel++) {
            blocks[channel] = insertionsPage.getBlock(channel);
        }
        blocks[columnCount] = createRowIdBlock(inputPage, columnCount, insertionsPage.getPositionCount());
        return new Page(insertionsPage.getPositionCount(), blocks);
    }

    private static Block createRowIdBlock(Page inputPage, int dataColumnCount, int additionCount)
    {
        // For V3, preserve source_row_id on UPDATE_INSERT rows when it is available.
        // Rows updated from pre-lineage files in upgraded v2->v3 tables legitimately have
        // a null source_row_id, in which case Iceberg assigns a fresh row ID to the new row.
        Block operationBlock = inputPage.getBlock(dataColumnCount);
        Block mergeRowIdBlock = inputPage.getBlock(dataColumnCount + 2);
        List<Block> mergeRowIdFields = RowBlock.getRowFieldsFromBlock(mergeRowIdBlock);
        Block sourceRowIdBlock = mergeRowIdFields.get(4);

        long[] rowIdValues = new long[additionCount];
        boolean[] rowIdNulls = new boolean[additionCount];

        int additionIndex = 0;
        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            byte operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    verify(additionIndex < additionCount, "INSERT row must be selected as an addition");
                    rowIdNulls[additionIndex] = true;
                    additionIndex++;
                }
                case UPDATE_INSERT_OPERATION_NUMBER -> {
                    verify(additionIndex < additionCount, "UPDATE_INSERT row must be selected as an addition");
                    if (sourceRowIdBlock.isNull(position)) {
                        rowIdNulls[additionIndex] = true;
                    }
                    else {
                        rowIdValues[additionIndex] = BIGINT.getLong(sourceRowIdBlock, position);
                        rowIdNulls[additionIndex] = false;
                    }
                    additionIndex++;
                }
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> {
                    // This helper produces source row IDs only for additions (INSERT/UPDATE_INSERT).
                    // DELETE and UPDATE_DELETE rows are consumed by the deletion path, not this additions block.
                }
                case UPDATE_OPERATION_NUMBER -> throw new IllegalArgumentException("UPDATE must be represented as UPDATE_DELETE followed by UPDATE_INSERT in Iceberg");
                default -> throw new IllegalArgumentException("Invalid merge operation: " + operation);
            }
        }
        verify(additionIndex == additionCount, "Additions produced did not match planned additions");

        return new LongArrayBlock(additionCount, Optional.of(rowIdNulls), rowIdValues);
    }

    protected static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final DeletionVector.Builder rowsToDelete = new DeletionVector.Builder();

        public FileDeletion(int partitionSpecId, String partitionDataJson)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        }

        public int partitionSpecId()
        {
            return partitionSpecId;
        }

        public String partitionDataJson()
        {
            return partitionDataJson;
        }

        public DeletionVector.Builder rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
