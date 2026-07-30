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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hive.parquet.GpuParquetConfig;
import io.trino.plugin.hive.parquet.HiveGpuParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.TestHivePageSourceProvider.RowData.rowData;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.ROWTYPE_OF_ROW_AND_PRIMITIVES;
import static io.trino.plugin.hive.TestHiveReaderProjectionsUtil.createProjectedColumnHandle;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHivePageSourceProvider
{
    private static final HiveColumnHandle BASE_COLUMN = createBaseColumn("col", 0, toHiveType(ROWTYPE_OF_ROW_AND_PRIMITIVES), ROWTYPE_OF_ROW_AND_PRIMITIVES, REGULAR, Optional.empty());
    private static final HiveColumnHandle PARTITION_COLUMN = createBaseColumn("partition_col", 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle DATA_COLUMN = createBaseColumn("data_col", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final HiveColumnHandle BUCKET_COLUMN = createBaseColumn("bucket_col", 1, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final String PARTITION_NAME = "part1";
    private static final HiveTablePartitioning HIVE_TABLE_PARTITIONING = new HiveTablePartitioning(
            true,
            BUCKETING_V1,
            10,
            ImmutableList.of(BUCKET_COLUMN),
            false,
            ImmutableList.of(),
            true);
    private static final Domain DATA_DOMAIN = Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 100L, true)), false);
    private static final Domain PARTITION_DOMAIN = Domain.create(ValueSet.of(VARCHAR, utf8Slice("part1")), false);
    private static final HiveTableHandle HIVE_TABLE_HANDLE = new HiveTableHandle(
            "schema",
            "table",
            ImmutableList.of(PARTITION_COLUMN),
            ImmutableList.of(BUCKET_COLUMN, DATA_COLUMN),
            TupleDomain.withColumnDomains(ImmutableMap.of(
                    DATA_COLUMN, DATA_DOMAIN,
                    PARTITION_COLUMN, PARTITION_DOMAIN)),
            TupleDomain.all(),
            Optional.of(HIVE_TABLE_PARTITIONING),
            Optional.empty(),
            Optional.empty(),
            NO_ACID_TRANSACTION);
    private static final HiveSplit HIVE_SPLIT = new HiveSplit(
            PARTITION_NAME,
            "path",
            0,
            100,
            10,
            12,
            new Schema(HiveStorageFormat.PARQUET.getSerde(), false, ImmutableMap.of()),
            ImmutableList.of(new HivePartitionKey(PARTITION_COLUMN.getName(), PARTITION_NAME)),
            ImmutableList.of(),
            Optional.empty(),
            OptionalInt.empty(),
            OptionalInt.of(1),
            false,
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            SplitWeight.standard());
    private HivePageSourceProvider pageSourceProvider;

    @BeforeAll
    public void setup()
    {
        HiveConfig config = new HiveConfig()
                .setDomainCompactionThreshold(2);
        pageSourceProvider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                config,
                new HiveGpuParquetPageSourceFactory(new ParquetReaderConfig(), new GpuParquetConfig()),
                ImmutableSet.of(),
                new MemoryFileSystemFactory());
    }

    @Test
    public void testGetUnenforcedPredicateCompactsData()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 20L), false)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 20L, true)), false))));
    }

    @Test
    public void testGetUnenforcedPredicateConsidersEffectivePredicate()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.all()))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(DATA_COLUMN, DATA_DOMAIN)));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 110L), false)))))
                // data column domain should not be simplified because it contains only 2 values after intersection with effective predicate
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L), false))));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.of(INTEGER, 1L, 10L, 12L), false)))))
                // data column domain should be simplified because it contains 3 values after intersection with effective predicate
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        DATA_COLUMN, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 12L, true)), false))));
    }

    @Test
    public void testGetUnenforcedPredicatePrunesPartitionColumn()
    {
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        PARTITION_COLUMN, Domain.create(ValueSet.of(VARCHAR, utf8Slice("part1")), false)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(DATA_COLUMN, DATA_DOMAIN)));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        PARTITION_COLUMN, Domain.create(ValueSet.of(VARCHAR, utf8Slice("part2")), false)))))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    public void testGetUnenforcedPredicateSkipsBucket()
    {
        Domain bucketDomain = Domain.create(ValueSet.of(INTEGER, 1L), false);
        TupleDomain<ColumnHandle> bucketTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                BUCKET_COLUMN, bucketDomain));
        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                bucketTupleDomain))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        BUCKET_COLUMN, bucketDomain,
                        DATA_COLUMN, DATA_DOMAIN)));

        assertThat(pageSourceProvider.getUnenforcedPredicate(
                SESSION,
                HIVE_SPLIT,
                HIVE_TABLE_HANDLE,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        BUCKET_COLUMN, Domain.create(ValueSet.of(INTEGER, 2L), false)))))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    void testProjectColumnDereferences()
            throws Exception
    {
        List<HiveColumnHandle> columns = ImmutableList.of(
                createProjectedColumnHandle(BASE_COLUMN, ImmutableList.of(0, 0)),
                createProjectedColumnHandle(BASE_COLUMN, ImmutableList.of(0)));

        Page outputPage;
        try (ConnectorPageSource connectorPageSource = projectColumnDereferences(columns, TestHivePageSourceProvider::createPageSource)) {
            outputPage = connectorPageSource
                    .getNextSourcePage()
                    .getPage();
        }
        // Verify output block values
        Block baseInputBlock = createInputPage().getBlock(0);
        for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
            HiveColumnHandle column = columns.get(i);
            verifyBlock(
                    outputPage.getBlock(i),
                    column.getType(),
                    baseInputBlock,
                    BASE_COLUMN.getType(),
                    HivePageSourceProvider.getProjection(column, BASE_COLUMN));
        }
    }

    private static FixedPageSource createPageSource(List<HiveColumnHandle> columns)
    {
        assertThat(columns).containsOnly(BASE_COLUMN);
        return new FixedPageSource(ImmutableList.of(createInputPage()));
    }

    private static Page createInputPage()
    {
        List<Object> inputBlockData = new ArrayList<>();
        inputBlockData.add(rowData(rowData(11L, 12L, 13L), 1L));
        inputBlockData.add(rowData(null, 2L));
        inputBlockData.add(null);
        inputBlockData.add(rowData(rowData(31L, 32L, 33L), 3L));

        return new Page(createInputBlock(inputBlockData, BASE_COLUMN.getType()));
    }

    private static Block createInputBlock(List<Object> data, Type type)
    {
        if (type instanceof RowType rowType) {
            return createRowBlock(data, rowType);
        }
        if (BIGINT.equals(type)) {
            return createLongArrayBlock(data);
        }
        throw new UnsupportedOperationException();
    }

    private static Block createRowBlock(List<Object> data, RowType rowType)
    {
        int positionCount = data.size();

        boolean[] isNull = new boolean[positionCount];
        int fieldCount = rowType.getFields().size();

        List<List<Object>> fieldsData = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fieldsData.add(new ArrayList<>());
        }

        // Extract data to generate fieldBlocks
        for (int position = 0; position < data.size(); position++) {
            RowData row = (RowData) data.get(position);
            if (row == null) {
                isNull[position] = true;
                for (int field = 0; field < fieldCount; field++) {
                    fieldsData.get(field).add(null);
                }
            }
            else {
                for (int field = 0; field < fieldCount; field++) {
                    fieldsData.get(field).add(row.getField(field));
                }
            }
        }

        Block[] fieldBlocks = new Block[fieldCount];
        for (int field = 0; field < fieldCount; field++) {
            fieldBlocks[field] = createInputBlock(fieldsData.get(field), rowType.getFields().get(field).getType());
        }

        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, Optional.of(isNull), fieldBlocks);
    }

    private static Block createLongArrayBlock(List<Object> data)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(data.size());
        for (Object datum : data) {
            if (datum == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, (Long) datum);
            }
        }
        return builder.build();
    }

    private static void verifyBlock(Block actualBlock, Type outputType, Block input, Type inputType, List<Integer> dereferences)
    {
        assertThat(inputType).isInstanceOf(RowType.class);
        Block expectedOutputBlock = createProjectedColumnBlock(input, outputType, (RowType) inputType, dereferences);
        assertBlockEquals(outputType, actualBlock, expectedOutputBlock);
    }

    private static Block createProjectedColumnBlock(Block data, Type finalType, RowType blockType, List<Integer> dereferences)
    {
        if (dereferences.isEmpty()) {
            return data;
        }

        BlockBuilder builder = finalType.createBlockBuilder(null, data.getPositionCount());

        for (int i = 0; i < data.getPositionCount(); i++) {
            RowType sourceType = blockType;

            SqlRow currentData = null;
            boolean isNull = data.isNull(i);

            if (!isNull) {
                // Get SqlRow corresponding to element at position i
                currentData = sourceType.getObject(data, i);
            }

            // Apply all dereferences except for the last one, because the type can be different
            for (int j = 0; j < dereferences.size() - 1; j++) {
                if (isNull) {
                    // If a null element is discovered at any dereferencing step, break
                    break;
                }

                int fieldIndex = dereferences.get(j);
                Block fieldBlock = currentData.getRawFieldBlock(fieldIndex);

                RowType rowType = sourceType;
                int rawIndex = currentData.getRawIndex();
                if (fieldBlock.isNull(rawIndex)) {
                    currentData = null;
                }
                else {
                    sourceType = (RowType) rowType.getFields().get(fieldIndex).getType();
                    currentData = sourceType.getObject(fieldBlock, rawIndex);
                }

                isNull = currentData == null;
            }

            if (isNull) {
                // Append null if any of the elements in the dereference chain were null
                builder.appendNull();
            }
            else {
                int lastDereference = dereferences.getLast();
                Block fieldBlock = currentData.getRawFieldBlock(lastDereference);
                builder.append(fieldBlock.getUnderlyingValueBlock(), fieldBlock.getUnderlyingValuePosition(currentData.getRawIndex()));
            }
        }

        return builder.build();
    }

    static class RowData
    {
        private final List<?> data;

        private RowData(Object... data)
        {
            this.data = Arrays.asList(requireNonNull(data, "data is null"));
        }

        static RowData rowData(Object... data)
        {
            return new RowData(data);
        }

        Object getField(int field)
        {
            checkArgument(field >= 0 && field < data.size());
            return data.get(field);
        }
    }
}
