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
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.execution.buffer.PagesSerdeStreamFactory;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.plugin.hive.util.SortTempFileFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.PageStreamWriter;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.PART_SUPPLIER;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSortTempFileFactory
{
    @Test
    void testWrittenBytes()
            throws Exception
    {
        // Sanity/regression test to ensure the two temp file implementations (PagesSerde vs TempFileWriter)
        // report similar written bytes for the same data. This is important because SortingFileWriter uses
        // getWrittenBytes() to report progress to the layer above, which decides when to split files.
        // If the two implementations diverge significantly, file splitting will be inconsistent depending
        // on which implementation is used.
        //
        // The expected ratios are snapshots from the current point in time and represent the expected
        // ratio of optimizedBytes / orcBytes. They vary by table because different column types and
        // data patterns compress differently.
        testWrittenBytes(CUSTOMER, 0.84);
        testWrittenBytes(ORDERS, 0.88);
        testWrittenBytes(LINE_ITEM, 0.86);
        testWrittenBytes(PART, 1.23);
        testWrittenBytes(PART_SUPPLIER, 0.82);
        testWrittenBytes(SUPPLIER, 0.81);
        testWrittenBytes(NATION, 0.66);
    }

    private <T extends TpchEntity> void testWrittenBytes(TpchTable<T> table, double expectedRatio)
            throws Exception
    {
        List<Type> columnTypes = table.getColumns().stream()
                .map(TestSortTempFileFactory::getColumnType)
                .collect(toImmutableList());

        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
        int actualRows = collectPages(table, pagesBuilder);
        List<Page> pages = pagesBuilder.build();

        long optimizedBytes = estimateWrittenBytes(columnTypes, pages, true);
        long orcBytes = estimateWrittenBytes(columnTypes, pages, false);

        double actualRatio = (double) optimizedBytes / orcBytes;
        double tolerance = 0.02; // 2% tolerance around expected ratio
        assertThat(actualRatio)
                .as("Written bytes ratio should be within %.0f%% of expected ratio %.4f. Optimized: %d bytes, ORC: %d bytes, Actual ratio: %.4f, Types: %s, Rows: %d",
                        tolerance * 100, expectedRatio, optimizedBytes, orcBytes, actualRatio, columnTypes, actualRows)
                .isBetween(expectedRatio * (1 - tolerance), expectedRatio * (1 + tolerance));
    }

    private static <T extends TpchEntity> int collectPages(TpchTable<T> table, ImmutableList.Builder<Page> pagesBuilder)
    {
        List<TpchColumn<T>> columns = table.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TestSortTempFileFactory::getColumnType)
                .collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);

        int maxRows = 120_000;
        int actualRows = 0;

        Iterator<T> generator = table.createGenerator(10, 1, 1).iterator();
        for (int i = 0; i < maxRows; i++) {
            if (!generator.hasNext()) {
                break;
            }

            T row = generator.next();
            actualRows++;

            pageBuilder.declarePosition();
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                TpchColumn<T> column = columns.get(columnIndex);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);

                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(row));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(row));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(row));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(row));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(row)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pagesBuilder.add(page);
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pagesBuilder.add(pageBuilder.build());
        }

        return actualRows;
    }

    private static long estimateWrittenBytes(List<Type> columnTypes, List<Page> pages, boolean optimized)
            throws IOException
    {
        File targetDir = createTempDirectory(TestSortTempFileFactory.class.getSimpleName()).toFile();
        try {
            DataSize sortingBufferSize = DataSize.of(64, MEGABYTE);
            LocalFileSystem fileSystem = new LocalFileSystem(targetDir.toPath());
            PagesSerdeStreamFactory serdeStreamFactory = new PagesSerdeStreamFactory(
                    new InternalBlockEncodingSerde(new BlockEncodingManager(new FeaturesConfig()), TESTING_TYPE_MANAGER));
            SortTempFileFactory sortTempFileFactory = new SortTempFileFactory(serdeStreamFactory, optimized, sortingBufferSize);
            Location tempFileLocation = Location.of("local:///temp_optimized_" + optimized);

            PageStreamWriter writer = sortTempFileFactory.createWriter(columnTypes, fileSystem, tempFileLocation);
            for (Page page : pages) {
                writer.writePage(page);
            }
            writer.close();
            return sortTempFileFactory.estimateWrittenBytesToOutputFile(writer.getWrittenBytes());
        }
        finally {
            deleteRecursively(targetDir.toPath(), ALLOW_INSECURE);
        }
    }

    private static Type getColumnType(TpchColumn<?> input)
    {
        return switch (input.getType().getBase()) {
            case IDENTIFIER -> BIGINT;
            case INTEGER -> INTEGER;
            case DATE -> DATE;
            case DOUBLE -> DOUBLE;
            case VARCHAR -> createUnboundedVarcharType();
        };
    }

    private static <T extends TpchEntity> String runAndGetSummary(TpchTable<T> table)
            throws Exception
    {
        List<TpchColumn<T>> columns = table.getColumns();
        List<Type> types = columns.stream()
                .map(TestSortTempFileFactory::getColumnType)
                .collect(toImmutableList());

        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
        int actualRows = collectPages(table, pagesBuilder);
        List<Page> pages = pagesBuilder.build();

        long optimizedBytes = estimateWrittenBytes(types, pages, true);
        long orcBytes = estimateWrittenBytes(types, pages, false);

        double ratio = (double) optimizedBytes / orcBytes;

        return "%-15s | %8d | %,15d | %,15d | %10.4f%n".formatted(table.getTableName(), actualRows, optimizedBytes, orcBytes, ratio);
    }

    public static void main(String[] args)
            throws Exception
    {
        StringBuilder output = new StringBuilder();

        output.append("=".repeat(80)).append("\n");
        output.append("%-15s | %8s | %15s | %15s | %10s%n".formatted("Table", "Rows", "Optimized (B)", "ORC (B)", "Ratio"));
        output.append("=".repeat(80)).append("\n");

        output.append(runAndGetSummary(CUSTOMER));
        output.append(runAndGetSummary(ORDERS));
        output.append(runAndGetSummary(LINE_ITEM));
        output.append(runAndGetSummary(PART));
        output.append(runAndGetSummary(PART_SUPPLIER));
        output.append(runAndGetSummary(SUPPLIER));
        output.append(runAndGetSummary(NATION));

        output.append("=".repeat(80)).append("\n");

        System.out.print(output);
    }
}
