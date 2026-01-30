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
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.hive.util.SortTempFileFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingConnectorContext;
import io.trino.tpch.LineItem;
import io.trino.tpch.TpchColumn;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.MemPoolProfiler;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(1)
@Warmup(iterations = 12, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkSortingFileWriter
{
    private static final long MIN_DATA_SIZE = DataSize.of(500, MEGABYTE).toBytes();
    private static final DataSize LARGE_PAGE_MIN_SIZE = DataSize.of(4, MEGABYTE);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final FileWriter OUTPUT_WRITER = new NoOpFileWriter();
    private static final int MAX_OPEN_TEMP_FILES = 30;
    private static final DataSize SORTING_BUFFER_SIZE = DataSize.of(16, MEGABYTE);
    private static final int RANDOM_SEED = 64542213;

    @Param({
            "LARGE_PAGE_COMPRESSIBLE",
            "LARGE_PAGE_INCOMPRESSIBLE",
            "LINE_ITEM_VARCHAR",
            "LINE_ITEM_BIGINT"
    })
    public DataSet dataSet;

    @Param({"true", "false"})
    public boolean optimizedSortedWriterEnabled;

    private TestData data;
    private File targetDir;
    private SortTempFileFactory sortTempFileFactory;
    private LocalFileSystem fileSystem;
    private Location tempFilePrefix;

    @Setup
    public void setup()
            throws IOException
    {
        targetDir = createTempDirectory(BenchmarkSortingFileWriter.class.getSimpleName()).toFile();
        data = dataSet.createTestData();
        sortTempFileFactory = new SortTempFileFactory(new TestingConnectorContext().getPageStreamFactory(), optimizedSortedWriterEnabled, SORTING_BUFFER_SIZE);
        fileSystem = new LocalFileSystem(targetDir.toPath());
        tempFilePrefix = Location.of("local:///temp_" + UUID.randomUUID());
    }

    @TearDown
    public void tearDown()
            throws IOException
    {
        deleteRecursively(targetDir.toPath(), ALLOW_INSECURE);
    }

    @Benchmark
    public Closeable write()
            throws IOException
    {
        List<Page> inputPages = data.pages();
        SortingFileWriter writer = createWriter();

        for (Page page : inputPages) {
            writer.appendRows(page);
        }
        return writer.commit();
    }

    @Test
    public void testBenchmarkData()
            throws IOException
    {
        optimizedSortedWriterEnabled = true;
        for (DataSet dataSet : DataSet.values()) {
            this.dataSet = dataSet;
            setup();

            SortingFileWriter writer = createWriter();

            long fileCount = 0;
            List<Page> inputPages = data.pages();
            for (Page page : inputPages) {
                writer.appendRows(page);
                try (Stream<Path> pathStream = Files.list(targetDir.toPath())) {
                    fileCount = Math.max(fileCount, pathStream.count());
                }
            }
            writer.commit();

            // Ensure the benchmark generates enough temp files to exercise the combining logic
            assertThat(fileCount).isGreaterThanOrEqualTo(MAX_OPEN_TEMP_FILES);

            tearDown();
        }
    }

    private SortingFileWriter createWriter()
    {
        return new SortingFileWriter(
                fileSystem,
                tempFilePrefix,
                OUTPUT_WRITER,
                SORTING_BUFFER_SIZE,
                MAX_OPEN_TEMP_FILES,
                sortTempFileFactory,
                data.columnTypes(),
                data.sortFields(),
                data.sortOrders(),
                PAGE_SORTER,
                TYPE_OPERATORS);
    }

    public enum DataSet
    {
        LARGE_PAGE_COMPRESSIBLE {
            @Override
            public TestData createTestData()
            {
                // Fewer unique symbols to increase compressibility
                return createLargePageDataSet("abcdef");
            }
        },
        LARGE_PAGE_INCOMPRESSIBLE {
            @Override
            public TestData createTestData()
            {
                // More unique symbols to decrease compressibility
                return createLargePageDataSet("abcdefghijklmnopqrstuvwxyz");
            }
        },
        LINE_ITEM_VARCHAR {
            @Override
            public TestData createTestData()
            {
                return createLineItemBasedDataSet(VARCHAR);
            }
        },
        LINE_ITEM_BIGINT {
            @Override
            public TestData createTestData()
            {
                return createLineItemBasedDataSet(BIGINT);
            }
        };

        public abstract TestData createTestData();
    }

    private static TestData createLargePageDataSet(String symbols)
    {
        Random random = new Random(RANDOM_SEED);

        int columnCount = 1000;
        int rowCount = 1000;
        int stringLength = 5000;

        List<Type> columnTypes = IntStream.range(0, columnCount)
                .mapToObj(_ -> VARCHAR)
                .collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        long dataSize = 0;
        for (int row = 0; row < rowCount; row++) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                createUnboundedVarcharType().writeString(blockBuilder, generateRandomString(random, symbols, stringLength));
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();

                checkState(
                        page.getSizeInBytes() >= LARGE_PAGE_MIN_SIZE.toBytes(),
                        "Generated page size %s is smaller than the target size %s",
                        page.getSizeInBytes(),
                        LARGE_PAGE_MIN_SIZE.toBytes());

                pages.add(page);
                pageBuilder.reset();
                dataSize += page.getSizeInBytes();

                if (dataSize >= MIN_DATA_SIZE) {
                    break;
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }

        return new TestData(columnTypes, pages.build(), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST));
    }

    private static String generateRandomString(Random random, String symbols, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = symbols.charAt(random.nextInt(symbols.length()));
        }
        return new String(chars);
    }

    private static TestData createLineItemBasedDataSet(Type sortByColumnType)
    {
        List<TpchColumn<LineItem>> columns = LINE_ITEM.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(BenchmarkSortingFileWriter::getColumnType)
                .collect(toImmutableList());

        Integer sortByColumnIndex = null;

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        long dataSize = 0;
        for (LineItem row : LINE_ITEM.createGenerator(10, 1, 1)) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                TpchColumn<LineItem> column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

                if (sortByColumnIndex == null && getColumnType(column).equals(sortByColumnType)) {
                    sortByColumnIndex = i;
                }

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
                pages.add(page);
                pageBuilder.reset();
                dataSize += page.getSizeInBytes();

                if (dataSize >= MIN_DATA_SIZE) {
                    break;
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }

        checkState(sortByColumnIndex != null, "Sort by column index not set");

        return new TestData(columnTypes, pages.build(), ImmutableList.of(sortByColumnIndex), ImmutableList.of(ASC_NULLS_FIRST));
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

    public record TestData(List<Type> columnTypes, List<Page> pages, List<Integer> sortFields, List<SortOrder> sortOrders) {}

    // We are not interested in benchmarking writing to the final output file.
    private static class NoOpFileWriter
            implements FileWriter
    {
        @Override
        public long getWrittenBytes()
        {
            return 0;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void appendRows(Page dataPage)
        {
        }

        @Override
        public Closeable commit()
        {
            return () -> {};
        }

        @Override
        public void rollback()
        {
        }

        @Override
        public long getValidationCpuNanos()
        {
            return 0;
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkSortingFileWriter.class)
                .withOptions(
                        options -> options
                                .addProfiler(GCProfiler.class)
                                .addProfiler(MemPoolProfiler.class))
                .run();
    }
}
