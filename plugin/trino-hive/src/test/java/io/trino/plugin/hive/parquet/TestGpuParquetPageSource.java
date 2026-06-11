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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTestUtils;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.ConnectorGpuPageSource.Result;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.format.CompressionCodec.GZIP;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.CompressionCodec.ZSTD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestGpuParquetPageSource
{
    @Test
    public void testBasicReadWithAllTypes()
            throws IOException
    {
        // Test all supported types with UNCOMPRESSED
        List<Type> types = List.of(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                VARCHAR);
        List<String> columnNames = List.of(
                "col_boolean",
                "col_tinyint",
                "col_smallint",
                "col_integer",
                "col_bigint",
                "col_real",
                "col_double",
                "col_varchar");

        int rowCount = 100;
        List<Page> pages = createTestPages(types, rowCount);

        Slice parquetFile = writeParquetFileWithCompression(types, columnNames, pages, UNCOMPRESSED);

        List<HiveColumnHandle> columns = List.of(
                createColumn("col_boolean", 0, BOOLEAN),
                createColumn("col_tinyint", 1, TINYINT),
                createColumn("col_smallint", 2, SMALLINT),
                createColumn("col_integer", 3, INTEGER),
                createColumn("col_bigint", 4, BIGINT),
                createColumn("col_real", 5, REAL),
                createColumn("col_double", 6, DOUBLE),
                createColumn("col_varchar", 7, VARCHAR));

        List<ColumnMapping> columnMappings = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            columnMappings.add(ColumnMapping.regular(columns.get(i), i, Optional.empty()));
        }

        try (Pages readPages = readGpuPages(parquetFile, columns, columnMappings)) {
            @Borrow GpuPage page = getOnlyElement(readPages.pages());
            assertThat(page.positionCount()).isEqualTo(rowCount);
            assertThat(page.columnCount()).isEqualTo(8);

            // Verify data correctness by reading back from GPU
            verifyGpuPageData(page, types, getOnlyElement(pages));
        }
    }

    @Test
    public void testReadWithPrefilledColumns()
            throws IOException
    {
        // Mix of REGULAR (from GPU) and PREFILLED (partition keys)
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("id", "name");
        int rowCount = 50;
        List<Page> pages = createTestPages(types, rowCount);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Create columns: 2 regular + 1 prefilled partition key
        List<HiveColumnHandle> regularColumns = List.of(
                createColumn("id", 0, BIGINT),
                createColumn("name", 1, VARCHAR));

        HiveColumnHandle partitionColumn = createPartitionColumn("partition_key", 2, INTEGER);

        List<ColumnMapping> columnMappings = List.of(
                ColumnMapping.regular(regularColumns.get(0), 0, Optional.empty()),
                ColumnMapping.regular(regularColumns.get(1), 1, Optional.empty()),
                ColumnMapping.prefilled(partitionColumn, NullableValue.of(INTEGER, 42L), Optional.empty()));

        try (Pages readPages = readGpuPages(parquetFile, regularColumns, columnMappings)) {
            @Borrow GpuPage page = getOnlyElement(readPages.pages());
            assertThat(page.positionCount()).isEqualTo(rowCount);
            assertThat(page.columnCount()).isEqualTo(3);

            // Verify first two columns are from GPU
            assertThat(page.column(0)).isInstanceOf(Column.DeviceMemory.class);
            assertThat(page.column(1)).isInstanceOf(Column.DeviceMemory.class);

            // Verify the prefilled column
            @Borrow ColumnVector columnVector = ((Column.DeviceMemory) page.column(2)).columnVector();
            try (HostColumnVector hostColumnVector = columnVector.copyToHost()) {
                assertThat(hostColumnVector.getRowCount()).isEqualTo(rowCount);
                for (int row = 0; row < rowCount; row++) {
                    assertThat(hostColumnVector.getInt(row)).isEqualTo(42);
                }
            }
        }
    }

    @Test
    public void testZeroRowFile()
            throws IOException
    {
        // File created with 0 rows
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("col1", "col2");
        List<Page> emptyPages = List.of();

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                emptyPages);

        List<HiveColumnHandle> columns = List.of(
                createColumn("col1", 0, BIGINT),
                createColumn("col2", 1, VARCHAR));

        List<ColumnMapping> columnMappings = List.of(
                ColumnMapping.regular(columns.get(0), 0, Optional.empty()),
                ColumnMapping.regular(columns.get(1), 1, Optional.empty()));

        try (Pages readPages = readGpuPages(parquetFile, columns, columnMappings)) {
            assertThat(readPages.pages()).isEmpty();
        }
    }

    @Test
    public void testColumnPruning()
            throws IOException
    {
        // File with 5 columns, request only 2
        List<Type> types = List.of(BIGINT, INTEGER, VARCHAR, DOUBLE, BOOLEAN);
        List<String> columnNames = List.of("col1", "col2", "col3", "col4", "col5");
        int rowCount = 75;
        List<Page> pages = createTestPages(types, rowCount);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Request only col1 and col4
        List<HiveColumnHandle> columns = List.of(
                createColumn("col1", 0, BIGINT),
                createColumn("col4", 3, DOUBLE));

        List<ColumnMapping> columnMappings = List.of(
                ColumnMapping.regular(columns.get(0), 0, Optional.empty()),
                ColumnMapping.regular(columns.get(1), 1, Optional.empty()));

        try (Pages readPages = readGpuPages(parquetFile, columns, columnMappings)) {
            @Borrow GpuPage page = getOnlyElement(readPages.pages());
            assertThat(page.positionCount()).isEqualTo(rowCount);
            assertThat(page.columnCount()).isEqualTo(2);
        }
    }

    @Test
    public void testMultipleRowGroups()
            throws IOException
    {
        // File with 3+ row groups
        List<Type> types = List.of(BIGINT, INTEGER);
        List<String> columnNames = List.of("a", "b");

        // Create multiple pages to ensure multiple row groups
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.addAll(createTestPages(types, 200));
        }

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        List<HiveColumnHandle> columns = List.of(
                createColumn("a", 0, BIGINT),
                createColumn("b", 1, INTEGER));

        List<ColumnMapping> columnMappings = List.of(
                ColumnMapping.regular(columns.get(0), 0, Optional.empty()),
                ColumnMapping.regular(columns.get(1), 1, Optional.empty()));

        try (Pages readPages = readGpuPages(parquetFile, columns, columnMappings)) {
            @Borrow GpuPage page = getOnlyElement(readPages.pages());
            assertThat(page.positionCount()).isEqualTo(1000);
            assertThat(page.columnCount()).isEqualTo(2);
        }
    }

    @Test
    public void testZstdCompression()
            throws IOException
    {
        testCompressionCodec(ZSTD);
    }

    @Test
    public void testGzipCompression()
            throws IOException
    {
        testCompressionCodec(GZIP);
    }

    @Test
    public void testSnappyCompression()
            throws IOException
    {
        testCompressionCodec(SNAPPY);
    }

    @Test
    public void testUncompressedFile()
            throws IOException
    {
        testCompressionCodec(UNCOMPRESSED);
    }

    private void testCompressionCodec(CompressionCodec codec)
            throws IOException
    {
        List<Type> types = List.of(BIGINT, VARCHAR, DOUBLE);
        List<String> columnNames = List.of("x", "y", "z");
        int rowCount = 150;
        List<Page> pages = createTestPages(types, rowCount);

        Slice parquetFile = writeParquetFileWithCompression(types, columnNames, pages, codec);

        List<HiveColumnHandle> columns = List.of(
                createColumn("x", 0, BIGINT),
                createColumn("y", 1, VARCHAR),
                createColumn("z", 2, DOUBLE));

        List<ColumnMapping> columnMappings = List.of(
                ColumnMapping.regular(columns.get(0), 0, Optional.empty()),
                ColumnMapping.regular(columns.get(1), 1, Optional.empty()),
                ColumnMapping.regular(columns.get(2), 2, Optional.empty()));

        try (Pages readPages = readGpuPages(parquetFile, columns, columnMappings)) {
            @Borrow GpuPage page = getOnlyElement(readPages.pages());
            assertThat(page.positionCount()).isEqualTo(rowCount);
            assertThat(page.columnCount()).isEqualTo(3);

            // Verify GPU decompression worked correctly
            verifyGpuPageData(page, types, getOnlyElement(pages));
        }
    }

    private @Move Pages readGpuPages(
            Slice parquetFile,
            List<HiveColumnHandle> columns,
            List<ColumnMapping> columnMappings)
            throws IOException
    {
        TrinoInputFile inputFile = new MemoryInputFile(
                Location.of("memory:///test.parquet"),
                Slices.wrappedBuffer(parquetFile.getBytes()));
        ParquetDataSource dataSource = new TrinoParquetDataSource(
                inputFile,
                ParquetReaderOptions.builder().build(),
                new FileFormatDataSourceStats());

        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(schema, schema);

        TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.all();
        TupleDomainParquetPredicate predicate = buildPredicate(
                schema,
                parquetTupleDomain,
                descriptorsByPath,
                UTC,
                false,
                false,
                false);

        try (ParquetFileFabricator fabricator = new ParquetFileFabricator(
                0,
                Long.MAX_VALUE,
                dataSource,
                columns,
                List.of(parquetTupleDomain),
                List.of(predicate),
                descriptorsByPath,
                new NameBasedColumnMatcher(),
                UTC,
                1000,
                newSimpleAggregatedMemoryContext(),
                ParquetReaderOptions.builder().build(),
                metadata)) {
            try (GpuParquetPageSource pageSource = new GpuParquetPageSource(
                    fabricator,
                    columns,
                    columnMappings)) {
                List<@Own GpuPage> pages = new ArrayList<>();
                try {
                    boolean finished = false;
                    while (!finished) {
                        @Own Result next = pageSource.readNext();
                        switch (next) {
                            case ConnectorGpuPageSource.Blocked(CompletableFuture<?> _) -> throw new IllegalStateException("Blocking not supported");
                            case ConnectorGpuPageSource.Data(GpuPage page) -> pages.add(page);
                            case ConnectorGpuPageSource.Finished() -> finished = true;
                            case ConnectorGpuPageSource.Yielded() -> {
                                /* continue */
                            }
                        }
                    }
                }
                catch (Throwable e) {
                    closeAllSuppress(e, pages.toArray(new GpuPage[0]));
                    throw e;
                }

                return new Pages(pages);
            }
        }
    }

    private void verifyGpuPageData(GpuPage gpuPage, List<Type> expectedTypes, Page expectedPage)
    {
        for (int columnIndex = 0; columnIndex < expectedTypes.size(); columnIndex++) {
            Type type = expectedTypes.get(columnIndex);
            Column column = gpuPage.column(columnIndex);

            assertThat(column).isInstanceOf(Column.DeviceMemory.class);
            Column.DeviceMemory deviceMemory = (Column.DeviceMemory) column;
            ColumnVector cudfColumn = deviceMemory.columnVector();

            // Copy from GPU to host for verification
            try (HostColumnVector hostColumn = cudfColumn.copyToHost()) {
                Block expectedBlock = expectedPage.getBlock(columnIndex);

                for (int row = 0; row < gpuPage.positionCount(); row++) {
                    if (expectedBlock.isNull(row)) {
                        assertThat(hostColumn.isNull(row)).isTrue();
                    }
                    else if (type == BOOLEAN) {
                        assertThat(hostColumn.getBoolean(row))
                                .isEqualTo(BOOLEAN.getBoolean(expectedBlock, row));
                    }
                    else if (type == TINYINT) {
                        assertThat(hostColumn.getByte(row))
                                .isEqualTo(TINYINT.getByte(expectedBlock, row));
                    }
                    else if (type == SMALLINT) {
                        assertThat(hostColumn.getShort(row))
                                .isEqualTo(SMALLINT.getShort(expectedBlock, row));
                    }
                    else if (type == INTEGER) {
                        assertThat(hostColumn.getInt(row))
                                .isEqualTo(INTEGER.getInt(expectedBlock, row));
                    }
                    else if (type == BIGINT) {
                        assertThat(hostColumn.getLong(row))
                                .isEqualTo(BIGINT.getLong(expectedBlock, row));
                    }
                    else if (type == REAL) {
                        assertThat(hostColumn.getFloat(row))
                                .isEqualTo(REAL.getFloat(expectedBlock, row));
                    }
                    else if (type == DOUBLE) {
                        assertThat(hostColumn.getDouble(row))
                                .isEqualTo(DOUBLE.getDouble(expectedBlock, row));
                    }
                    else if (type == VARCHAR) {
                        assertThat(new String(hostColumn.getUTF8(row), StandardCharsets.UTF_8))
                                .isEqualTo(VARCHAR.getSlice(expectedBlock, row).toStringUtf8());
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                }
            }
        }
    }

    private Slice writeParquetFileWithCompression(
            List<Type> types,
            List<String> columnNames,
            List<Page> pages,
            CompressionCodec compression)
            throws IOException
    {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (ParquetWriter writer = ParquetTestUtils.createParquetWriter(outputStream, ParquetWriterOptions.builder().build(), types, columnNames, compression)) {
                for (Page page : pages) {
                    writer.write(page);
                }
            }
            return Slices.wrappedBuffer(outputStream.toByteArray());
        }
    }

    private List<Page> createTestPages(List<Type> types, int rowCount)
    {
        List<Block> blocks = new ArrayList<>();
        for (Type type : types) {
            BlockBuilder builder = type.createBlockBuilder(null, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (type == BOOLEAN) {
                    type.writeBoolean(builder, i % 2 == 0);
                }
                else if (type == TINYINT) {
                    type.writeLong(builder, (byte) (i % 128));
                }
                else if (type == SMALLINT) {
                    type.writeLong(builder, (short) i);
                }
                else if (type == INTEGER) {
                    type.writeLong(builder, i);
                }
                else if (type == BIGINT) {
                    type.writeLong(builder, i);
                }
                else if (type == REAL) {
                    REAL.writeFloat(builder, (float) i + 0.5f);
                }
                else if (type == DOUBLE) {
                    type.writeDouble(builder, i + 0.25);
                }
                else if (type == VARCHAR) {
                    type.writeSlice(builder, Slices.utf8Slice("value_" + i));
                }
                else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
            }
            blocks.add(builder.build());
        }
        return List.of(new Page(blocks.toArray(new Block[0])));
    }

    private HiveColumnHandle createColumn(String name, int hiveColumnIndex, Type type)
    {
        return new HiveColumnHandle(
                name,
                hiveColumnIndex,
                toHiveType(type),
                type,
                Optional.empty(),
                REGULAR,
                Optional.empty());
    }

    private HiveColumnHandle createPartitionColumn(String name, int hiveColumnIndex, Type type)
    {
        return new HiveColumnHandle(
                name,
                hiveColumnIndex,
                toHiveType(type),
                type,
                Optional.empty(),
                PARTITION_KEY,
                Optional.empty());
    }

    private record Pages(@Own List<GpuPage> pages)
            implements RuntimeCloseable
    {
        Pages
        {
            pages = ImmutableList.copyOf(pages);
        }

        @Override
        public void close()
        {
            pages.forEach(GpuPage::close);
        }
    }
}
