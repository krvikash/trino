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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTestUtils;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetFileFabricator.FabricatedParquet;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.CompressionCodec.ZSTD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestParquetFileFabricator
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    @AfterEach
    void cleanUp()
            throws Exception
    {
        closer.close();
    }

    @Test
    void testFabricateWithColumnPruning()
            throws IOException
    {
        // Create a Parquet file with 3 columns
        List<Type> types = List.of(BIGINT, INTEGER, VARCHAR);
        List<String> columnNames = List.of("col1", "col2", "col3");
        List<Page> pages = createTestPages(types, 1000);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Request only col1 and col3 (prune col2)
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("col1", 0, BIGINT),
                createColumn("col3", 2, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);
        assertThat(fabricated.rowCount()).isEqualTo(1000);

        // Verify fabricated file is valid and contains only requested columns
        ParquetDataSource dataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();

        assertThat(schema.getFields()).hasSize(2);
        assertThat(schema.getFieldIndex("col1")).isEqualTo(0);
        assertThat(schema.getFieldIndex("col3")).isEqualTo(1);

        // Verify we can read the data
        ParquetReader reader = ParquetTestUtils.createParquetReader(
                dataSource,
                metadata,
                List.of(BIGINT, VARCHAR),
                List.of("col1", "col3"));

        int rowCount = 0;
        SourcePage page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }
        assertThat(rowCount).isEqualTo(1000);
    }

    @Test
    void testFabricateWithAllColumns()
            throws IOException
    {
        // Create a Parquet file
        List<Type> types = List.of(BIGINT, INTEGER);
        List<String> columnNames = List.of("a", "b");
        List<Page> pages = createTestPages(types, 500);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Request all columns
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("a", 0, BIGINT),
                createColumn("b", 1, INTEGER));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);
        assertThat(fabricated.rowCount()).isEqualTo(500);

        // Verify all columns present
        ParquetDataSource dataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();

        assertThat(schema.getFields()).hasSize(2);
        assertThat(schema.containsField("a")).isTrue();
        assertThat(schema.containsField("b")).isTrue();
    }

    @Test
    void testFabricateEmptyFile()
            throws IOException
    {
        // Create a Parquet file with data
        List<Type> types = List.of(BIGINT);
        List<String> columnNames = List.of("col1");
        List<Page> pages = createTestPages(types, 100);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Request with a split range that doesn't overlap any row groups
        List<HiveColumnHandle> requestedColumns = List.of(createColumn("col1", 0, BIGINT));

        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///test.parquet"), Slices.wrappedBuffer(parquetFile.getBytes()));
        ParquetDataSource originalDataSource = closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));
        ParquetMetadata originalMetadata = MetadataReader.readFooter(originalDataSource);
        ParquetFileFabricator fabricator = closer.register(new ParquetFileFabricator(
                Long.MAX_VALUE, // Split start beyond file end
                100,
                originalDataSource,
                requestedColumns,
                List.of(),
                List.of(),
                Map.of(),
                new NameBasedColumnMatcher(),
                UTC,
                1000,
                ParquetReaderOptions.builder().build(),
                originalMetadata));

        FabricatedParquet fabricated = fabricator.fabricate();
        assertThat(fabricated.rowCount()).isEqualTo(0);
        assertThat(fabricated.data()).isEmpty();
    }

    @Test
    void testFabricateZeroRowFile()
            throws IOException
    {
        // Create a Parquet file with 0 rows (empty pages list)
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("col1", "col2");
        List<Page> emptyPages = List.of(); // No pages = no rows

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                emptyPages);

        // Request columns from the 0-row file
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("col1", 0, BIGINT),
                createColumn("col2", 1, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);

        assertThat(fabricated.rowCount()).isEqualTo(0);
        assertThat(fabricated.data()).isEmpty();
    }

    @Test
    void testFabricatePreservesCompression()
            throws IOException
    {
        testFabricatePreservesCompression(SNAPPY);
        testFabricatePreservesCompression(ZSTD);
        testFabricatePreservesCompression(UNCOMPRESSED);
    }

    private void testFabricatePreservesCompression(CompressionCodec compression)
            throws IOException
    {
        List<Type> types = List.of(BIGINT, INTEGER);
        List<String> columnNames = List.of("x", "y");
        List<Page> pages = createTestPages(types, 1000);

        Slice parquetFile = writeParquetFileWithCompression(types, columnNames, pages, compression);

        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("x", 0, BIGINT),
                createColumn("y", 1, INTEGER));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);
        assertThat(fabricated.rowCount()).isEqualTo(1000);

        // Verify compression is preserved
        ParquetDataSource dataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);

        metadata.getBlocks().forEach(block -> {
            block.columns().forEach(column -> {
                assertThat(column.getCodec().name()).isEqualTo(compression.name());
            });
        });
    }

    @Test
    void testFabricateMultipleTypes()
            throws IOException
    {
        // Test with various Parquet types to ensure type-agnostic handling
        List<Type> types = List.of(BIGINT, INTEGER, VARCHAR);
        List<String> columnNames = List.of("bigint_col", "int_col", "varchar_col");
        List<Page> pages = createTestPages(types, 200);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("bigint_col", 0, BIGINT),
                createColumn("int_col", 1, INTEGER),
                createColumn("varchar_col", 2, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);

        // Verify all types preserved
        ParquetDataSource dataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();

        assertThat(schema.getFields()).hasSize(3);

        // Read and verify data
        ParquetReader reader = ParquetTestUtils.createParquetReader(
                dataSource,
                metadata,
                types,
                columnNames);

        int rowCount = 0;
        SourcePage page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }
        assertThat(fabricated.rowCount()).isEqualTo(200);
        assertThat(rowCount).isEqualTo(200);
    }

    @Test
    void testFabricatedFileIsValidParquet(@TempDir Path tempDir)
            throws IOException
    {
        // Create Parquet file
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("id", "name");
        List<Page> pages = createTestPages(types, 500);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("id", 0, BIGINT),
                createColumn("name", 1, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);

        // Write to file and verify Trino can read it
        Path outputPath = tempDir.resolve("fabricated.parquet");
        Files.write(outputPath, fabricated.data().orElseThrow());

        // Read using Trino's ParquetReader
        TrinoInputFile inputFile = new MemoryInputFile(
                Location.of("memory:///fabricated.parquet"),
                Slices.wrappedBuffer(fabricated.data().orElseThrow()));
        ParquetDataSource dataSource = closer.register(new TrinoParquetDataSource(
                inputFile,
                ParquetReaderOptions.builder().build(),
                new FileFormatDataSourceStats()));

        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        ParquetReader reader = ParquetTestUtils.createParquetReader(
                dataSource,
                metadata,
                types,
                columnNames);

        // Should be able to read all data without errors
        int rowCount = 0;
        SourcePage page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }
        assertThat(fabricated.rowCount()).isEqualTo(500);
        assertThat(rowCount).isEqualTo(500);
    }

    private FabricatedParquet fabricateFile(Slice parquetFile, List<HiveColumnHandle> columns)
            throws IOException
    {
        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///test.parquet"), Slices.wrappedBuffer(parquetFile.getBytes()));
        ParquetDataSource dataSource = closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));

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

        ParquetFileFabricator fabricator = closer.register(new ParquetFileFabricator(
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
                ParquetReaderOptions.builder().build(),
                metadata));

        return fabricator.fabricate();
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

    @Test
    void testFabricateLineitemFile()
            throws IOException
    {
        Path lineitemPath = Path.of("../../lib/trino-parquet/src/test/resources/lineitem_sorted_by_shipdate/data.parquet");

        TrinoInputFile inputFile = new LocalInputFile(lineitemPath.toFile());
        ParquetDataSource dataSource = closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));

        // Read metadata to understand schema
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();

        // Request subset of columns (including date columns to test type-agnostic handling)
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("l_orderkey", 0, BIGINT),
                createColumn("l_quantity", 4, DecimalType.createDecimalType(12, 2)),
                createColumn("l_shipdate", 10, DATE));

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

        ParquetFileFabricator fabricator = closer.register(new ParquetFileFabricator(
                0,
                Long.MAX_VALUE,
                dataSource,
                requestedColumns,
                List.of(parquetTupleDomain),
                List.of(predicate),
                descriptorsByPath,
                new NameBasedColumnMatcher(),
                UTC,
                1000,
                ParquetReaderOptions.builder().build(),
                metadata));

        FabricatedParquet fabricated = fabricator.fabricate();

        // Verify fabricated file has only requested columns
        ParquetDataSource fabricatedDataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata fabricatedMetadata = MetadataReader.readFooter(fabricatedDataSource);
        MessageType fabricatedSchema = fabricatedMetadata.getFileMetaData().getSchema();

        assertThat(fabricatedSchema.getFields()).hasSize(requestedColumns.size());

        // Verify data is readable by Trino reader
        List<Type> types = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        for (HiveColumnHandle column : requestedColumns) {
            types.add(column.getType());
            columnNames.add(column.getBaseColumnName());
        }

        ParquetReader reader = ParquetTestUtils.createParquetReader(
                fabricatedDataSource,
                fabricatedMetadata,
                types,
                columnNames);

        int rowCount = 0;
        SourcePage page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }

        // Verify we read some data
        assertThat(rowCount).isGreaterThan(0);
    }

    @Test
    void testRowGroupFilteringBySplitRange()
            throws IOException
    {
        // Create Parquet file with multiple row groups
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("id", "value");
        List<Page> pages = createTestPages(types, 3000);

        // Force multiple row groups by setting small max block size
        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxBlockSize(DataSize.ofBytes(1000))
                        .build(),
                types,
                columnNames,
                pages);

        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///test.parquet"), Slices.wrappedBuffer(parquetFile.getBytes()));
        ParquetDataSource dataSource = closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));

        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        List<BlockMetadata> rowGroups = metadata.getBlocks();

        // Verify we have multiple row groups
        assertThat(rowGroups.size()).isGreaterThan(1);

        // Get middle row group(s) position
        BlockMetadata middleRowGroup = rowGroups.get(rowGroups.size() / 2);
        long splitStart = middleRowGroup.columns().stream()
                .mapToLong(col -> col.getStartingPos())
                .min()
                .orElse(0);
        long splitLength = middleRowGroup.columns().stream()
                .mapToLong(col -> col.getTotalSize())
                .sum();

        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("id", 0, BIGINT),
                createColumn("value", 1, VARCHAR));

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

        ParquetFileFabricator fabricator = closer.register(new ParquetFileFabricator(
                splitStart,
                splitLength,
                dataSource,
                requestedColumns,
                List.of(parquetTupleDomain),
                List.of(predicate),
                descriptorsByPath,
                new NameBasedColumnMatcher(),
                UTC,
                1000,
                ParquetReaderOptions.builder().build(),
                metadata));

        FabricatedParquet fabricated = fabricator.fabricate();

        // Verify fabricated file has only selected row group(s)
        ParquetDataSource fabricatedDataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata fabricatedMetadata = MetadataReader.readFooter(fabricatedDataSource);

        assertThat(fabricatedMetadata.getBlocks().size()).isLessThan(rowGroups.size());

        // Verify row count reflects filtering
        long expectedRows = middleRowGroup.rowCount();
        long actualRows = fabricatedMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();

        assertThat(actualRows).isLessThanOrEqualTo(expectedRows * 2); // Allow some tolerance
    }

    @Test
    void testRowGroupFilteringByPredicate()
            throws IOException
    {
        // Create file with multiple row groups with known value ranges
        List<Type> types = List.of(BIGINT);
        List<String> columnNames = List.of("col1");

        // Create pages with different value ranges for different row groups
        List<Page> pages = new ArrayList<>();
        // First row group: values 0-999
        pages.add(createPageWithRange(BIGINT, 1000, 0));
        // Second row group: values 1000-1999
        pages.add(createPageWithRange(BIGINT, 1000, 1000));
        // Third row group: values 2000-2999
        pages.add(createPageWithRange(BIGINT, 1000, 2000));

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxBlockSize(DataSize.ofBytes(1000))
                        .build(),
                types,
                columnNames,
                pages);

        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///test.parquet"), Slices.wrappedBuffer(parquetFile.getBytes()));
        ParquetDataSource dataSource = closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));

        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(schema, schema);

        // Create predicate that matches only values >= 1000 (should exclude first row group)
        ColumnDescriptor columnDescriptor = descriptorsByPath.get(List.of("col1"));
        TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.withColumnDomains(
                Map.of(columnDescriptor, Domain.create(
                        ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1000L)),
                        false)));

        TupleDomainParquetPredicate predicate = buildPredicate(
                schema,
                parquetTupleDomain,
                descriptorsByPath,
                UTC,
                false,
                false,
                false);

        List<HiveColumnHandle> requestedColumns = List.of(createColumn("col1", 0, BIGINT));

        ParquetFileFabricator fabricator = closer.register(new ParquetFileFabricator(
                0,
                Long.MAX_VALUE,
                dataSource,
                requestedColumns,
                List.of(parquetTupleDomain),
                List.of(predicate),
                descriptorsByPath,
                new NameBasedColumnMatcher(),
                UTC,
                1000,
                ParquetReaderOptions.builder().build(),
                metadata));

        FabricatedParquet fabricated = fabricator.fabricate();

        // Verify fabricated file excludes filtered row groups
        ParquetDataSource fabricatedDataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata fabricatedMetadata = MetadataReader.readFooter(fabricatedDataSource);

        // Should have fewer row groups than original
        assertThat(fabricatedMetadata.getBlocks().size()).isLessThan(metadata.getBlocks().size());

        // Verify row count reflects filtering (should be approximately 2000, not 3000)
        long rowCount = fabricatedMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();

        assertThat(rowCount).isLessThan(3000);
        assertThat(rowCount).isGreaterThanOrEqualTo(1000);
    }

    @Test
    void testCaseInsensitiveColumnMatching()
            throws IOException
    {
        // Create file with mixed-case column names
        List<Type> types = List.of(BIGINT, VARCHAR);
        List<String> columnNames = List.of("ColA", "ColB");
        List<Page> pages = createTestPages(types, 100);

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder().build(),
                types,
                columnNames,
                pages);

        // Request columns with different case
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("cola", 0, BIGINT),
                createColumn("COLB", 1, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);
        assertThat(fabricated.rowCount()).isEqualTo(100);

        // Verify matching worked and data is correct
        ParquetDataSource dataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource);
        MessageType schema = metadata.getFileMetaData().getSchema();

        assertThat(schema.getFields()).hasSize(2);
        // Verify columns are present (ParquetTestUtils writes lowercase column names)
        assertThat(schema.containsField("cola")).isTrue();
        assertThat(schema.containsField("colb")).isTrue();

        // Verify data is readable
        ParquetReader reader = ParquetTestUtils.createParquetReader(
                dataSource,
                metadata,
                types,
                columnNames);

        int rowCount = 0;
        SourcePage page = reader.nextPage();
        while (page != null) {
            rowCount += page.getPositionCount();
            page = reader.nextPage();
        }
        assertThat(rowCount).isEqualTo(100);
    }

    @Test
    void testMultipleRowGroupsPreserved()
            throws IOException
    {
        // Create file with multiple row groups
        List<Type> types = List.of(BIGINT, INTEGER, VARCHAR);
        List<String> columnNames = List.of("a", "b", "c");

        // Create multiple pages to force multiple row groups
        List<Page> pages = new ArrayList<>();
        pages.add(createPageWithRange(types, 500, 0));
        pages.add(createPageWithRange(types, 500, 500));
        pages.add(createPageWithRange(types, 500, 1000));

        Slice parquetFile = ParquetTestUtils.writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxBlockSize(DataSize.ofBytes(1000))
                        .build(),
                types,
                columnNames,
                pages);

        // Request all columns, no filtering
        List<HiveColumnHandle> requestedColumns = List.of(
                createColumn("a", 0, BIGINT),
                createColumn("b", 1, INTEGER),
                createColumn("c", 2, VARCHAR));

        FabricatedParquet fabricated = fabricateFile(parquetFile, requestedColumns);

        // Verify fabricated file maintains all row groups
        ParquetDataSource originalDataSource = createDataSource(parquetFile.getBytes());
        ParquetMetadata originalMetadata = MetadataReader.readFooter(originalDataSource);

        ParquetDataSource fabricatedDataSource = createDataSource(fabricated.data().orElseThrow());
        ParquetMetadata fabricatedMetadata = MetadataReader.readFooter(fabricatedDataSource);

        // Verify we have multiple row groups
        assertThat(fabricatedMetadata.getBlocks().size()).isGreaterThanOrEqualTo(3);

        // Verify row counts per row group match or are close
        long originalRowCount = originalMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();
        long fabricatedRowCount = fabricatedMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();

        assertThat(fabricated.rowCount()).isEqualTo(originalRowCount);
        assertThat(fabricatedRowCount).isEqualTo(originalRowCount);
    }

    private static List<Page> createTestPages(List<Type> types, int rowCount)
    {
        List<Block> blocks = new ArrayList<>();
        for (Type type : types) {
            BlockBuilder builder = type.createBlockBuilder(null, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (type == BIGINT) {
                    type.writeLong(builder, i);
                }
                else if (type == INTEGER) {
                    type.writeLong(builder, i);
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

    private Page createPageWithRange(Type type, int rowCount, long startValue)
    {
        BlockBuilder builder = type.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            type.writeLong(builder, startValue + i);
        }
        return new Page(builder.build());
    }

    private Page createPageWithRange(List<Type> types, int rowCount, long startValue)
    {
        List<Block> blocks = new ArrayList<>();
        for (Type type : types) {
            BlockBuilder builder = type.createBlockBuilder(null, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (type == BIGINT) {
                    type.writeLong(builder, startValue + i);
                }
                else if (type == INTEGER) {
                    type.writeLong(builder, startValue + i);
                }
                else if (type == VARCHAR) {
                    type.writeSlice(builder, Slices.utf8Slice("value_" + (startValue + i)));
                }
                else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
            }
            blocks.add(builder.build());
        }
        return new Page(blocks.toArray(new Block[0]));
    }

    private ParquetDataSource createDataSource(byte[] data)
            throws IOException
    {
        TrinoInputFile inputFile = new MemoryInputFile(Location.of("memory:///fabricated.parquet"), Slices.wrappedBuffer(data));
        return closer.register(new TrinoParquetDataSource(inputFile, ParquetReaderOptions.builder().build(), new FileFormatDataSourceStats()));
    }

    private static HiveColumnHandle createColumn(String name, int hiveColumnIndex, Type type)
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
}
