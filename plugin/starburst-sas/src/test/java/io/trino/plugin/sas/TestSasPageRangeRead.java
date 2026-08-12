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
package io.trino.plugin.sas;

import com.epam.parso.impl.CustomSasFileParser;
import com.epam.parso.impl.SasFileReaderImpl;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.scharp.sas7bdat.Sas7bdatExporter;
import org.scharp.sas7bdat.Sas7bdatMetadata;
import org.scharp.sas7bdat.Variable;
import org.scharp.sas7bdat.VariableType;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that page-range reads through {@link CustomSasFileParser#setPageRange} partition a file
 * without losing, duplicating, or reordering rows. Concurrent split cursors each open the file and
 * restrict themselves to a page range; concatenating the ranges in order must reproduce a full
 * sequential read exactly.
 */
final class TestSasPageRangeRead
{
    // SASYZCRL-compressed, 3 pages, 328 rows — rows live in data subheaders on META pages
    private static final Path COMPRESSED_FILE = Path.of("src/test/resources/prod/param_gen_nn1.sas7bdat").toAbsolutePath();
    // uncompressed, 7 pages, 15564 rows — rows read positionally from MIX/DATA pages
    private static final Path UNCOMPRESSED_FILE = Path.of("src/test/resources/schema1/colon.sas7bdat").toAbsolutePath();

    @Test
    void testCompressedFileSplitsCoverAllRowsExactlyOnce()
    {
        assertPageRangesMatchFullRead(COMPRESSED_FILE, 3, 328);
    }

    @Test
    void testUncompressedFileSplitsCoverAllRowsExactlyOnce()
    {
        assertPageRangesMatchFullRead(UNCOMPRESSED_FILE, 7, 15564);
    }

    @Test
    void testRangeBeyondEndOfFileIsEmpty()
    {
        assertThat(readPageRange(COMPRESSED_FILE, 10, 12)).isEmpty();
        assertThat(readPageRange(UNCOMPRESSED_FILE, 7, 14)).isEmpty();
    }

    @Test
    void testRangeLargerThanFileReadsAllRows()
    {
        assertThat(readPageRange(COMPRESSED_FILE, 0, 100)).hasSize(328);
        assertThat(readPageRange(UNCOMPRESSED_FILE, 0, 100)).hasSize(15564);
    }

    @Test
    void testMetadataSpanningMultiplePages(@TempDir Path tempDir)
            throws Exception
    {
        // A table wide enough that its column metadata overflows onto several pages: the parser
        // constructor then consumes pages 0..k with k > 0, and splits covering those pages must
        // serve nothing while the split containing the first data-bearing page serves its rows
        int columnCount = 400;
        int rowCount = 200;
        Path file = generateWideFile(tempDir.resolve("wide.sas7bdat"), columnCount, rowCount);

        assertThat(firstDataBearingPage(file))
                .as("metadata must span multiple pages for this test to cover the k > 0 case")
                .isGreaterThanOrEqualTo(1);

        long pageCount;
        try (InputStream in = Files.newInputStream(file)) {
            pageCount = new SasFileReaderImpl(in).getSasFileProperties().getPageCount();
        }
        assertThat(pageCount).isGreaterThanOrEqualTo(4);

        assertPageRangesMatchFullRead(file, (int) pageCount, rowCount);
    }

    private static Path generateWideFile(Path file, int columnCount, int rowCount)
            throws IOException
    {
        ImmutableList.Builder<Variable> variables = ImmutableList.builder();
        for (int column = 0; column < columnCount; column++) {
            variables.add(Variable.builder()
                    .name("WIDE_TABLE_COLUMN_NUMBER_" + column)
                    .type(VariableType.NUMERIC)
                    .length(8)
                    .label(("This is a deliberately long column label for column number " + column + " ").repeat(3))
                    .build());
        }
        Sas7bdatMetadata metadata = Sas7bdatMetadata.builder()
                .datasetName("WIDE")
                .variables(variables.build())
                .build();

        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (int row = 0; row < rowCount; row++) {
            ImmutableList.Builder<Object> values = ImmutableList.builder();
            for (int column = 0; column < columnCount; column++) {
                values.add((double) (row * columnCount + column));
            }
            rows.add(values.build());
        }
        Sas7bdatExporter.exportDataset(file, metadata, rows.build());
        return file;
    }

    private static long firstDataBearingPage(Path file)
            throws Exception
    {
        try (InputStream in = Files.newInputStream(file)) {
            CustomSasFileParser reader = new CustomSasFileParser.Builder(in).build();
            // the fork tracks the stream index of the page loaded during metadata parsing
            Field currentPageIndex = CustomSasFileParser.class.getDeclaredField("currentPageIndex");
            currentPageIndex.setAccessible(true);
            return (long) currentPageIndex.get(reader);
        }
    }

    private static void assertPageRangesMatchFullRead(Path file, int pageCount, int expectedRowCount)
    {
        List<List<Object>> fullRead = readFully(file);
        assertThat(fullRead).hasSize(expectedRowCount);

        for (int pagesPerSplit = 1; pagesPerSplit <= pageCount; pagesPerSplit++) {
            ImmutableList.Builder<List<Object>> combined = ImmutableList.builder();
            for (int start = 0; start < pageCount; start += pagesPerSplit) {
                combined.addAll(readPageRange(file, start, start + pagesPerSplit));
            }
            assertThat(combined.build())
                    .as("rows concatenated from page ranges of %s pages each", pagesPerSplit)
                    .isEqualTo(fullRead);
        }
    }

    @Test
    void testShortSkippingStreamKeepsPagesAligned()
    {
        // InputStream.skip may legally consume fewer bytes than requested; skipping pages must not
        // desynchronize the tracked page index from the actual stream position when that happens
        assertThat(read(COMPRESSED_FILE, ShortSkippingInputStream::new, reader -> reader.setPageRange(2, 3)))
                .isEqualTo(readPageRange(COMPRESSED_FILE, 2, 3));
        assertThat(read(UNCOMPRESSED_FILE, ShortSkippingInputStream::new, reader -> reader.setPageRange(3, 5)))
                .isEqualTo(readPageRange(UNCOMPRESSED_FILE, 3, 5));
        assertThat(read(UNCOMPRESSED_FILE, ShortSkippingInputStream::new, reader -> reader.setPageRange(100, 200)))
                .isEmpty();
    }

    private static List<List<Object>> readFully(Path file)
    {
        return read(file, UnaryOperator.identity(), _ -> {});
    }

    private static List<List<Object>> readPageRange(Path file, long startPage, long endPageExclusive)
    {
        return read(file, UnaryOperator.identity(), reader -> reader.setPageRange(startPage, endPageExclusive));
    }

    private static List<List<Object>> read(Path file, UnaryOperator<InputStream> streamDecorator, ReaderConfigurator configurator)
    {
        try (InputStream in = streamDecorator.apply(Files.newInputStream(file))) {
            CustomSasFileParser reader = new CustomSasFileParser.Builder(in).build();
            configurator.configure(reader);
            ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
            Object[] row;
            while ((row = reader.readNext()) != null) {
                // Arrays.asList instead of ImmutableList: row values can be null
                rows.add(Arrays.asList(row));
            }
            return rows.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface ReaderConfigurator
    {
        void configure(CustomSasFileParser reader)
                throws IOException;
    }

    private static final class ShortSkippingInputStream
            extends FilterInputStream
    {
        private ShortSkippingInputStream(InputStream delegate)
        {
            super(delegate);
        }

        @Override
        public long skip(long requested)
                throws IOException
        {
            // exercise the InputStream.skip contract: consume fewer bytes than requested
            return super.skip(Math.min(requested, 7));
        }
    }
}
