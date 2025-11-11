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
package io.trino.plugin.hive.util;

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.util.ChunkedInputStream;
import io.trino.plugin.hive.SortingFileWriterConfig;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.PageStreamReader;
import io.trino.spi.PageStreamWriter;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SortTempFileFactory
{
    private static final long MIN_READ_CHUNK_SIZE_IN_BYTES = DataSize.of(4, KILOBYTE).toBytes();
    private static final long MAX_READ_CHUNK_SIZE_IN_BYTES = DataSize.of(8, MEGABYTE).toBytes();

    private final PageStreamFactory pageStreamFactory;
    private final boolean usePagesSerde;
    private final DataSize writerSortBufferSize;

    @Inject
    public SortTempFileFactory(PageStreamFactory pageStreamFactory, SortingFileWriterConfig config)
    {
        this(pageStreamFactory, config.isOptimizedSortedWriterEnabled(), config.getWriterSortBufferSize());
    }

    public SortTempFileFactory(PageStreamFactory pageStreamFactory, boolean usePagesSerde, DataSize writerSortBufferSize)
    {
        this.pageStreamFactory = requireNonNull(pageStreamFactory, "pageStreamFactory is null");
        this.usePagesSerde = usePagesSerde;
        this.writerSortBufferSize = requireNonNull(writerSortBufferSize, "writerSortBufferSize is null");
    }

    public long estimateWrittenBytesToOutputFile(long writtenBytesToTempFile)
    {
        if (usePagesSerde) {
            // See io.trino.plugin.hive.TestSortTempFileFactory.testWrittenBytes() for explanation of the scaling factor.
            return (long) (writtenBytesToTempFile * 0.8);
        }
        return writtenBytesToTempFile;
    }

    public PageStreamWriter createWriter(List<Type> types, TrinoFileSystem fileSystem, Location tempFile)
            throws IOException
    {
        if (usePagesSerde) {
            TrinoOutputFile outputFile = fileSystem.newOutputFile(tempFile);
            return pageStreamFactory.createWriter(outputFile.create());
        }
        return new TempFileWriter(types, fileSystem, tempFile);
    }

    public PageStreamReader createReader(List<Type> types, TrinoFileSystem fileSystem, Location tempFile, long fileSizeInBytes, int openSortFiles)
            throws IOException
    {
        if (usePagesSerde) {
            // The readChunkSize is computed by dividing writerSortBufferSize by openSortFiles,
            // ensuring the total buffering across all temp files stays within the writer’s sort buffer.
            // The chunk size is clamped to a minimum of 4 KB (to match typical filesystem block sizes)
            // and a maximum of 8 MB (to avoid excessively large reads).
            checkArgument(openSortFiles > 0, "openSortFiles must be greater than 0");
            long preferredChunkSize = writerSortBufferSize.toBytes() / openSortFiles;
            long clampedChunkSize = Math.min(Math.max(preferredChunkSize, MIN_READ_CHUNK_SIZE_IN_BYTES), MAX_READ_CHUNK_SIZE_IN_BYTES);
            DataSize readChunkSize = DataSize.of(clampedChunkSize, BYTE);

            TrinoInputFile inputFile = fileSystem.newInputFile(tempFile);
            ChunkedInputStream chunkedInputStream = new ChunkedInputStream(
                    inputFile.newInput(),
                    fileSizeInBytes,
                    toIntExact(readChunkSize.toBytes()));

            return pageStreamFactory.createReader(chunkedInputStream);
        }
        return new TempFileReader(types, fileSystem, tempFile);
    }
}
