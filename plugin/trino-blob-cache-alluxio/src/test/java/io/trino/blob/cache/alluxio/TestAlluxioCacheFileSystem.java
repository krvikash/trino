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
package io.trino.blob.cache.alluxio;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.blob.cache.alluxio.TestingBlobCache.testingBlobCache;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAlluxioCacheFileSystem
        extends AbstractTestTrinoFileSystem
{
    private MemoryFileSystem memoryFileSystem;
    private CacheFileSystem fileSystem;
    private AlluxioCache cache;
    private Path tempDirectory;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        Path cacheDirectory = tempDirectory.resolve("cache");
        Files.createDirectory(cacheDirectory);
        AlluxioCacheConfig configuration = new AlluxioCacheConfig()
                .setCacheDirectories(ImmutableList.of(cacheDirectory.toAbsolutePath().toString()))
                .setCachePageSize(DataSize.valueOf("32003B"))
                .disableTTL()
                .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("100MB")));
        memoryFileSystem = new IncompleteStreamMemoryFileSystem();
        cache = new AlluxioCache(noopTracer(), configuration);
        fileSystem = new CacheFileSystem(memoryFileSystem, testingBlobCache(cache, new AlluxioCacheStats()), new DefaultCacheKeyProvider());
    }

    @AfterAll
    void afterAll()
            throws IOException
    {
        cleanupFiles(tempDirectory);
        Files.delete(tempDirectory);
    }

    private void cleanupFiles(Path directory)
            throws IOException
    {
        // tests will leave directories
        try (Stream<Path> walk = Files.walk(directory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (!path.equals(directory)) {
                    Files.delete(path);
                }
            }
        }
    }

    @Test
    void testReadFullyPastEndOfFile()
            throws IOException
    {
        Location location = getRootLocation().appendPath("testReadFullyPastEndOfFile");
        byte[] content = new byte[64];
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }
        // cold-cache positioned read starting beyond the file length takes the external read path
        try (TrinoInput input = fileSystem.newInputFile(location).newInput()) {
            assertThatThrownBy(() -> input.readFully(content.length + 16, new byte[16], 0, 16))
                    .isInstanceOf(EOFException.class);
        }
        fileSystem.deleteFile(location);
    }

    @Test
    void testReadFullyIntoByteBufferFromCache()
            throws IOException
    {
        Location location = getRootLocation().appendPath("testReadFullyIntoByteBufferFromCache");
        byte[] content = sequentialBytes(128 * 1024);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        int position = 1_000;
        int length = 50_000;
        try (TrinoInput input = fileSystem.newInputFile(location).newInput()) {
            // warm the whole file into the cache
            input.readFully(0, new byte[content.length], 0, content.length);

            // cache hit fills a direct buffer without a heap intermediate
            ByteBuffer direct = ByteBuffer.allocateDirect(length);
            input.readFully(position, direct);
            assertThat(direct.position()).isEqualTo(direct.limit());
            assertBufferMatches(direct, content, position, length);

            // cache hit into a heap buffer
            ByteBuffer heap = ByteBuffer.allocate(length);
            input.readFully(position, heap);
            assertThat(heap.position()).isEqualTo(heap.limit());
            assertBufferMatches(heap, content, position, length);
        }
        fileSystem.deleteFile(location);
    }

    @Test
    void testReadFullyIntoByteBufferAcrossCacheBoundary()
            throws IOException
    {
        Location location = getRootLocation().appendPath("testReadFullyIntoByteBufferAcrossCacheBoundary");
        byte[] content = sequentialBytes(200 * 1024);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        try (TrinoInput input = fileSystem.newInputFile(location).newInput()) {
            // warm only a prefix, leaving later pages uncached
            input.readFully(0, new byte[40_000], 0, 40_000);

            // read spans the cached prefix and the uncached remainder
            int length = 120_000;
            ByteBuffer buffer = ByteBuffer.allocate(length);
            input.readFully(0, buffer);
            assertThat(buffer.position()).isEqualTo(buffer.limit());
            assertBufferMatches(buffer, content, 0, length);
        }
        fileSystem.deleteFile(location);
    }

    private static byte[] sequentialBytes(int size)
    {
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) i;
        }
        return content;
    }

    private static void assertBufferMatches(ByteBuffer buffer, byte[] expected, int offset, int length)
    {
        buffer.flip();
        for (int i = 0; i < length; i++) {
            assertThat(buffer.get()).isEqualTo(expected[offset + i]);
        }
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("memory://");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        assertThat(memoryFileSystem.isEmpty()).isTrue();
    }
}
