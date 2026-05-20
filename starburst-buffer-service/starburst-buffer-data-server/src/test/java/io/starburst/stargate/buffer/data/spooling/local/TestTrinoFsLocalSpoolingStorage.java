/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.DiskChunkDataLease;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.diskChunkDataLease;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsLocalSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsLocalSpoolingStorage;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoFsLocalSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    // @TempDir is per-method; AbstractTestSpoolingStorage wires the storage in @BeforeAll
    // (PER_CLASS), so the directory has to be created manually.
    private Path tempDir;

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        try {
            tempDir = Files.createTempDirectory("trino-fs-spooling-test-");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return createTrinoFsLocalSpoolingStorage(tempDir);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsLocalSpooledChunkReader(tempDir);
    }

    @Test
    public void testDiskLeaseChunkSpool()
            throws Exception
    {
        long bufferNodeId = 1L;
        String exchangeId = "exchange-disk-lease";
        long chunkId = 0L;

        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, Slices.utf8Slice("disk-page-0")),
                new DataPage(1, 0, Slices.utf8Slice("disk-page-1")));

        Path diskFile = tempDir.resolve("chunk-source.bin");
        DiskChunkDataLease lease = diskChunkDataLease(dataPages, diskFile);

        try {
            try (SpoolingStorage storage = createTrinoFsLocalSpoolingStorage(tempDir);
                    SpooledChunkReader reader = createTrinoFsLocalSpooledChunkReader(tempDir)) {
                Map<Long, SpooledChunk> spooledChunkMap = getFutureValue(storage.writeMergedChunks(
                        bufferNodeId,
                        exchangeId,
                        ImmutableMap.of(new Chunk(chunkId), lease),
                        lease.serializedSizeInBytes()));

                URI spooledUri = URI.create(spooledChunkMap.get(chunkId).location());
                Path spooledFile = tempDir.resolve(spooledUri.getPath().replaceFirst("^/", ""));
                assertThat(spooledFile).exists();

                assertThat(getFutureValue(reader.getDataPages(spooledChunkMap.get(chunkId))))
                        .containsExactlyElementsOf(dataPages);

                getFutureValue(storage.removeExchange(bufferNodeId, exchangeId));

                // verify spooled file deleted after exchange removal
                assertThat(spooledFile).doesNotExist();
            }
        }
        finally {
            lease.release();
        }
    }

    @AfterAll
    public void cleanupTempDir()
            throws IOException
    {
        if (tempDir != null) {
            MoreFiles.deleteRecursively(tempDir, RecursiveDeleteOption.ALLOW_INSECURE);
            tempDir = null;
        }
    }
}
