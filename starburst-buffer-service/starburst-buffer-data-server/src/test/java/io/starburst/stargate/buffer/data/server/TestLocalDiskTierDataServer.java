/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsLocalSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolingStorageDriver.TRINO_FS;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

class TestLocalDiskTierDataServer
        extends BaseDataServerTest
{
    private final Path diskTierDir;

    TestLocalDiskTierDataServer(@TempDir Path diskTierDir)
    {
        super(false, getLocalDiskProperties(diskTierDir));
        this.diskTierDir = diskTierDir;
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsLocalSpooledChunkReader(diskTierDir);
    }

    @Test
    public void testLocalDiskTierAllocatesChunks()
    {
        String exchangeId = "exchange-disk-smoke";
        registerExchange(exchangeId, STANDARD);

        // Routing gate below sets watermark=0 and floor=1B, so every allocated chunk
        // takes the disk path regardless of current memory pressure.
        byte[] payload = new byte[3 * 1024];
        addDataPages(exchangeId, 0, 0, 0L, ImmutableListMultimap.of(0, wrappedBuffer(payload)));
        addDataPages(exchangeId, 1, 0, 1L, ImmutableListMultimap.of(0, wrappedBuffer(payload)));
        finishExchange(exchangeId);

        assertEventually(() ->
                assertThat(getDataServerStats().getDiskChunksOpened().getTotalCount()).isGreaterThan(0));

        removeExchange(exchangeId);
    }

    private static ImmutableMap<String, String> getLocalDiskProperties(Path diskTierDir)
    {
        return ImmutableMap.<String, String>builder()
                .put("chunk.target-size", DataSize.of(4, KILOBYTE).toString())
                .put("chunk.slice-size", DataSize.of(4, KILOBYTE).toString())
                .put("local-disk.enabled", "true")
                .put("local-disk.directory", diskTierDir.toString())
                .put("local-disk.capacity", DataSize.of(100, MEGABYTE).toString())
                // Force the disk path for this smoke test: any non-empty chunk passes the syscall
                // floor, and a 0% watermark routes every chunk to disk regardless of memory state.
                .put("local-disk.routing.memory-high-watermark", "0.0")
                .put("local-disk.routing.memory-low-watermark", "0.0")
                .put("spooling.local.location", diskTierDir.toString())
                .put("spooling.directory", "file:///spooling/")
                .put("spooling.storage-driver", TRINO_FS.toString())
                .put("testing.allow-local-spooling", "true")
                .buildOrThrow();
    }
}
