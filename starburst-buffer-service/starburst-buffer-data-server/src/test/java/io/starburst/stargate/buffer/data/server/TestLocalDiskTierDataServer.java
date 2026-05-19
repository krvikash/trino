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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.client.ChunkDeliveryMode.STANDARD;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

class TestLocalDiskTierDataServer
        extends BaseDataServerTest
{
    TestLocalDiskTierDataServer(@TempDir Path diskTierDir)
    {
        super(false, getLocalDiskProperties(diskTierDir));
    }

    @Test
    public void testLocalDiskTierAllocatesChunks()
    {
        String exchangeId = "exchange-disk-smoke";
        registerExchange(exchangeId, STANDARD);

        // Two 3kB pages overflow the 4kB chunk. Once that chunk closes,
        // exchangeCumulativeClosedBytes >= 1B and subsequent allocations land on disk.
        byte[] payload = new byte[3 * 1024];
        addDataPages(exchangeId, 0, 0, 0L, ImmutableListMultimap.of(0, wrappedBuffer(payload)));
        addDataPages(exchangeId, 1, 0, 1L, ImmutableListMultimap.of(0, wrappedBuffer(payload)));
        finishExchange(exchangeId);

        assertEventually(() ->
                assertThat(getChunkAllocationStats().diskChunks()).isGreaterThan(0));

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
                // 1B threshold: as soon as any chunk closes, subsequent allocations land on disk.
                .put("local-disk.memory-skip-threshold", "1B")
                .buildOrThrow();
    }
}
