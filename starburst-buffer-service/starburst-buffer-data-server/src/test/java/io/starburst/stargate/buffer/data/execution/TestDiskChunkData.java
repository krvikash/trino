/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.disk.DiskChunkSlot;
import io.starburst.stargate.buffer.data.disk.DiskSpaceLease;
import io.starburst.stargate.buffer.data.disk.LocalDiskAllocator;
import io.starburst.stargate.buffer.data.disk.LocalDiskTier;
import io.starburst.stargate.buffer.data.disk.LocalDiskTierConfig;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.execution.ChunkDataLease.CHUNK_SLICES_METADATA_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDiskChunkData
{
    private static final long BUFFER_NODE_ID = 7;
    private static final String EXCHANGE_ID = "exchange-1";
    private static final int PARTITION_ID = 0;
    private static final long CHUNK_ID = 42;
    private static final int CHUNK_SIZE_BYTES = 1024;
    private static final Slice PAGE = Slices.utf8Slice("payload");

    private final Executor executor = directExecutor();

    @TempDir
    Path tempDir;

    @Test
    public void testReleaseWithoutReadersDeletesFile()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.write(1, 0, PAGE));
        assertThat(slot.file()).exists();

        chunk.release();

        assertThat(slot.file()).doesNotExist();
    }

    @Test
    public void testGetOnNeverWrittenChunkThrows()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.close());

        assertThatThrownBy(chunk::get)
                .isInstanceOf(DataServerException.class)
                .hasMessageContaining("empty or never-materialized");

        chunk.release();
    }

    @Test
    public void testReleaseWithReaderHoldKeepsFileAlive()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.write(1, 0, PAGE));

        getFutureValue(chunk.close());
        ChunkDataLease lease = chunk.get();

        chunk.release();
        // writer dropped its reference, but the reader's lease still holds — destroy must not fire
        assertThat(slot.file()).exists();

        lease.release();
        // last reference dropped — destroy fires, file deleted
        assertThat(slot.file()).doesNotExist();
    }

    @Test
    public void testMultipleReadersAllReleaseBeforeDestroy()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.write(1, 0, PAGE));

        getFutureValue(chunk.close());
        ChunkDataLease lease1 = chunk.get();
        ChunkDataLease lease2 = chunk.get();
        ChunkDataLease lease3 = chunk.get();

        chunk.release();
        assertThat(slot.file()).exists();

        lease1.release();
        assertThat(slot.file()).exists();

        lease2.release();
        assertThat(slot.file()).exists();

        lease3.release();
        assertThat(slot.file()).doesNotExist();
    }

    @Test
    public void testReaderHoldDefersCapacityRefund()
    {
        LocalDiskTier tier = createDiskTier(DataSize.ofBytes(CHUNK_SIZE_BYTES));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.write(1, 0, PAGE));
        getFutureValue(chunk.close());
        ChunkDataLease lease = chunk.get();

        chunk.release();
        // destroy hasn't fired yet — lease still holds — capacity still consumed
        assertThat(tier.allocate(CHUNK_SIZE_BYTES)).isEmpty();

        lease.release();
        // destroy fired — capacity available again
        DiskSpaceLease nextLease = tier.allocate(CHUNK_SIZE_BYTES).orElseThrow();
        nextLease.release();
    }

    @Test
    public void testCloseIsIdempotent()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);

        assertThatCode(() -> getFutureValue(chunk.close())).doesNotThrowAnyException();
        assertThatCode(() -> getFutureValue(chunk.close())).doesNotThrowAnyException();

        chunk.release();
    }

    @Test
    public void testReleaseAfterCloseSucceeds()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));

        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);
        getFutureValue(chunk.write(1, 0, PAGE));

        getFutureValue(chunk.close());
        chunk.release();

        assertThat(slot.file()).doesNotExist();
    }

    @Test
    public void testWriteRoundTripsHeaderAndPageBytesToFile()
            throws Exception
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));
        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);

        Slice page = Slices.utf8Slice("hello-disk-chunk");
        int taskId = 12;
        int attemptId = 3;

        getFutureValue(chunk.write(taskId, attemptId, page));
        getFutureValue(chunk.close());

        ChunkDataLease lease = chunk.get();
        try {
            assertThat(lease.getNumDataPages()).isEqualTo(1);
            assertThat(lease.serializedSizeInBytes())
                    .isEqualTo(DATA_PAGE_HEADER_SIZE + page.length() + CHUNK_SLICES_METADATA_SIZE);

            int writtenBytes = DATA_PAGE_HEADER_SIZE + page.length();
            assertThat(Files.size(slot.file())).isGreaterThanOrEqualTo(writtenBytes);
            byte[] fileBytes = new byte[writtenBytes];
            try (InputStream in = Files.newInputStream(slot.file())) {
                int read = in.readNBytes(fileBytes, 0, writtenBytes);
                assertThat(read).isEqualTo(writtenBytes);
            }

            SliceInput input = Slices.wrappedBuffer(fileBytes).getInput();
            assertThat(input.readShort()).isEqualTo((short) taskId);
            assertThat(input.readByte()).isEqualTo((byte) attemptId);
            assertThat(input.readInt()).isEqualTo(page.length());
            assertThat(input.readSlice(page.length()).toStringUtf8()).isEqualTo(page.toStringUtf8());
        }
        finally {
            lease.release();
            chunk.release();
        }
    }

    @Test
    public void testWriteAdvancesLeaseSnapshotAcrossMultiplePages()
            throws Exception
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));
        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);

        Slice page1 = Slices.utf8Slice("first");
        Slice page2 = Slices.utf8Slice("second-page");

        getFutureValue(chunk.write(1, 0, page1));
        getFutureValue(chunk.write(2, 0, page2));
        getFutureValue(chunk.close());

        ChunkDataLease lease = chunk.get();
        try {
            assertThat(lease.getNumDataPages()).isEqualTo(2);
            int expectedDataBytes = (DATA_PAGE_HEADER_SIZE + page1.length()) + (DATA_PAGE_HEADER_SIZE + page2.length());
            assertThat(lease.serializedSizeInBytes()).isEqualTo(expectedDataBytes + CHUNK_SLICES_METADATA_SIZE);
            assertThat(Files.size(slot.file())).isGreaterThanOrEqualTo(expectedDataBytes);
        }
        finally {
            lease.release();
            chunk.release();
        }
    }

    @Test
    public void testGetReturnsLeaseWithFilePathAndLength()
    {
        LocalDiskTier tier = createDiskTier(DataSize.of(8, MEGABYTE));
        DiskChunkSlot slot = reserveSlot(tier, CHUNK_ID);
        DiskChunkData chunk = new DiskChunkData(executor, CHUNK_ID, CHUNK_SIZE_BYTES, false, slot);

        Slice page = Slices.utf8Slice("hello-disk-chunk");
        getFutureValue(chunk.write(1, 0, page));
        getFutureValue(chunk.close());

        DiskChunkDataLease lease = (DiskChunkDataLease) chunk.get();
        try {
            assertThat(lease.file()).isEqualTo(slot.file());
            assertThat(lease.length()).isEqualTo(DATA_PAGE_HEADER_SIZE + page.length());
            assertThat(lease.serializedSizeInBytes()).isEqualTo(DATA_PAGE_HEADER_SIZE + page.length() + CHUNK_SLICES_METADATA_SIZE);
            assertThat(lease.getNumDataPages()).isEqualTo(1);
        }
        finally {
            lease.release();
            chunk.release();
        }
    }

    private DiskChunkSlot reserveSlot(LocalDiskTier tier, long chunkId)
    {
        return tier.tryReserveChunkSlot(EXCHANGE_ID, PARTITION_ID, chunkId, CHUNK_SIZE_BYTES).orElseThrow();
    }

    private LocalDiskTier createDiskTier(DataSize capacity)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(capacity);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }
}
