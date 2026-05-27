/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocalDiskTier
{
    private static final long BUFFER_NODE_ID = 7;
    private static final DataSize DEFAULT_CAPACITY = DataSize.of(10, MEGABYTE);

    @TempDir
    Path tempDir;

    @Test
    public void testCreatesPerNodeSubdirectory()
            throws ExecutionException, InterruptedException
    {
        LocalDiskTier tier = createDiskTier(tempDir);
        tier.getDirectoryTracker().awaitPendingTasks();

        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID))).isDirectory();
    }

    @Test
    public void testCleansOrphanNodeDirectoriesFromPreviousDeployments()
            throws IOException, ExecutionException, InterruptedException
    {
        Files.createFile(tempDir.resolve(".disk-buffer"));
        Path orphanNodeDir = Files.createDirectory(tempDir.resolve("999"));
        Files.writeString(orphanNodeDir.resolve("stale.dat"), "left over from a previous buffer node id");

        LocalDiskTier tier = createDiskTier(tempDir);
        tier.getDirectoryTracker().awaitPendingTasks();

        assertThat(orphanNodeDir).doesNotExist();
    }

    @Test
    public void testRefusesToStartOnNonEmptyRootDirectoryWithoutOwnershipMarker()
            throws IOException
    {
        Files.writeString(tempDir.resolve("foreign.dat"), "not owned by the disk tier");

        assertThatThrownBy(() -> createDiskTier(tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing ownership marker");
    }

    @Test
    public void testFailsWhenRootDirectoryMissing()
    {
        Path missingRoot = tempDir.resolve("non-existent");

        assertThatThrownBy(() -> createDiskTier(missingRoot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("directory does not exist");
    }

    @Test
    public void testNormalizesNonCanonicalRootDirectory()
            throws IOException
    {
        Files.createFile(tempDir.resolve(".disk-buffer"));
        Path nested = Files.createDirectory(tempDir.resolve("nested"));
        Path nonCanonicalRoot = nested.resolve("..");

        LocalDiskTier diskTier = createDiskTier(nonCanonicalRoot);
        diskTier.createPartitionDirectory("exchange-1", 3);

        // If the root weren't normalized, the partition directory would not land at the canonical schema path.
        Path expected = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1").resolve("3");
        assertThat(expected).isDirectory();
    }

    @Test
    public void testValidateExchangeIdAcceptsSingleSegment()
    {
        assertThatCode(() -> LocalDiskTier.validateExchangeIdAsPathSegment(tempDir, "exchange-1"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateExchangeIdRejectsMultiSegment()
    {
        assertThatThrownBy(() -> LocalDiskTier.validateExchangeIdAsPathSegment(tempDir, "foo/bar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not resolve to a direct subdirectory");
    }

    @Test
    public void testValidateExchangeIdRejectsParentDirectoryEscape()
    {
        assertThatThrownBy(() -> LocalDiskTier.validateExchangeIdAsPathSegment(tempDir, ".."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not resolve to a direct subdirectory");
    }

    @Test
    public void testValidateExchangeIdRejectsCurrentDirectoryAlias()
    {
        assertThatThrownBy(() -> LocalDiskTier.validateExchangeIdAsPathSegment(tempDir, "."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not resolve to a direct subdirectory");
    }

    @Test
    public void testValidateExchangeIdRejectsNestedTraversalThatEscapesRoot()
    {
        assertThatThrownBy(() -> LocalDiskTier.validateExchangeIdAsPathSegment(tempDir, "foo/../.."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not resolve to a direct subdirectory");
    }

    @Test
    public void testPartitionDirectoryReturnsExpectedPath()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        Path partitionDirectory = diskTier.partitionDirectory("exchange-1", 3);

        Path expected = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1").resolve("3");
        assertThat(partitionDirectory).isEqualTo(expected);
    }

    @Test
    public void testPartitionDirectoryDoesNotTouchFilesystem()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        diskTier.partitionDirectory("exchange-1", 3);

        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1")).doesNotExist();
    }

    @Test
    public void testCreatePartitionDirectoryCreatesDirectoryAtExpectedPath()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        diskTier.createPartitionDirectory("exchange-1", 3);

        Path expected = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1").resolve("3");
        assertThat(expected).isDirectory();
    }

    @Test
    public void testCreatePartitionDirectoryIsIdempotent()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        diskTier.createPartitionDirectory("exchange-1", 3);
        diskTier.createPartitionDirectory("exchange-1", 3);

        Path expected = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1").resolve("3");
        assertThat(expected).isDirectory();
    }

    @Test
    public void testCreatePartitionDirectoryFailsWhenParentIsRegularFile()
            throws IOException
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);
        Path nodeDirectory = tempDir.resolve(String.valueOf(BUFFER_NODE_ID));
        Files.writeString(nodeDirectory.resolve("blocking-exchange"), "regular file");

        assertThatThrownBy(() -> diskTier.createPartitionDirectory("blocking-exchange", 3))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("failed to create partition directory");
    }

    @Test
    public void testExchangeDirectoryDeletedWhenLastChunkReleases()
            throws ExecutionException, InterruptedException
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);
        Path exchangeDirectory = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1");

        DiskChunkSlot slot1 = diskTier.tryReserveChunkSlot("exchange-1", 0, 1L, 1024).orElseThrow();
        DiskChunkSlot slot2 = diskTier.tryReserveChunkSlot("exchange-1", 1, 2L, 1024).orElseThrow();
        // Simulate the first-write materialization that the chunk's executor task would normally drive.
        slot1.materializeDirectory().run();
        slot2.materializeDirectory().run();

        slot1.diskRelease().run();
        slot1.lease().release();
        diskTier.getDirectoryTracker().awaitPendingTasks();
        assertThat(exchangeDirectory).isDirectory();

        slot2.diskRelease().run();
        slot2.lease().release();
        diskTier.getDirectoryTracker().awaitPendingTasks();
        assertThat(exchangeDirectory).doesNotExist();
    }

    @Test
    public void testTryReserveChunkSlotComputesPathWithoutTouchingFilesystem()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        DiskChunkSlot slot = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024).orElseThrow();

        Path expectedFile = tempDir.resolve(String.valueOf(BUFFER_NODE_ID))
                .resolve("exchange-1")
                .resolve("3")
                .resolve("chunk-42.data");
        assertThat(slot.file()).isEqualTo(expectedFile);
        // Partition directory materialization is lazy - the parent must not exist until the chunk's first write invokes materializeDirectory().
        assertThat(expectedFile.getParent()).doesNotExist();

        slot.materializeDirectory().run();
        assertThat(expectedFile.getParent()).isDirectory();

        slot.lease().release();
    }

    @Test
    public void testTryReserveChunkSlotReturnsEmptyWhenCapacityExhausted()
    {
        LocalDiskTier diskTier = createDiskTierWithCapacity(DataSize.ofBytes(1024));

        DiskChunkSlot first = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024).orElseThrow();
        Optional<DiskChunkSlot> second = diskTier.tryReserveChunkSlot("exchange-1", 3, 43L, 1024);

        assertThat(second).isEmpty();

        first.lease().release();

        // capacity available again after the first lease is released
        DiskChunkSlot third = diskTier.tryReserveChunkSlot("exchange-1", 3, 44L, 1024).orElseThrow();
        third.lease().release();
    }

    @Test
    public void testShouldRouteToDiskFollowsMemoryWatermark()
    {
        // Watermark at 70%; low watermark at 0% so routing deactivates only at 0% memory.
        LocalDiskTier diskTier = createDiskTierWithRouting(0.7, 0.0);

        // Below watermark - routing not yet active
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(69.9, 0, 0L, 0L))).isFalse();
        // At watermark - activates routing
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(70.0, 0, 0L, 0L))).isTrue();
        // Above watermark - routing remains active
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(100.0, 0, 0L, 0L))).isTrue();
    }

    @Test
    public void testShouldRouteToDiskOpenChunksBackpressure()
    {
        // maxOpenDiskChunks = 2; chunks above threshold with memory pressure
        LocalDiskTier diskTier = createDiskTierWithMaxOpenChunks(2);
        int chunkSize = (int) DataSize.of(64, KILOBYTE).toBytes();

        // No open chunks yet - routing allowed
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(100.0, 0, 0L, 0L))).isTrue();

        DiskChunkSlot slot1 = diskTier.tryReserveChunkSlot("exchange-1", 0, 1L, chunkSize).orElseThrow();
        DiskChunkSlot slot2 = diskTier.tryReserveChunkSlot("exchange-1", 0, 2L, chunkSize).orElseThrow();

        // Both slots open - limit reached, routing blocked
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(100.0, 2, 0L, 0L))).isFalse();

        // Release one slot - routing resumes
        slot1.diskRelease().run();
        slot1.lease().release();
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(100.0, 1, 0L, 0L))).isTrue();

        slot2.diskRelease().run();
        slot2.lease().release();
    }

    @Test
    public void testShouldRouteToDiskHysteresisStaysActiveUntilLowWatermark()
    {
        // High=0.7, low=0.5: routing must persist between 50–70% after activation.
        LocalDiskTier diskTier = createDiskTierWithRouting(0.7, 0.5);

        // Not active initially
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(65.0, 0, 0L, 0L))).isFalse();

        // Cross high watermark - activates
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(75.0, 0, 0L, 0L))).isTrue();

        // Drop below high but above low - stays active (hysteresis)
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(65.0, 0, 0L, 0L))).isTrue();
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(55.0, 0, 0L, 0L))).isTrue();
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(50.1, 0, 0L, 0L))).isTrue();

        // Drop below low watermark - deactivates
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(49.9, 0, 0L, 0L))).isFalse();

        // Stays inactive below low watermark
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(40.0, 0, 0L, 0L))).isFalse();

        // Re-activates when crossing high watermark again
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(70.0, 0, 0L, 0L))).isTrue();
    }

    @Test
    public void testShouldRouteToDiskHysteresisDoesNotActivateBelowHighWatermark()
    {
        LocalDiskTier diskTier = createDiskTierWithRouting(0.7, 0.5);

        // Memory bounces in the hysteresis zone but never crosses high - routing must not activate.
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(69.9, 0, 0L, 0L))).isFalse();
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(60.0, 0, 0L, 0L))).isFalse();
        assertThat(diskTier.shouldRouteToDisk(new LocalDiskTier.RoutingDecisionInputs(55.0, 0, 0L, 0L))).isFalse();
    }

    private static LocalDiskTier createDiskTier(Path rootDirectory)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(rootDirectory)
                .setCapacity(DEFAULT_CAPACITY);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }

    private LocalDiskTier createDiskTierWithCapacity(DataSize capacity)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(capacity);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }

    private LocalDiskTier createDiskTierWithRouting(double memoryHighWatermark, double memoryLowWatermark)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(DEFAULT_CAPACITY)
                .setMemoryHighWatermark(memoryHighWatermark)
                .setMemoryLowWatermark(memoryLowWatermark);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }

    private LocalDiskTier createDiskTierWithMaxOpenChunks(int maxOpenDiskChunks)
    {
        // Default watermarks (high=0.7, low=0.5); test always passes 100% memory so routing fires immediately.
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(DEFAULT_CAPACITY)
                .setMaxOpenDiskChunks(maxOpenDiskChunks);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config, new LocalDiskAllocator(config));
    }
}
