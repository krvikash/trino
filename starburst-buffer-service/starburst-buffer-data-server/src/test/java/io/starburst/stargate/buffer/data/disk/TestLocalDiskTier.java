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

import static io.airlift.units.DataSize.Unit.BYTE;
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
                .hasMessageContaining("root directory does not exist or is not a directory");
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
        DataSize threshold = DataSize.of(1, BYTE);
        LocalDiskTier diskTier = createDiskTierWithThreshold(threshold);
        Path exchangeDirectory = tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1");

        DiskChunkSlot slot1 = diskTier.tryReserveChunkSlot("exchange-1", 0, 1L, 1024, Long.MAX_VALUE).orElseThrow();
        DiskChunkSlot slot2 = diskTier.tryReserveChunkSlot("exchange-1", 1, 2L, 1024, Long.MAX_VALUE).orElseThrow();

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
    public void testTryReserveChunkSlotReturnsEmptyWhenThresholdNotConfigured()
    {
        LocalDiskTier diskTier = createDiskTier(tempDir);

        Optional<DiskChunkSlot> slot = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024, Long.MAX_VALUE);

        assertThat(slot).isEmpty();
        // no side effects on disk
        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1")).doesNotExist();
    }

    @Test
    public void testTryReserveChunkSlotReturnsEmptyBelowThreshold()
    {
        DataSize threshold = DataSize.of(1, MEGABYTE);
        LocalDiskTier diskTier = createDiskTierWithThreshold(threshold);

        Optional<DiskChunkSlot> slot = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024, threshold.toBytes() - 1);

        assertThat(slot).isEmpty();
        assertThat(tempDir.resolve(String.valueOf(BUFFER_NODE_ID)).resolve("exchange-1")).doesNotExist();
    }

    @Test
    public void testTryReserveChunkSlotMaterializesPathAndDirectoryAtOrAboveThreshold()
    {
        DataSize threshold = DataSize.of(1, MEGABYTE);
        LocalDiskTier diskTier = createDiskTierWithThreshold(threshold);

        DiskChunkSlot slot = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024, threshold.toBytes()).orElseThrow();

        Path expectedFile = tempDir.resolve(String.valueOf(BUFFER_NODE_ID))
                .resolve("exchange-1")
                .resolve("3")
                .resolve("chunk-42.data");
        assertThat(slot.file()).isEqualTo(expectedFile);
        assertThat(expectedFile.getParent()).isDirectory();

        slot.lease().release();
    }

    @Test
    public void testTryReserveChunkSlotReturnsEmptyWhenCapacityExhausted()
    {
        DataSize threshold = DataSize.of(1, BYTE);
        LocalDiskTier diskTier = createDiskTierWithThresholdAndCapacity(threshold, DataSize.ofBytes(1024));

        DiskChunkSlot first = diskTier.tryReserveChunkSlot("exchange-1", 3, 42L, 1024, Long.MAX_VALUE).orElseThrow();
        Optional<DiskChunkSlot> second = diskTier.tryReserveChunkSlot("exchange-1", 3, 43L, 1024, Long.MAX_VALUE);

        assertThat(second).isEmpty();

        first.lease().release();

        // capacity available again after the first lease is released
        DiskChunkSlot third = diskTier.tryReserveChunkSlot("exchange-1", 3, 44L, 1024, Long.MAX_VALUE).orElseThrow();
        third.lease().release();
    }

    private static LocalDiskTier createDiskTier(Path rootDirectory)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(rootDirectory)
                .setCapacity(DEFAULT_CAPACITY);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config);
    }

    private LocalDiskTier createDiskTierWithThreshold(DataSize threshold)
    {
        return createDiskTierWithThresholdAndCapacity(threshold, DEFAULT_CAPACITY);
    }

    private LocalDiskTier createDiskTierWithThresholdAndCapacity(DataSize threshold, DataSize capacity)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(tempDir)
                .setCapacity(capacity)
                .setMemorySkipThreshold(threshold);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config);
    }
}
