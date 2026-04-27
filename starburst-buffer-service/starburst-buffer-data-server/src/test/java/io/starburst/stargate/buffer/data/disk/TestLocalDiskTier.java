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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
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
        tier.awaitStartupCleanup();

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
        tier.awaitStartupCleanup();

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

    private static LocalDiskTier createDiskTier(Path rootDirectory)
    {
        LocalDiskTierConfig config = new LocalDiskTierConfig()
                .setDirectory(rootDirectory)
                .setCapacity(DEFAULT_CAPACITY);
        return new LocalDiskTier(new BufferNodeId(BUFFER_NODE_ID), config);
    }
}
