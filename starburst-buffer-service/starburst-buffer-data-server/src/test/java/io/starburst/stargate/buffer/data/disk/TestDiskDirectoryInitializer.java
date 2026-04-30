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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.OWNERSHIP_MARKER;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.initializeDirectories;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDiskDirectoryInitializer
{
    @TempDir
    Path tempDir;

    private DiskDirectoryTracker tracker;

    @BeforeEach
    public void setUp()
    {
        tracker = new DiskDirectoryTracker();
    }

    @AfterEach
    public void tearDown()
    {
        tracker.shutdown();
    }

    @Test
    public void testFailsForNonExistentRootDirectory()
    {
        Path missingRoot = tempDir.resolve("non-existent");
        Path nodeDir = missingRoot.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(missingRoot, nodeDir, tracker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("root directory does not exist or is not a directory");
    }

    @Test
    public void testFailsForRootFilePath()
            throws IOException
    {
        Path rootAsFile = Files.writeString(tempDir.resolve("not-a-dir.txt"), "data");
        Path nodeDir = rootAsFile.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(rootAsFile, nodeDir, tracker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("root directory does not exist or is not a directory");
    }

    @Test
    public void testFailsWhenNodeDirectoryEscapesRoot()
    {
        Path unrelatedNodeDir = tempDir.getParent().resolve("elsewhere");

        assertThatThrownBy(() -> initializeDirectories(tempDir, unrelatedNodeDir, tracker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a subdirectory of");
    }

    @Test
    public void testClaimsEmptyRootDirectoryAndCreatesNodeDirectory()
            throws ExecutionException, InterruptedException
    {
        Path nodeDir = tempDir.resolve("0");

        Future<?> cleanupFuture = initializeDirectories(tempDir, nodeDir, tracker);
        cleanupFuture.get();

        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testRefusesToCleanRootDirectoryWithoutOwnershipMarker()
            throws IOException
    {
        Files.writeString(tempDir.resolve("foreign.dat"), "owned by something else");
        Path nodeDir = tempDir.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(tempDir, nodeDir, tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing ownership marker")
                .hasMessageContaining(OWNERSHIP_MARKER);

        assertThat(tempDir.resolve("foreign.dat")).exists();
        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).doesNotExist();
        assertThat(nodeDir).doesNotExist();
    }

    @Test
    public void testCleansRootDirectoryContentsWhenMarkerPresent()
            throws IOException, ExecutionException, InterruptedException
    {
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));
        Files.writeString(tempDir.resolve("stale.dat"), "stale");
        Path staleSubdir = Files.createDirectory(tempDir.resolve("exchange-abc"));
        Files.writeString(staleSubdir.resolve("chunk.dat"), "chunk data");
        Path nodeDir = tempDir.resolve("0");

        Future<?> cleanupFuture = initializeDirectories(tempDir, nodeDir, tracker);
        cleanupFuture.get();

        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(tempDir.resolve("stale.dat")).doesNotExist();
        assertThat(tempDir.resolve("exchange-abc")).doesNotExist();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testCleansOrphanNodeDirectoriesFromPreviousBufferNodes()
            throws IOException, ExecutionException, InterruptedException
    {
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));
        Path orphan = Files.createDirectory(tempDir.resolve("999"));
        Files.writeString(orphan.resolve("stale.dat"), "left over from a previous deployment");
        Path nodeDir = tempDir.resolve("0");

        Future<?> cleanupFuture = initializeDirectories(tempDir, nodeDir, tracker);
        cleanupFuture.get();

        assertThat(orphan).doesNotExist();
        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }
}
