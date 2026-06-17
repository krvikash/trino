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

import io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.DirectoryOwnership;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.OWNERSHIP_LOCK;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.OWNERSHIP_MARKER;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.acquireDirectoryLock;
import static io.starburst.stargate.buffer.data.disk.DiskDirectoryInitializer.initializeDirectories;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
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

        assertThatThrownBy(() -> initializeDirectories(missingRoot, nodeDir, tracker, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("directory does not exist");
    }

    @Test
    public void testFailsForRootFilePath()
            throws IOException
    {
        Path rootAsFile = Files.writeString(tempDir.resolve("not-a-dir.txt"), "data");
        Path nodeDir = rootAsFile.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(rootAsFile, nodeDir, tracker, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a directory");
    }

    @Test
    public void testFailsWhenNodeDirectoryEscapesRoot()
    {
        Path unrelatedNodeDir = tempDir.getParent().resolve("elsewhere");

        assertThatThrownBy(() -> initializeDirectories(tempDir, unrelatedNodeDir, tracker, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a subdirectory of");
    }

    @Test
    public void testClaimsEmptyRootDirectoryAndCreatesNodeDirectory()
            throws ExecutionException, InterruptedException
    {
        Path nodeDir = tempDir.resolve("0");

        DirectoryOwnership initResult = initializeDirectories(tempDir, nodeDir, tracker, false);
        initResult.cleanup().get();
        initResult.directoryLock().close();

        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testRefusesToCleanRootDirectoryWithoutOwnershipMarker()
            throws IOException
    {
        Files.writeString(tempDir.resolve("foreign.dat"), "owned by something else");
        Path nodeDir = tempDir.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(tempDir, nodeDir, tracker, false))
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

        DirectoryOwnership initResult = initializeDirectories(tempDir, nodeDir, tracker, false);
        initResult.cleanup().get();
        initResult.directoryLock().close();

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

        DirectoryOwnership initResult = initializeDirectories(tempDir, nodeDir, tracker, false);
        initResult.cleanup().get();
        initResult.directoryLock().close();

        assertThat(orphan).doesNotExist();
        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testAllowDirectoryCreationCreatesNonExistentRoot()
            throws ExecutionException, InterruptedException
    {
        Path missingRoot = tempDir.resolve("auto-created");
        Path nodeDir = missingRoot.resolve("0");

        DirectoryOwnership initResult = initializeDirectories(missingRoot, nodeDir, tracker, true);
        initResult.cleanup().get();
        initResult.directoryLock().close();

        assertThat(missingRoot).isDirectory();
        assertThat(missingRoot.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testAllowDirectoryCreationWorksWhenRootAlreadyExists()
            throws ExecutionException, InterruptedException
    {
        Path nodeDir = tempDir.resolve("0");

        DirectoryOwnership initResult = initializeDirectories(tempDir, nodeDir, tracker, true);
        initResult.cleanup().get();
        initResult.directoryLock().close();

        assertThat(tempDir.resolve(OWNERSHIP_MARKER)).isRegularFile();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testAllowDirectoryCreationStillFailsForFilePath()
            throws IOException
    {
        Path rootAsFile = Files.writeString(tempDir.resolve("not-a-dir.txt"), "data");
        Path nodeDir = rootAsFile.resolve("0");

        assertThatThrownBy(() -> initializeDirectories(rootAsFile, nodeDir, tracker, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a directory");
    }

    @Test
    public void testLockPreventsSecondInstanceFromClaimingSameRoot()
    {
        try (DiskDirectoryLock firstLock = acquireDirectoryLock(tempDir)) {
            assertThat(firstLock).isNotNull();
            assertThatThrownBy(() -> acquireDirectoryLock(tempDir))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(tempDir.toString());
        }
    }

    @Test
    public void testLockIsRecoverableAfterOwnerDies()
            throws IOException
    {
        try (FileChannel channel = FileChannel.open(tempDir.resolve(OWNERSHIP_LOCK), CREATE, WRITE)) {
            assertThat(channel.tryLock()).isNotNull();
        }

        try (DiskDirectoryLock lock = acquireDirectoryLock(tempDir)) {
            assertThat(lock).isNotNull();
        }
    }

    @Test
    public void testLockIsReleasedOnCleanShutdown()
    {
        DiskDirectoryLock firstLock = acquireDirectoryLock(tempDir);
        firstLock.close();

        try (DiskDirectoryLock secondLock = acquireDirectoryLock(tempDir)) {
            assertThat(secondLock).isNotNull();
        }
    }

    @Test
    public void testCleanupSkipsLiveNodeDirectories()
            throws ExecutionException, InterruptedException
    {
        // Simulate two live nodes sharing the same root (embedded buffer)
        Path nodeDir1 = tempDir.resolve("1");
        Path nodeDir2 = tempDir.resolve("2");

        DirectoryOwnership result1 = initializeDirectories(tempDir, nodeDir1, tracker, false);
        result1.cleanup().get();

        // Node 2 starts after marker is present and it triggers cleanup path
        DirectoryOwnership result2 = initializeDirectories(tempDir, nodeDir2, tracker, false);
        result2.cleanup().get();

        // Node 1's directory must survive node 2's cleanup.
        assertThat(nodeDir1).isDirectory();
        assertThat(nodeDir2).isDirectory();

        result1.directoryLock().close();
        result2.directoryLock().close();
    }

    @Test
    public void testCleanupDeletesStaleNodeDirectories()
            throws IOException, ExecutionException, InterruptedException
    {
        // Simulate a previous run's node directory (lock file exists but no live holder).
        Path staleNodeDir = tempDir.resolve("99");
        Files.createDirectories(staleNodeDir);
        Files.writeString(staleNodeDir.resolve("chunk.dat"), "stale data");
        // Create lock file but don't hold the lock (simulates stale process).
        Files.createFile(staleNodeDir.resolve(OWNERSHIP_LOCK));
        // Place the ownership marker so the new node takes the cleanup path.
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));

        Path nodeDir = tempDir.resolve("1");
        DirectoryOwnership result = initializeDirectories(tempDir, nodeDir, tracker, false);
        result.cleanup().get();
        result.directoryLock().close();

        assertThat(staleNodeDir).doesNotExist();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testCleanupWithMixOfLiveAndStaleNodeDirectories()
            throws IOException, ExecutionException, InterruptedException
    {
        // Stale node - lock file present but no holder
        Path staleNodeDir = tempDir.resolve("99");
        Files.createDirectories(staleNodeDir);
        Files.createFile(staleNodeDir.resolve(OWNERSHIP_LOCK));

        // Place ownership marker
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));

        // Live node 1 initializes first
        Path nodeDir1 = tempDir.resolve("1");
        DirectoryOwnership result1 = initializeDirectories(tempDir, nodeDir1, tracker, false);
        result1.cleanup().get();

        // Live node 2 initializes - sees marker, triggers cleanup, must skip node 1 and delete stale node.
        Path nodeDir2 = tempDir.resolve("2");
        DirectoryOwnership result2 = initializeDirectories(tempDir, nodeDir2, tracker, false);
        result2.cleanup().get();

        assertThat(nodeDir1).isDirectory();
        assertThat(nodeDir2).isDirectory();
        assertThat(staleNodeDir).doesNotExist();

        result1.directoryLock().close();
        result2.directoryLock().close();
    }

    @Test
    public void testCleanupHoldsLockThroughDeletion()
            throws IOException, ExecutionException, InterruptedException
    {
        // Simulating the race: a node directory exists but its lock is not yet held
        // (created by a racing initializer that hasn't called acquireDirectoryLock yet).
        // Cleanup must atomically own the directory before deleting it so that the racing
        // initializer observes the contention and fails rather than proceeding with a directory that is being deleted underneath it.
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));
        Path racingNodeDir = tempDir.resolve("99");
        Files.createDirectories(racingNodeDir);

        // No lock file yet - simulates the window between createDirectoryIfNeeded and acquireDirectoryLock.
        // Cleanup acquires the stale lock and deletes the directory.
        Path nodeDir = tempDir.resolve("1");
        DirectoryOwnership result = initializeDirectories(tempDir, nodeDir, tracker, false);
        result.cleanup().get();
        result.directoryLock().close();

        assertThat(racingNodeDir).doesNotExist();
        assertThat(nodeDir).isDirectory();
    }

    @Test
    public void testCleanupDoesNotReleaseExistingLockOfSameJvmNode()
            throws IOException, ExecutionException, InterruptedException
    {
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));
        Path liveNodeDir = tempDir.resolve("99");
        Files.createDirectories(liveNodeDir);

        DiskDirectoryLock liveLock = acquireDirectoryLock(liveNodeDir);

        // Another node in the same JVM runs cleanup. Must skip liveNodeDir without opening a second channel to its lock file
        Path nodeDir = tempDir.resolve("1");
        DirectoryOwnership result = initializeDirectories(tempDir, nodeDir, tracker, false);
        result.cleanup().get();
        result.directoryLock().close();

        assertThat(liveNodeDir).isDirectory();
        // If cleanup accidentally released the lock via fd-close, this second acquisition
        // would succeed instead of throwing — proving the original lock was lost.
        assertThatThrownBy(() -> acquireDirectoryLock(liveNodeDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Another buffer-data-server process");

        liveLock.close();
    }

    @Test
    public void testCleanupCannotDeleteDirectoryWithHeldLock()
            throws IOException, ExecutionException, InterruptedException
    {
        // If the racing initializer wins the lock race, cleanup must skip the directory.
        Files.createFile(tempDir.resolve(OWNERSHIP_MARKER));
        Path concurrentNodeDir = tempDir.resolve("99");
        Files.createDirectories(concurrentNodeDir);

        // Racing initializer acquires the lock first.
        DiskDirectoryLock concurrentLock = acquireDirectoryLock(concurrentNodeDir);

        Path nodeDir = tempDir.resolve("1");
        DirectoryOwnership result = initializeDirectories(tempDir, nodeDir, tracker, false);
        result.cleanup().get();
        result.directoryLock().close();

        // Cleanup must have skipped the locked directory.
        assertThat(concurrentNodeDir).isDirectory();
        concurrentLock.close();
    }
}
