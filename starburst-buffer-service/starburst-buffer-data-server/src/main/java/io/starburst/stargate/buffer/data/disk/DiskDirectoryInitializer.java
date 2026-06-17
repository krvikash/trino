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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

final class DiskDirectoryInitializer
{
    // Marker file proving that a directory is exclusively owned by the disk tier - guards against data loss from misconfiguration.
    static final String OWNERSHIP_MARKER = ".disk-buffer";
    // OS-level lock file held for the process lifetime; prevents two live processes from claiming the same node directory.
    static final String OWNERSHIP_LOCK = ".disk-buffer.lock";

    private static final Logger log = Logger.get(DiskDirectoryInitializer.class);

    private DiskDirectoryInitializer() {}

    record DirectoryOwnership(DiskDirectoryLock directoryLock, Future<?> cleanup) {}

    static DirectoryOwnership initializeDirectories(Path rootDirectory, Path nodeDirectory, DiskDirectoryTracker directoryTracker, boolean allowDirectoryCreation)
    {
        checkArgument(
                nodeDirectory.startsWith(rootDirectory) && !nodeDirectory.equals(rootDirectory),
                "nodeDirectory %s must be a subdirectory of rootDirectory %s",
                nodeDirectory,
                rootDirectory);
        checkIfRootDirectoryExists(rootDirectory, allowDirectoryCreation);

        boolean nodeDirectoryCreated = createDirectoryIfNeeded(nodeDirectory);
        DiskDirectoryLock directoryLock = acquireDirectoryLock(nodeDirectory);
        try {
            Path marker = rootDirectory.resolve(OWNERSHIP_MARKER);
            boolean cleanupNeeded = exists(marker, NOFOLLOW_LINKS);
            if (!cleanupNeeded) {
                claimEmptyDirectory(rootDirectory, marker);
            }

            if (!cleanupNeeded) {
                return new DirectoryOwnership(directoryLock, CompletableFuture.completedFuture(null));
            }
            return new DirectoryOwnership(directoryLock, directoryTracker.submitCleanup(() -> cleanStaleRootEntries(rootDirectory, marker, nodeDirectory)));
        }
        catch (RuntimeException e) {
            directoryLock.close();
            if (nodeDirectoryCreated) {
                try {
                    deleteRecursively(nodeDirectory, ALLOW_INSECURE);
                }
                catch (IOException cleanupException) {
                    e.addSuppressed(cleanupException);
                }
            }
            throw e;
        }
    }

    @VisibleForTesting
    static DiskDirectoryLock acquireDirectoryLock(Path directory)
    {
        Path lockFile = directory.resolve(OWNERSHIP_LOCK);
        try {
            return tryOpenAndLock(lockFile).orElseThrow(() -> new IllegalStateException(format(
                    "Another buffer-data-server process owns the disk tier node directory: %s. " +
                            "Each process must use a distinct local-disk.directory.",
                    directory)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to acquire disk directory lock: %s", lockFile), e);
        }
    }

    private static void closeQuietly(FileChannel channel, Throwable primaryException)
    {
        try {
            channel.close();
        }
        catch (IOException e) {
            if (primaryException != null) {
                primaryException.addSuppressed(e);
            }
            else {
                log.warn(e, "Failed to close disk directory lock channel");
            }
        }
    }

    private static void checkIfRootDirectoryExists(Path rootDirectory, boolean allowDirectoryCreation)
    {
        if (isDirectory(rootDirectory)) {
            return;
        }

        if (exists(rootDirectory)) {
            throw new IllegalArgumentException(format("Disk tier root %s is not a directory", rootDirectory));
        }

        if (allowDirectoryCreation) {
            try {
                createDirectories(rootDirectory);
                return;
            }
            catch (IOException e) {
                throw new UncheckedIOException(format("Failed to create disk tier root %s", rootDirectory), e);
            }
        }
        throw new IllegalArgumentException(format("Disk tier root %s directory does not exist", rootDirectory));
    }

    private static boolean createDirectoryIfNeeded(Path nodeDirectory)
    {
        try {
            createDirectory(nodeDirectory);
            return true;
        }
        catch (FileAlreadyExistsException e) {
            if (!isDirectory(nodeDirectory)) {
                throw new IllegalArgumentException(format("Node directory path exists as a non-directory file: %s", nodeDirectory));
            }
            return false;
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to create buffer-node directory %s", nodeDirectory), e);
        }
    }

    private static void cleanStaleRootEntries(Path rootDirectory, Path marker, Path nodeDirectory)
    {
        List<Path> staleEntries = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(rootDirectory)) {
            for (Path entry : entries) {
                if (entry.equals(marker) || entry.equals(nodeDirectory)) {
                    continue;
                }
                staleEntries.add(entry);
            }
        }
        catch (IOException e) {
            log.warn(e, "Failed to list disk tier directory for cleanup: %s", rootDirectory);
            return;
        }
        if (staleEntries.isEmpty()) {
            return;
        }
        log.info("Cleaning %d stale entries from disk tier root directory: %s", staleEntries.size(), rootDirectory);
        for (Path entry : staleEntries) {
            try {
                if (isDirectory(entry)) {
                    // Hold the lock through deletion so a concurrently initializing node that
                    // created the directory but hasn't locked it yet will fail on tryLock rather
                    // than proceed with a directory that is being deleted underneath it.
                    Optional<DiskDirectoryLock> staleLock = tryAcquireStaleDirectoryLock(entry);
                    if (staleLock.isEmpty()) {
                        log.info("Skipping directory during cleanup - lock held by live process: %s", entry);
                        continue;
                    }
                    log.info("Deleting stale node directory - acquired exclusive lock (no live process holds it): %s", entry);
                    try (DiskDirectoryLock _ = staleLock.get()) {
                        deleteRecursively(entry, ALLOW_INSECURE);
                    }
                }
                else {
                    // Non-directory entries are always stale - only node directories can be live.
                    log.debug("Deleting stale non-directory entry: %s", entry);
                    delete(entry);
                }
            }
            catch (IOException e) {
                log.warn(e, "Failed to clean stale entry during startup: %s", entry);
            }
        }
    }

    // Returns a held lock if the directory is stale (safe to delete), empty Optional if it is live.
    // Holding the returned lock through deletion prevents a racing initializer from locking
    // the same directory after the stale check but before the delete.
    private static Optional<DiskDirectoryLock> tryAcquireStaleDirectoryLock(Path nodeDirectory)
    {
        Path lockFile = nodeDirectory.resolve(OWNERSHIP_LOCK);
        try {
            return tryOpenAndLock(lockFile);
        }
        catch (IOException e) {
            log.warn(e, "Failed to acquire lock for stale-check: %s", lockFile);
            return Optional.empty();
        }
    }

    // Opens (or creates) the lock file and attempts a non-blocking lock.
    // Returns empty if the lock is already held (by this JVM or another process).
    private static Optional<DiskDirectoryLock> tryOpenAndLock(Path lockFile)
            throws IOException
    {
        log.debug("Attempting to open and lock: %s", lockFile);
        // Guard: do NOT open a second FileChannel to a file already locked by this JVM.
        // Closing any fd to a file releases all POSIX advisory (fcntl) locks the process holds
        // on it - including locks held via a different, still-open channel.
        if (DiskDirectoryLock.isHeldByThisJvm(lockFile)) {
            log.debug("Lock already held by this JVM - skipping open to avoid releasing existing lock: %s", lockFile);
            return Optional.empty();
        }
        FileChannel channel = FileChannel.open(lockFile, CREATE, WRITE);
        FileLock fileLock;
        try {
            fileLock = channel.tryLock();
        }
        catch (OverlappingFileLockException e) {
            // Can still happen due to a race with another thread in this JVM that opened and locked
            // the file between the isHeldByThisJvm check above and the tryLock call here.
            log.warn("tryLock threw OverlappingFileLockException - lock held by this JVM: %s", lockFile);
            fileLock = null;
        }
        catch (IOException e) {
            closeQuietly(channel, e);
            throw e;
        }
        if (fileLock == null) {
            log.debug("tryLock returned null - lock held by another process: %s", lockFile);
            closeQuietly(channel, null);
            return Optional.empty();
        }
        log.debug("tryLock succeeded - lock acquired: %s", lockFile);
        return Optional.of(new DiskDirectoryLock(lockFile, channel, fileLock));
    }

    private static void claimEmptyDirectory(Path directory, Path marker)
    {
        Path lockFile = directory.resolve(OWNERSHIP_LOCK);
        if (!isEmpty(directory, lockFile)) {
            throw new IllegalStateException(
                    format("Refusing to clean disk tier directory: missing ownership marker '%s' and directory is not empty: %s", OWNERSHIP_MARKER, directory));
        }
        try {
            Files.createFile(marker);
        }
        catch (FileAlreadyExistsException ignored) {
            // Another node in the same process already claimed the directory concurrently
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to create disk tier ownership marker: %s", marker), e);
        }
    }

    private static boolean isEmpty(Path directory, Path ignoredEntry)
    {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                // Subdirectories are treated as node directories (live or from previous runs) and not counted as foreign content.
                // NOTE: this means a pre-existing foreign subdirectory (e.g. lost+found) would not block ownership claim.
                if (entry.equals(ignoredEntry) || isDirectory(entry)) {
                    continue;
                }
                return false;
            }
            return true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Failed to list disk tier directory: %s", directory), e);
        }
    }
}
