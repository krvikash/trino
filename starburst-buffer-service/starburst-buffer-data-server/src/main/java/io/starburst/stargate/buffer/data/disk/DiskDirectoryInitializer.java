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

import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

final class DiskDirectoryInitializer
{
    // Marker file proving that a directory is exclusively owned by the disk tier — guards against data loss from misconfiguration.
    static final String OWNERSHIP_MARKER = ".disk-buffer";

    private static final Logger log = Logger.get(DiskDirectoryInitializer.class);

    private DiskDirectoryInitializer() {}

    static Future<?> initializeDirectories(Path rootDirectory, Path nodeDirectory, DiskDirectoryTracker directoryTracker)
    {
        checkArgument(
                nodeDirectory.startsWith(rootDirectory) && !nodeDirectory.equals(rootDirectory),
                "nodeDirectory %s must be a subdirectory of rootDirectory %s",
                nodeDirectory,
                rootDirectory);
        checkIfRootDirectoryExists(rootDirectory);

        Path marker = rootDirectory.resolve(OWNERSHIP_MARKER);
        boolean cleanupNeeded = Files.exists(marker, NOFOLLOW_LINKS);
        if (!cleanupNeeded) {
            claimEmptyDirectory(rootDirectory, marker);
        }

        createDirectoryIfNeeded(nodeDirectory);

        if (!cleanupNeeded) {
            return CompletableFuture.completedFuture(null);
        }
        return directoryTracker.submitCleanup(() -> cleanStaleRootEntries(rootDirectory, marker, nodeDirectory));
    }

    private static void checkIfRootDirectoryExists(Path rootDirectory)
    {
        if (!isDirectory(rootDirectory)) {
            throw new IllegalArgumentException("Disk tier root directory does not exist or is not a directory: " + rootDirectory);
        }
    }

    private static void createDirectoryIfNeeded(Path nodeDirectory)
    {
        try {
            createDirectories(nodeDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to create buffer-node directory " + nodeDirectory, e);
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
                    deleteRecursively(entry, ALLOW_INSECURE);
                }
                else {
                    delete(entry);
                }
            }
            catch (IOException e) {
                log.warn(e, "Failed to clean stale file during startup: %s", entry);
            }
        }
    }

    private static void claimEmptyDirectory(Path directory, Path marker)
    {
        if (!isEmpty(directory)) {
            throw new IllegalStateException(
                    "Refusing to clean disk tier directory: missing ownership marker '" + OWNERSHIP_MARKER + "' and directory is not empty: " + directory);
        }
        try {
            Files.createFile(marker);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create disk tier ownership marker: " + marker, e);
        }
    }

    private static boolean isEmpty(Path directory)
    {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            return !entries.iterator().hasNext();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to list disk tier directory: " + directory, e);
        }
    }
}
