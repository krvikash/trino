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
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class DiskDirectoryLock
        implements AutoCloseable
{
    private static final Logger log = Logger.get(DiskDirectoryLock.class);

    // JVM-global registry of lock files held by this process.
    // Checked before opening a second FileChannel to the same file - closing any fd to a file
    // releases all fcntl locks held by the process on that file, so we must
    // never open a second channel to a file we already locked.
    private static final Set<Path> heldLockFiles = ConcurrentHashMap.newKeySet();

    private final Path lockFile;
    private final FileChannel channel;
    private final FileLock lock;
    private final AtomicBoolean closed = new AtomicBoolean();

    DiskDirectoryLock(Path lockFile, FileChannel channel, FileLock lock)
    {
        this.lockFile = registryKey(lockFile);
        this.channel = channel;
        this.lock = lock;
        heldLockFiles.add(this.lockFile);
    }

    static boolean isHeldByThisJvm(Path lockFile)
    {
        return heldLockFiles.contains(registryKey(lockFile));
    }

    @PreDestroy
    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            boolean released = false;
            boolean channelClosed = false;
            try {
                lock.release();
                released = true;
            }
            catch (IOException e) {
                log.warn(e, "Failed to release disk directory lock: %s", lock.toString());
            }
            try {
                channel.close();
                channelClosed = true;
            }
            catch (IOException e) {
                log.warn(e, "Failed to close disk directory lock channel: %s", channel.toString());
            }
            if (released || channelClosed) {
                heldLockFiles.remove(lockFile);
            }
        }
    }

    // Canonicalize so equivalent paths (./, ../, symlinks) map to the same registry key.
    private static Path registryKey(Path lockFile)
    {
        try {
            return lockFile.toRealPath();
        }
        catch (IOException ignored) {
            return lockFile.toAbsolutePath().normalize();
        }
    }
}
