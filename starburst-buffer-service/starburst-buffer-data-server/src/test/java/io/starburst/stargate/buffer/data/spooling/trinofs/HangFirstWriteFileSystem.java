/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Blocks the first write until the calling thread is interrupted and delegates every later call. Used to verify that a
 * draining attempt whose write was cancelled leaves its chunks behind for the next attempt to spool successfully.
 */
class HangFirstWriteFileSystem
        implements TrinoFileSystem
{
    final CountDownLatch interrupted = new CountDownLatch(1);

    private final TrinoFileSystem delegate;
    private final AtomicBoolean firstWrite = new AtomicBoolean(true);

    HangFirstWriteFileSystem(TrinoFileSystem delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return delegate.newInputFile(location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return delegate.newInputFile(location, length);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return delegate.newInputFile(location, length, lastModified);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        if (firstWrite.compareAndSet(true, false)) {
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("unreachable");
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
                throw new RuntimeException(new IOException(e));
            }
        }
        return delegate.newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        delegate.deleteFile(location);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        delegate.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return delegate.directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        delegate.createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        delegate.renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return delegate.listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }
}
