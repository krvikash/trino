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

/**
 * Blocks every TrinoFileSystem write/delete call until the calling thread is interrupted. Used by timeout tests
 * to simulate a hung blocking I/O call deterministically.
 */
class HangingFileSystem
        implements TrinoFileSystem
{
    final CountDownLatch interrupted = new CountDownLatch(1);

    HangingFileSystem() {}

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        try {
            blockUntilInterrupted();
            throw new AssertionError("unreachable");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        blockUntilInterrupted();
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private void blockUntilInterrupted()
            throws IOException
    {
        try {
            new CountDownLatch(1).await();
        }
        catch (InterruptedException e) {
            interrupted.countDown();
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
