/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.cache.Blob;
import io.trino.spi.metrics.Metrics;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobTrinoInput
        implements TrinoInput
{
    private final Location location;
    private final Blob blob;
    private boolean closed;

    BlobTrinoInput(Location location, Blob blob)
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        blob.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, ByteBuffer destination)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        int length = destination.remaining();
        long blobLength = blob.length();
        if (length > blobLength - position) {
            throw new EOFException("Read past end of file %s: position %s, length %s, file length %s".formatted(location, position, length, blobLength));
        }
        blob.read(position, destination);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        long blobLength = blob.length();
        int readSize = toIntExact(min(blobLength, length));
        blob.read(blobLength - readSize, buffer, offset, readSize);
        return readSize;
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(
                "bytesReadFromCache", new LongCount(blob.cachedSize()),
                "bytesReadExternally", new LongCount(blob.loadedSize())));
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            blob.close();
        }
        catch (Exception e) {
            throw new IOException("Could not close cached blob", e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + location);
        }
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
