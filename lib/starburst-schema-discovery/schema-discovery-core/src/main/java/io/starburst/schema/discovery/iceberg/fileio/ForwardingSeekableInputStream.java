/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.iceberg.fileio;

import io.trino.filesystem.TrinoInputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Copy of io.trino.plugin.iceberg.fileio.ForwardingSeekableInputStream
 */
public class ForwardingSeekableInputStream
        extends SeekableInputStream
{
    private final TrinoInputStream stream;

    public ForwardingSeekableInputStream(TrinoInputStream stream)
    {
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public long getPos()
            throws IOException
    {
        return stream.getPosition();
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        stream.seek(pos);
    }

    @Override
    public int read()
            throws IOException
    {
        return stream.read();
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        return stream.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes()
            throws IOException
    {
        return stream.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len)
            throws IOException
    {
        return stream.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len)
            throws IOException
    {
        return stream.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        return stream.skip(n);
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        stream.skipNBytes(n);
    }

    @Override
    public int available()
            throws IOException
    {
        return stream.available();
    }

    @Override
    public void close()
            throws IOException
    {
        stream.close();
    }

    @Override
    public void mark(int readlimit)
    {
        stream.mark(readlimit);
    }

    @Override
    public void reset()
            throws IOException
    {
        stream.reset();
    }

    @Override
    public boolean markSupported()
    {
        return stream.markSupported();
    }

    @Override
    public long transferTo(OutputStream out)
            throws IOException
    {
        return stream.transferTo(out);
    }
}
