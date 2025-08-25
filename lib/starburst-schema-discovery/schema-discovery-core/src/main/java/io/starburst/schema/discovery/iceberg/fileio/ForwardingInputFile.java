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

import io.trino.filesystem.TrinoInputFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

/**
 * Copy of io.trino.plugin.iceberg.fileio.ForwardingInputFile
 */
public class ForwardingInputFile
        implements InputFile
{
    private final TrinoInputFile inputFile;

    public ForwardingInputFile(TrinoInputFile inputFile)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
    }

    @Override
    public long getLength()
    {
        try {
            return inputFile.length();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to get status for file: " + location(), e);
        }
    }

    @Override
    public SeekableInputStream newStream()
    {
        try {
            return new ForwardingSeekableInputStream(inputFile.newStream());
        }
        catch (FileNotFoundException e) {
            throw new NotFoundException(e, "Failed to open input stream for file: %s", location());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open input stream for file: " + location(), e);
        }
    }

    @Override
    public String location()
    {
        return inputFile.location().toString();
    }

    @Override
    public boolean exists()
    {
        try {
            return inputFile.exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to check existence for file: " + location(), e);
        }
    }

    @Override
    public String toString()
    {
        return inputFile.toString();
    }
}
