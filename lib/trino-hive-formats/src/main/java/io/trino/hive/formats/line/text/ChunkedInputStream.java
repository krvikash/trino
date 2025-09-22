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
package io.trino.hive.formats.line.text;

import io.trino.filesystem.TrinoInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This class is a wrapper around {@link TrinoInput} that reads data in chunks.
 * It is a workaround for the abort in S3InputStream on a large file not completing quickly,
 * that is potentially caused by reading too much data after the end of a split that ends
 * in the middle of a large file.
 */
// TODO: https://starburstdata.atlassian.net/browse/INTAKE-797 When the original issue is fixed, this class should be removed.
public final class ChunkedInputStream
        extends InputStream
{
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;
    private final TrinoInput input;
    private final long inputLength;
    private final byte[] buffer;

    private int bufferPosition;
    private int bytesBuffered;
    private long inputPosition;

    public ChunkedInputStream(TrinoInput input, long inputLength)
    {
        this(input, inputLength, DEFAULT_BUFFER_SIZE);
    }

    public ChunkedInputStream(TrinoInput input, long inputLength, int bufferSize)
    {
        this.input = requireNonNull(input, "input is null");
        this.inputLength = inputLength;
        this.buffer = new byte[bufferSize];
    }

    @Override
    public int read()
            throws IOException
    {
        if (bytesBuffered == 0) {
            fillBuffer();
        }

        if (bytesBuffered == 0) {
            return -1;
        }
        bytesBuffered--;
        return buffer[bufferPosition++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len <= 0) {
            return 0;
        }

        if (bytesBuffered == 0) {
            fillBuffer();
        }

        if (bytesBuffered == 0) {
            return -1;
        }

        if (len > bytesBuffered) {
            len = bytesBuffered;
        }

        System.arraycopy(buffer, bufferPosition, b, off, len);
        bufferPosition += len;
        bytesBuffered -= len;
        return len;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (n <= 0) {
            return 0;
        }
        if (n < bytesBuffered) {
            bufferPosition += toIntExact(n);
            bytesBuffered -= toIntExact(n);
            return n;
        }
        if (inputPosition + n - bytesBuffered > inputLength) {
            n = inputLength - inputPosition - bytesBuffered;
        }
        inputPosition += n - bytesBuffered;
        bytesBuffered = 0;
        bufferPosition = 0;
        return n;
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    public long getRetainedSize()
    {
        return sizeOf(buffer);
    }

    private void fillBuffer()
            throws IOException
    {
        bufferPosition = 0;
        bytesBuffered = toIntExact(Math.min(inputLength - inputPosition, buffer.length));
        input.readFully(inputPosition, buffer, 0, bytesBuffered);
        inputPosition += bytesBuffered;
    }
}
