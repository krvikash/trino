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
package io.trino.plugin.hive.parquet;

import ai.rapids.cudf.HostMemoryBuffer;
import io.trino.annotation.NotThreadSafe;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;

@NotThreadSafe
public class HostMemoryBufferOutputStream
        extends OutputStream
        implements RuntimeCloseable
{
    private @Own HostMemoryBuffer out;
    private long position;

    private @Nullable byte[] localBuffer;

    public HostMemoryBufferOutputStream(long size)
    {
        this.out = HostMemoryBuffer.allocate(size);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        // setByte does range checks
        out.setByte(position, (byte) b);
        position++;
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        out.setBytes(position, b, off, len);
        position += len;
    }

    public void writeBytes(InputStream in, int length)
            throws IOException
    {
        checkArgument(0 <= length, "length cannot be negative: %s", length);
        if (localBuffer == null) {
            localBuffer = new byte[16 * 1024];
        }

        byte[] buffer = localBuffer;
        // prevent accidental sharing if provided InputStream is somehow connected to `this`
        localBuffer = null;

        int remaining = length;
        while (remaining > 0) {
            int read = in.read(buffer, 0, min(remaining, buffer.length));
            checkState(read != -1, "Unexpected EOF when reading %s bytes, %s still remaining", length, remaining);
            write(buffer, 0, read);
            remaining -= read;
        }

        // return for reuse
        localBuffer = buffer;
    }

    public long getWrittenBytes()
    {
        return position;
    }

    public @Move BufferAndLength getWrittenDataAndClose()
    {
        BufferAndLength written = new BufferAndLength(out, position);
        out = null; // no longer owned
        return written;
    }

    @Override
    public void close()
    {
        if (out != null) {
            out.close();
            out = null;
        }
    }
}
