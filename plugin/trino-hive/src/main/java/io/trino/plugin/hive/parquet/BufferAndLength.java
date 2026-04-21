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
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static java.util.Objects.requireNonNull;

public final class BufferAndLength
        implements RuntimeCloseable
{
    private @Own HostMemoryBuffer buffer;
    private final long length;

    public BufferAndLength(@Move HostMemoryBuffer buffer, long length)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.length = length;
    }

    public @Borrow HostMemoryBuffer buffer()
    {
        return buffer;
    }

    public long length()
    {
        return length;
    }

    @Override
    public void close()
    {
        if (buffer != null) {
            buffer.close();
            buffer = null;
        }
    }
}
