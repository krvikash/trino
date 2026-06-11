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
package io.trino.spi.gpu;

import ai.rapids.cudf.ColumnVector;
import io.trino.spi.Unstable;
import io.trino.spi.block.Block;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static io.trino.spi.gpu.Preconditions.checkArgument;
import static io.trino.spi.gpu.Preconditions.checkState;

/**
 * Not thread-safe.
 */
public sealed interface Column
        extends RuntimeCloseable
{
    int positionCount();

    long retainedHeapMemoryBytes();

    long retainedOffHeapMemoryBytes();

    long retainedDeviceMemoryBytes();

    // TODO is this API right name?
    @Unstable
    @Move
    Column incRefCount();

    final class Blocks
            implements Column
    {
        private final int positionCount;
        private final List<Block> blocks;
        private final long retainedHeapMemoryBytes;

        public Blocks(List<Block> blocks)
        {
            this.positionCount = blocks.stream().mapToInt(Block::getPositionCount).sum();
            this.blocks = List.copyOf(blocks);
            this.retainedHeapMemoryBytes = blocks.stream().mapToLong(Block::getRetainedSizeInBytes).sum();
        }

        @Override
        public int positionCount()
        {
            return positionCount;
        }

        @Override
        public long retainedHeapMemoryBytes()
        {
            return retainedHeapMemoryBytes;
        }

        @Override
        public long retainedOffHeapMemoryBytes()
        {
            return 0;
        }

        @Override
        public long retainedDeviceMemoryBytes()
        {
            return 0;
        }

        public List<Block> blocks()
        {
            return blocks;
        }

        /**
         * @deprecated No-op. Exists only to satisfy interface. No point in calling directly.
         */
        @Deprecated
        @Override
        public Column incRefCount()
        {
            // Nothing to do, no ref-counting
            return this;
        }

        @Override
        public void close() {}
    }

    final class DeviceMemory
            implements Column
    {
        private final @Own ColumnVector columnVector;
        private final long retainedDeviceMemoryBytes;
        private boolean closed;

        public DeviceMemory(ColumnVector columnVector)
        {
            checkArgument(columnVector.getRowCount() <= Integer.MAX_VALUE, "Too many rows: %s", columnVector.getRowCount());
            this.columnVector = columnVector;
            this.retainedDeviceMemoryBytes = columnVector.getDeviceMemorySize();
        }

        public @Borrow ColumnVector columnVector()
        {
            checkState(!closed, "Already closed");
            return columnVector;
        }

        @Override
        public int positionCount()
        {
            checkState(!closed, "Already closed");
            return (int) columnVector.getRowCount();
        }

        @Override
        public long retainedHeapMemoryBytes()
        {
            return 0;
        }

        @Override
        public long retainedOffHeapMemoryBytes()
        {
            return 0;
        }

        @Override
        public long retainedDeviceMemoryBytes()
        {
            checkState(!closed, "Already closed");
            return retainedDeviceMemoryBytes;
        }

        @Override
        public @Move Column incRefCount()
        {
            return new DeviceMemory(columnVector().incRefCount());
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            // Decrements internal refcount and disposes memory if it was the last reference.
            columnVector.close();
        }
    }
}
