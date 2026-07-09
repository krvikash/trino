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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import org.apache.arrow.vector.BaseFixedWidthVector;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Bulk copies a Trino flat array block into a fixed-width Arrow vector. For the columns where the Arrow value is
 * the raw block element (integers, doubles, dates, ...), the values are copied with a single memory copy instead
 * of a per-position loop, and the null bitmap is filled in one pass. Trino and Arrow both use the platform byte
 * order, so a raw copy is layout-compatible on the little-endian hardware Trino runs on.
 */
final class ArrowBulkCopy
{
    private ArrowBulkCopy() {}

    /**
     * Copies {@code positionCount} elements starting at {@code rawOffset} from {@code source} (a heap segment over
     * the block's raw value array) into the vector's data buffer.
     */
    static void copyFixedWidth(BaseFixedWidthVector vector, MemorySegment source, int rawOffset, int positionCount)
    {
        int typeWidth = vector.getTypeWidth();
        long bytes = (long) positionCount * typeWidth;
        MemorySegment destination = MemorySegment.ofAddress(vector.getDataBufferAddress()).reinterpret(bytes);
        MemorySegment.copy(source, (long) rawOffset * typeWidth, destination, 0, bytes);
    }

    /**
     * Fills the vector's validity buffer from the block's nulls: all-ones when the block has no nulls, otherwise a
     * packed bitmap where a set bit means the position is valid (Trino's null flag inverted). Values under null
     * positions keep whatever the data copy left there, which Arrow ignores.
     */
    static void writeValidity(BaseFixedWidthVector vector, Block block, int positionCount)
    {
        int byteCount = (positionCount + 7) / 8;
        MemorySegment validity = MemorySegment.ofAddress(vector.getValidityBufferAddress()).reinterpret(byteCount);
        if (!block.mayHaveNull()) {
            validity.fill((byte) 0xFF);
            return;
        }
        for (int byteIndex = 0; byteIndex < byteCount; byteIndex++) {
            int base = byteIndex << 3;
            int limit = Math.min(8, positionCount - base);
            int bits = 0;
            for (int offset = 0; offset < limit; offset++) {
                if (!block.isNull(base + offset)) {
                    bits |= 1 << offset;
                }
            }
            validity.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) bits);
        }
    }
}
