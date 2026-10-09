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

import io.airlift.slice.Slice;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

/**
 * Bulk copies a Trino flat array block into an Arrow vector. For the columns where the Arrow value is the raw
 * block element (integers, doubles, dates, varchars, ...), the values are copied with a memory copy instead of a
 * per-position loop, and the null bitmap is filled in one pass. Trino and Arrow both use the platform byte order,
 * so a raw copy is layout-compatible on the little-endian hardware Trino runs on.
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
        MemorySegment destination = segment(vector.getDataBuffer(), bytes);
        MemorySegment.copy(source, (long) rawOffset * typeWidth, destination, 0, bytes);
    }

    /**
     * Copies a variable-width block into the vector: the raw data slice in one memory copy, and the offset buffer
     * rebased so the first value starts at zero (the caller must still {@code setLastSet} + {@code setValueCount}).
     */
    static void copyVariableWidth(BaseVariableWidthVector vector, VariableWidthBlock block, int positionCount)
    {
        int[] rawOffsets = block.getRawOffsets();
        int base = block.getRawArrayBase();
        int dataStart = rawOffsets[base];
        int dataBytes = rawOffsets[base + positionCount] - dataStart;

        Slice slice = block.getRawSlice();
        MemorySegment source = MemorySegment.ofArray(slice.byteArray());
        MemorySegment dataDestination = segment(vector.getDataBuffer(), dataBytes);
        MemorySegment.copy(source, (long) slice.byteArrayOffset() + dataStart, dataDestination, 0, dataBytes);

        MemorySegment offsets = segment(vector.getOffsetBuffer(), (long) (positionCount + 1) * Integer.BYTES);
        for (int position = 0; position <= positionCount; position++) {
            offsets.setAtIndex(ValueLayout.JAVA_INT, position, rawOffsets[base + position] - dataStart);
        }
    }

    static void writeValidity(BaseFixedWidthVector vector, ValueBlock block, int positionCount)
    {
        writeValidity(vector.getValidityBuffer(), block, positionCount);
    }

    static void writeValidity(BaseVariableWidthVector vector, ValueBlock block, int positionCount)
    {
        writeValidity(vector.getValidityBuffer(), block, positionCount);
    }

    /**
     * Fills the validity buffer from the block's nulls: all-ones when the block has no nulls, otherwise a packed
     * bitmap where a set bit means the position is valid (Trino's null flag inverted).
     */
    private static void writeValidity(ArrowBuf validityBuffer, ValueBlock block, int positionCount)
    {
        int byteCount = (positionCount + 7) / 8;
        MemorySegment validity = segment(validityBuffer, byteCount);
        Optional<Bitmap> validityBitmap = block.getValidityBitmap();
        if (validityBitmap.isEmpty()) {
            validity.fill((byte) 0xFF);
            return;
        }
        long[] rawWords = validityBitmap.get().getRawWords();
        int rawBitOffset = validityBitmap.get().getRawBitOffset();
        if (rawBitOffset % 8 == 0) {
            // Byte-aligned: the source bytes already line up with Arrow's own byte-packed layout
            MemorySegment source = MemorySegment.ofArray(rawWords);
            MemorySegment.copy(source, rawBitOffset / 8, validity, 0, byteCount);
            return;
        }
        // A block sliced off a bit offset that isn't byte-aligned (e.g. Block#getRegion) needs each byte
        // gathered from its true bit position instead of a raw copy
        for (int byteIndex = 0; byteIndex < byteCount; byteIndex++) {
            int position = byteIndex << 3;
            int bitsInByte = Math.min(8, positionCount - position);
            validity.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) Bitmap.getBits(rawWords, rawBitOffset, position, bitsInByte));
        }
    }

    /**
     * The first {@code bytes} bytes of the buffer. The view is bounded by the buffer's capacity, so a copy into a
     * vector that was not allocated large enough fails instead of writing past the end of the buffer.
     */
    private static MemorySegment segment(ArrowBuf buffer, long bytes)
    {
        return MemorySegment.ofAddress(buffer.memoryAddress())
                .reinterpret(buffer.capacity())
                .asSlice(0, bytes);
    }
}
