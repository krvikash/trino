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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;

import java.util.List;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;

public interface ColumnAdapter<BufferType>
{
    /**
     * Temporary buffer used for null unpacking
     */
    default BufferType createTemporaryBuffer(int currentOffset, int size, BufferType buffer)
    {
        // Null unpacking may read one sentinel value after the last non-null value when filling trailing nulls.
        return createBuffer(size + 1);
    }

    BufferType createBuffer(int size);

    void copyValues(BufferType source, int sourceIndex, BufferType destination, int destinationIndex, int length);

    /// Creates a block from values and a validity bitmap using the [io.trino.spi.block.Bitmap] encoding.
    Block createNullableBlock(long[] valueIsValid, BufferType values);

    /// Creates a dictionary block with one trailing null entry. The validity bitmap uses the
    /// [io.trino.spi.block.Bitmap] encoding.
    default Block createNullableDictionaryBlock(BufferType dictionary, int nonNullsCount)
    {
        long[] valueIsValid = allocateWords(nonNullsCount + 1, true);
        clear(valueIsValid, 0, nonNullsCount);
        return createNullableBlock(valueIsValid, dictionary);
    }

    Block createNonNullBlock(BufferType values);

    /// Expands packed non-null values into a destination shaped by a validity bitmap using the
    /// [io.trino.spi.block.Bitmap] encoding.
    void unpackNullValues(BufferType source, BufferType destination, long[] valueIsValid, int destOffset, int nonNullCount, int totalValuesCount);

    void decodeDictionaryIds(BufferType values, int offset, int length, int[] ids, BufferType dictionary);

    long getSizeInBytes(BufferType values);

    BufferType merge(List<BufferType> buffers);
}
