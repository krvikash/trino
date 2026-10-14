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
package io.trino.operator.join;

import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;

/**
 * Join key values of {@link BigintPagesHash}, stored at the natural width of the key type.
 */
public interface JoinKeyValues
{
    long get(int position);

    /**
     * Copies the first positionCount keys of the block to the positions starting at offset and their starting hash buckets to hashPositions.
     */
    void copyFrom(Block block, int offset, int positionCount, int mask, int[] hashPositions);

    /**
     * Copies the keys of the block at the given non-null positions to offset plus each position and their starting hash buckets to hashPositions.
     */
    void copyFrom(Block block, int offset, int[] positions, int mask, int[] hashPositions);

    long readKey(ValueBlock block, int position);

    /**
     * Reads the keys of the block at the first positionCount positions into target and their starting hash buckets into hashPositions.
     */
    void readKeys(Block block, int[] positions, int positionCount, int mask, long[] target, int[] hashPositions);

    long getRetainedSizeInBytes();
}
