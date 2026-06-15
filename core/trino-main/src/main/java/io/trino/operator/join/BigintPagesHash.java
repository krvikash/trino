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

import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PreSizedBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.trino.operator.join.PagesHash.getHashPosition;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This implementation assumes:
 * -There is only one join channel and it is of type bigint
 * -arrays used in the hash are always a power of 2.
 */
public final class BigintPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = instanceSize(BigintPagesHash.class);

    private final int positionCount;
    private final PagesHashStrategy pagesHashStrategy;

    private final int mask;
    private final int[] keys;
    private final long[] values;
    // Resolves a row number to its (block index, position) for output.
    private final BlockPositionIndex blockPositionIndex;
    private final long size;

    public BigintPagesHash(
            PagesHashStrategy pagesHashStrategy,
            BlockPositionIndex blockPositionIndex,
            PositionLinks.FactoryBuilder positionLinks,
            HashArraySizeSupplier hashArraySizeSupplier,
            List<Page> pages,
            int joinChannel)
    {
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.blockPositionIndex = requireNonNull(blockPositionIndex, "blockPositionIndex is null");
        this.positionCount = blockPositionIndex.getPositionCount();
        requireNonNull(pages, "pages is null");
        int maxPagePositions = 0;
        for (Page page : pages) {
            maxPagePositions = Math.max(maxPagePositions, page.getPositionCount());
        }

        // reserve memory for the arrays
        int hashSize = hashArraySizeSupplier.getHashArraySize(positionCount);

        mask = hashSize - 1;
        keys = new int[hashSize];
        values = new long[positionCount];
        Arrays.fill(keys, -1);

        // (block, position) come from the page structure.
        // Per page the non-null positions are pulled out first; the all-non-null case runs the full range
        // branch-free, otherwise only the non-null positions are processed.
        int[] hashPositions = new int[maxPagePositions];
        Block[] nullableBlocks = new Block[1];
        int offset = 0;
        for (Page page : pages) {
            Block block = page.getBlock(joinChannel);
            int pagePositions = page.getPositionCount();
            int nullableCount = block.mayHaveNull() ? 1 : 0;
            nullableBlocks[0] = block;
            Optional<int[]> nonNullPositions = NullablePositions.getNonNullPositions(nullableBlocks, nullableCount, pagePositions);
            if (nonNullPositions.isEmpty()) {
                indexRange(positionLinks, block, offset, pagePositions, hashPositions);
            }
            else {
                indexPositions(positionLinks, block, offset, nonNullPositions.get(), hashPositions);
            }
            offset += pagePositions;
        }

        size = pagesHashStrategy.getSizeInBytes() +
                sizeOf(keys) + sizeOf(values) + blockPositionIndex.getRetainedSizeInBytes();
    }

    // A batched pass materializes values and starting hash buckets (sequential, no hash-table access), then a
    // separate pass inserts so probe stalls are not serialized behind hash computation.
    private void indexRange(PositionLinks.FactoryBuilder positionLinks, Block block, int offset, int pagePositions, int[] hashPositions)
    {
        for (int position = 0; position < pagePositions; position++) {
            long value = BIGINT.getLong(block, position);
            values[offset + position] = value;
            hashPositions[position] = getHashPosition(value, mask);
        }
        for (int position = 0; position < pagePositions; position++) {
            insertValue(positionLinks, offset + position, hashPositions[position]);
        }
    }

    private void indexPositions(PositionLinks.FactoryBuilder positionLinks, Block block, int offset, int[] positions, int[] hashPositions)
    {
        for (int position : positions) {
            long value = BIGINT.getLong(block, position);
            values[offset + position] = value;
            hashPositions[position] = getHashPosition(value, mask);
        }
        for (int position : positions) {
            insertValue(positionLinks, offset + position, hashPositions[position]);
        }
    }

    private void insertValue(PositionLinks.FactoryBuilder positionLinks, int addressIndex, int pos)
    {
        // value already materialized by the batched pass
        long value = values[addressIndex];
        // look for an empty slot or a slot containing this key
        while (keys[pos] != -1) {
            int currentKey = keys[pos];
            if (value == values[currentKey]) {
                // found a slot for this key
                // link the new key position to the current key position
                addressIndex = positionLinks.link(addressIndex, currentKey);

                // key[pos] updated outside of this loop
                break;
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }

        keys[pos] = addressIndex;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage, long rawHash)
    {
        return getAddressIndex(position, hashChannelsPage);
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        long value = BIGINT.getLong(hashChannelsPage.getBlock(0), position);
        int pos = getHashPosition(value, mask);

        while (keys[pos] != -1) {
            if (value == values[keys[pos]]) {
                return keys[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public int[] getAddressIndex(int[] positions, Page hashChannelsPage, long[] rawHashes)
    {
        checkArgument(hashChannelsPage.getChannelCount() == 1, "Multiple channel page passed to BigintPagesHash");

        int positionCount = positions.length;
        long[] incomingValues = new long[positionCount];
        int[] hashPositions = new int[positionCount];

        extractAndHashValues(positions, hashChannelsPage, positionCount, incomingValues, hashPositions);

        int[] found = new int[positionCount];
        int foundCount = 0;
        int[] result = new int[positionCount];
        Arrays.fill(result, -1);
        int[] foundKeys = new int[positionCount];

        // Search for positions in the hash array. This is the most CPU-consuming part as
        // it relies on random memory accesses
        findPositions(positionCount, hashPositions, foundKeys);
        // Found positions are put into `found` array
        for (int i = 0; i < positionCount; i++) {
            if (foundKeys[i] != -1) {
                found[foundCount++] = i;
            }
        }

        // At this step we determine if the found keys were indeed the proper ones or it is a hash collision.
        // The result array is updated for the found ones, while the collisions land into `remaining` array.
        int remainingCount = checkFoundPositions(incomingValues, found, foundCount, result, foundKeys);
        int[] remaining = found; // Rename for readability

        // At this point for any reasoable load factor of a hash array (< .75), there is no more than
        // 10 - 15% of positions left. We search for them in a sequential order and update the result array.
        findRemainingPositions(incomingValues, hashPositions, result, remaining, remainingCount);

        return result;
    }

    private void findRemainingPositions(long[] incomingValues, int[] hashPositions, int[] result, int[] remaining, int remainingCount)
    {
        for (int i = 0; i < remainingCount; i++) {
            int index = remaining[i];
            int position = (hashPositions[index] + 1) & mask; // hashPositions[index] position has already been checked

            while (keys[position] != -1) {
                if (values[keys[position]] == incomingValues[index]) {
                    result[index] = keys[position];
                    break;
                }
                // increment position and mask to handler wrap around
                position = (position + 1) & mask;
            }
        }
    }

    private int checkFoundPositions(long[] incomingValues, int[] found, int foundCount, int[] result, int[] foundKeys)
    {
        int[] remaining = found; // Rename for readability
        int remainingCount = 0;
        for (int i = 0; i < foundCount; i++) {
            int index = found[i];
            if (values[foundKeys[index]] == incomingValues[index]) {
                result[index] = foundKeys[index];
            }
            else {
                remaining[remainingCount++] = index;
            }
        }
        return remainingCount;
    }

    private void findPositions(int positionCount, int[] hashPositions, int[] foundKeys)
    {
        for (int i = 0; i < positionCount; i++) {
            foundKeys[i] = keys[hashPositions[i]];
        }
    }

    private void extractAndHashValues(int[] positions, Page hashChannelsPage, int positionCount, long[] incomingValues, int[] hashPositions)
    {
        switch (hashChannelsPage.getBlock(0)) {
            case RunLengthEncodedBlock rleBlock -> {
                long value = BIGINT.getLong(rleBlock.getUnderlyingValueBlock(), 0);
                Arrays.fill(incomingValues, value);
                Arrays.fill(hashPositions, getHashPosition(value, mask));
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock valueBlock = dictionaryBlock.getUnderlyingValueBlock();
                for (int i = 0; i < positionCount; i++) {
                    incomingValues[i] = BIGINT.getLong(valueBlock, dictionaryBlock.getUnderlyingValuePosition(positions[i]));
                    hashPositions[i] = getHashPosition(incomingValues[i], mask);
                }
            }
            case ValueBlock valueBlock -> {
                for (int i = 0; i < positionCount; i++) {
                    incomingValues[i] = BIGINT.getLong(valueBlock, positions[i]);
                    hashPositions[i] = getHashPosition(incomingValues[i], mask);
                }
            }
        }
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int rowNumber = toIntExact(position);
        int blockIndex = blockPositionIndex.decodeBlockIndex(rowNumber);
        pagesHashStrategy.appendTo(blockIndex, blockPositionIndex.decodePosition(rowNumber, blockIndex), pageBuilder, outputChannelOffset);
    }

    @Override
    public void appendTo(long position, PreSizedBlockBuilder[] builders)
    {
        int rowNumber = toIntExact(position);
        int blockIndex = blockPositionIndex.decodeBlockIndex(rowNumber);
        pagesHashStrategy.appendTo(blockIndex, blockPositionIndex.decodePosition(rowNumber, blockIndex), builders);
    }

    public static long getEstimatedRetainedSizeInBytes(
            int positionCount,
            HashArraySizeSupplier hashArraySizeSupplier,
            List<ObjectArrayList<Block>> channels,
            long blocksSizeInBytes)
    {
        int blockCount = 0;
        if (!channels.isEmpty()) {
            blockCount = channels.getFirst().size();
        }
        long blockPositionIndexSize = BlockPositionIndex.getEstimatedRetainedSizeInBytes(blockCount, positionCount);
        return (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                blocksSizeInBytes +
                sizeOfIntArray(hashArraySizeSupplier.getHashArraySize(positionCount)) +
                sizeOfLongArray(positionCount) +
                blockPositionIndexSize;
    }
}
