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
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.operator.join.PagesHash.getHashPosition;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This implementation assumes:
 * -There is only one join channel and it is of type bigint, integer, date, smallint or tinyint
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
    private final JoinKeyValues values;
    // Resolves a row number to its (block index, position) for output.
    private final BlockPositionIndex blockPositionIndex;
    private final long size;

    public BigintPagesHash(
            PagesHashStrategy pagesHashStrategy,
            BlockPositionIndex blockPositionIndex,
            PositionLinks.FactoryBuilder positionLinks,
            HashArraySizeSupplier hashArraySizeSupplier,
            List<Page> pages,
            int joinChannel,
            Type joinType)
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
        values = createValues(joinType, positionCount);
        Arrays.fill(keys, -1);

        // (block, position) come from the page structure.
        // Per page the non-null positions are pulled out first; the all-non-null case runs the full range
        // branch-free, otherwise only the non-null positions are processed.
        int[] hashPositions = new int[maxPagePositions];
        int[] collisionPositions = new int[maxPagePositions];
        Block[] nullableBlocks = new Block[1];
        int offset = 0;
        for (Page page : pages) {
            Block block = page.getBlock(joinChannel);
            int pagePositions = page.getPositionCount();
            int nullableCount = block.mayHaveNull() ? 1 : 0;
            nullableBlocks[0] = block;
            Optional<int[]> nonNullPositions = NullablePositions.getNonNullPositions(nullableBlocks, nullableCount, pagePositions);
            // A batched pass materializes values and starting hash buckets (sequential, no hash-table access), then a
            // separate pass inserts so probe stalls are not serialized behind hash computation.
            if (nonNullPositions.isEmpty()) {
                values.copyFrom(block, offset, pagePositions, mask, hashPositions);
                indexRange(positionLinks, offset, pagePositions, hashPositions, collisionPositions);
            }
            else {
                values.copyFrom(block, offset, nonNullPositions.get(), mask, hashPositions);
                indexPositions(positionLinks, offset, nonNullPositions.get(), hashPositions, collisionPositions);
            }
            offset += pagePositions;
        }

        size = pagesHashStrategy.getSizeInBytes() +
                sizeOf(keys) + values.getRetainedSizeInBytes() + blockPositionIndex.getRetainedSizeInBytes();
    }

    private void indexRange(PositionLinks.FactoryBuilder positionLinks, int offset, int pagePositions, int[] hashPositions, int[] collisionPositions)
    {
        int collisionCount = 0;
        for (int position = 0; position < pagePositions; position++) {
            int hashPosition = hashPositions[position];
            if (keys[hashPosition] == -1) {
                keys[hashPosition] = offset + position;
            }
            else {
                collisionPositions[collisionCount++] = position;
            }
        }
        insertCollisions(positionLinks, offset, hashPositions, collisionPositions, collisionCount);
    }

    private void indexPositions(PositionLinks.FactoryBuilder positionLinks, int offset, int[] positions, int[] hashPositions, int[] collisionPositions)
    {
        int collisionCount = 0;
        for (int position : positions) {
            int hashPosition = hashPositions[position];
            if (keys[hashPosition] == -1) {
                keys[hashPosition] = offset + position;
            }
            else {
                collisionPositions[collisionCount++] = position;
            }
        }
        insertCollisions(positionLinks, offset, hashPositions, collisionPositions, collisionCount);
    }

    // Inserts rows whose starting bucket was taken, so the claim loop makes no JoinKeyValues call.
    private void insertCollisions(PositionLinks.FactoryBuilder positionLinks, int offset, int[] hashPositions, int[] collisionPositions, int collisionCount)
    {
        for (int i = 0; i < collisionCount; i++) {
            int position = collisionPositions[i];
            insertValue(positionLinks, offset + position, hashPositions[position], values.get(offset + position));
        }
    }

    private void insertValue(PositionLinks.FactoryBuilder positionLinks, int addressIndex, int pos, long value)
    {
        // look for an empty slot or a slot containing this key
        while (keys[pos] != -1) {
            int currentKey = keys[pos];
            if (value == values.get(currentKey)) {
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
        long value = readValue(hashChannelsPage.getBlock(0), position);
        int pos = getHashPosition(value, mask);

        while (keys[pos] != -1) {
            if (value == values.get(keys[pos])) {
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

        values.readKeys(hashChannelsPage.getBlock(0), positions, positionCount, mask, incomingValues, hashPositions);

        // Search for positions in the hash array. This is the most CPU-consuming part as
        // it relies on random memory accesses
        int[] result = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            result[i] = keys[hashPositions[i]];
        }

        // At this step we determine if the found keys were indeed the proper ones or it is a hash collision.
        // For any reasoable load factor of a hash array (< .75), there is no more than
        // 10 - 15% of positions left. We search for them in a sequential order and update the result array.
        for (int i = 0; i < positionCount; i++) {
            int key = result[i];
            if (key != -1 && values.get(key) != incomingValues[i]) {
                result[i] = findRemainingPosition(incomingValues[i], hashPositions[i]);
            }
        }
        return result;
    }

    private int findRemainingPosition(long value, int hashPosition)
    {
        int position = (hashPosition + 1) & mask; // hashPosition has already been checked
        while (keys[position] != -1) {
            if (values.get(keys[position]) == value) {
                return keys[position];
            }
            // increment position and mask to handler wrap around
            position = (position + 1) & mask;
        }
        return -1;
    }

    private long readValue(Block block, int position)
    {
        return switch (block) {
            case RunLengthEncodedBlock rleBlock -> values.readKey(rleBlock.getValue(), 0);
            case DictionaryBlock dictionaryBlock -> values.readKey(dictionaryBlock.getDictionary(), dictionaryBlock.getId(position));
            case ValueBlock valueBlock -> values.readKey(valueBlock, position);
        };
    }

    private static JoinKeyValues createValues(Type joinType, int positionCount)
    {
        if (joinType == BIGINT) {
            return new LongJoinKeyValues(positionCount);
        }
        if (joinType == INTEGER || joinType == DATE) {
            return new IntJoinKeyValues(positionCount);
        }
        if (joinType == SMALLINT) {
            return new ShortJoinKeyValues(positionCount);
        }
        if (joinType == TINYINT) {
            return new ByteJoinKeyValues(positionCount);
        }
        throw unsupportedType(joinType);
    }

    private static long getEstimatedValuesRetainedSizeInBytes(Type joinType, int positionCount)
    {
        if (joinType == BIGINT) {
            return LongJoinKeyValues.getEstimatedRetainedSizeInBytes(positionCount);
        }
        if (joinType == INTEGER || joinType == DATE) {
            return IntJoinKeyValues.getEstimatedRetainedSizeInBytes(positionCount);
        }
        if (joinType == SMALLINT) {
            return ShortJoinKeyValues.getEstimatedRetainedSizeInBytes(positionCount);
        }
        if (joinType == TINYINT) {
            return ByteJoinKeyValues.getEstimatedRetainedSizeInBytes(positionCount);
        }
        throw unsupportedType(joinType);
    }

    private static IllegalArgumentException unsupportedType(Type joinType)
    {
        return new IllegalArgumentException("Unsupported join key type: " + joinType);
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
            long blocksSizeInBytes,
            Type joinType)
    {
        int blockCount = 0;
        if (!channels.isEmpty()) {
            blockCount = channels.getFirst().size();
        }
        long blockPositionIndexSize = BlockPositionIndex.getEstimatedRetainedSizeInBytes(blockCount, positionCount);
        return (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                blocksSizeInBytes +
                sizeOfIntArray(hashArraySizeSupplier.getHashArraySize(positionCount)) +
                getEstimatedValuesRetainedSizeInBytes(joinType, positionCount) +
                blockPositionIndexSize;
    }
}
