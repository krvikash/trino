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
package io.trino.spi;

import io.trino.spi.block.Block;
import io.trino.spi.block.PreSizedBlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.OptionalDouble;

import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * This plays similar role as PageBuilder but for PreSizedBlockBuilder.
 * The underlying PreSizedBlockBuilders maintain fixed capacity, but the
 * page builder starts them at small size and grows them to larger size
 * based on heuristics about observed page sizes.
 * This implementation avoids BlockBuilderStatus tracking and capacity checks
 * and re-sizing within hot loop that happens in PageBuilder/BlockBuilder.
 */
public class PreSizedPageBuilder
{
    // We choose default initial size to be 8 for PreSizedPageBuilder
    // so the underlying data is larger than the object overhead, and the size is power of 2.
    //
    // This could be any other small number.
    static final int DEFAULT_INITIAL_CAPACITY = 8;
    static final int MAX_ENTRIES = 64 * 1024;
    private static final int BATCH_GROWTH_FACTOR = 2;

    private final PreSizedBlockBuilder[] blockBuilders;
    private int capacity = DEFAULT_INITIAL_CAPACITY;
    private int declaredPositions;
    private OptionalDouble maxAverageBytesPerPosition = OptionalDouble.empty();

    /**
     * Create a PreSizedPageBuilder with given types.
     * <p>
     * A PreSizedPageBuilder instance created with this constructor has no estimation about bytes per entry,
     * therefore it can resize frequently while appending new rows.
     * <p>
     * This constructor should only be used to get the initial PreSizedPageBuilder.
     * Once the PreSizedPageBuilder is full use reset() to create a new
     * PreSizedPageBuilder instance with its size estimated based on previous data.
     */
    public PreSizedPageBuilder(List<? extends Type> types)
    {
        // Stream API should not be used since constructor can be called in performance sensitive sections
        this.blockBuilders = new PreSizedBlockBuilder[types.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            this.blockBuilders[i] = types.get(i).createPreSizedBlockBuilder(capacity);
        }
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }

        int newCapacity = calculateNewCapacity();
        declaredPositions = 0;
        capacity = newCapacity;

        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilderLike(newCapacity);
        }
    }

    public PreSizedBlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    public void declarePosition()
    {
        if (isFull()) {
            throw new IllegalStateException("PageBuilder is full with " + declaredPositions + " positions; cannot declare more positions");
        }
        declaredPositions++;
    }

    public boolean isFull()
    {
        return declaredPositions >= capacity;
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public int getPositionCount()
    {
        return declaredPositions;
    }

    public Page build()
    {
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        Page result = Page.wrapBlocksWithoutCopy(declaredPositions, blocks);
        double averageBytesPerPosition = (double) result.getSizeInBytes() / declaredPositions;
        if (averageBytesPerPosition > 0) {
            if (maxAverageBytesPerPosition.isEmpty()) {
                maxAverageBytesPerPosition = OptionalDouble.of(averageBytesPerPosition);
            }
            else {
                maxAverageBytesPerPosition = OptionalDouble.of(max(maxAverageBytesPerPosition.getAsDouble(), averageBytesPerPosition));
            }
        }
        return result;
    }

    private int calculateNewCapacity()
    {
        if (maxAverageBytesPerPosition.isEmpty()) {
            return capacity;
        }

        if (maxAverageBytesPerPosition.getAsDouble() == 0) {
            return MAX_ENTRIES;
        }

        return (int) min(
                MAX_ENTRIES,
                min(BATCH_GROWTH_FACTOR * declaredPositions, DEFAULT_MAX_PAGE_SIZE_IN_BYTES / maxAverageBytesPerPosition.getAsDouble()));
    }
}
