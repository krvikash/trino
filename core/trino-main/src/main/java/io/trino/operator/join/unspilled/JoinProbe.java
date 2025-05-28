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
package io.trino.operator.join.unspilled;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.join.LookupSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BooleanArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * This class eagerly calculates all join positions and stores them in an array
 * PageJoiner is responsible for ensuring that only the first position is processed for RLE with no or single build row match
 */
public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0
        private final boolean hasFilter;
        private final InterpretedHashGenerator hashGenerator;

        public JoinProbeFactory(List<Integer> probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel, boolean hasFilter, InterpretedHashGenerator hashGenerator)
        {
            this.probeOutputChannels = Ints.toArray(requireNonNull(probeOutputChannels, "probeOutputChannels is null"));
            this.probeJoinChannels = Ints.toArray(requireNonNull(probeJoinChannels, "probeJoinChannels is null"));
            this.probeHashChannel = requireNonNull(probeHashChannel, "probeHashChannel is null").orElse(-1);
            this.hasFilter = hasFilter;
            this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        }

        public JoinProbe createJoinProbe(Page page, LookupSource lookupSource)
        {
            Page probePage = page.getColumns(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, lookupSource, probeHashChannel >= 0 ? page.getBlock(probeHashChannel) : null, hasFilter, hashGenerator);
        }
    }

    private final int[] probeOutputChannels;
    private final Page page;
    private final long[] joinPositionCache;
    private final boolean isRle;
    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, LookupSource lookupSource, @Nullable Block probeHashBlock, boolean hasFilter, InterpretedHashGenerator hashGenerator)
    {
        this.probeOutputChannels = requireNonNull(probeOutputChannels, "probeOutputChannels is null");
        this.page = requireNonNull(page, "page is null");

        // if filter channels are not RLE encoded, then every probe
        // row might be unique and must be matched independently
        this.isRle = !hasFilter && hasOnlyRleBlocks(probePage);
        joinPositionCache = fillCache(lookupSource, page, probeHashBlock, probePage, isRle, hashGenerator);
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        verify(++position <= page.getPositionCount(), "already finished");
        return !isFinished();
    }

    public void finish()
    {
        position = page.getPositionCount();
    }

    public boolean isFinished()
    {
        return position == page.getPositionCount();
    }

    public long getCurrentJoinPosition()
    {
        return joinPositionCache[position];
    }

    public int getPosition()
    {
        return position;
    }

    public boolean areProbeJoinChannelsRunLengthEncoded()
    {
        return isRle;
    }

    public Page getPage()
    {
        return page;
    }

    private static long[] fillCache(
            LookupSource lookupSource,
            Page page,
            Block probeHashBlock,
            Page probePage,
            boolean isRle,
            InterpretedHashGenerator hashGenerator)
    {
        int positionCount = page.getPositionCount();

        Block[] nullableBlocks = new Block[probePage.getChannelCount()];
        int nullableBlocksCount = 0;
        for (int channel = 0; channel < probePage.getChannelCount(); channel++) {
            Block probeBlock = probePage.getBlock(channel);
            if (probeBlock.mayHaveNull()) {
                nullableBlocks[nullableBlocksCount++] = probeBlock;
            }
        }

        if (isRle) {
            long[] joinPositionCache;
            // Null values cannot be joined, so if any column contains null, there is no match
            boolean anyAllNullsBlock = false;
            for (int i = 0; i < nullableBlocksCount; i++) {
                Block nullableBlock = nullableBlocks[i];
                if (nullableBlock.isNull(0)) {
                    anyAllNullsBlock = true;
                    break;
                }
            }
            if (anyAllNullsBlock) {
                joinPositionCache = new long[1];
                joinPositionCache[0] = -1;
            }
            else {
                joinPositionCache = new long[positionCount];
                // We can fall back to processing all positions in case there are multiple build rows matched for the first probe position
                Arrays.fill(joinPositionCache, lookupSource.getJoinPosition(0, probePage, page));
            }

            return joinPositionCache;
        }

        long[] joinPositionCache = new long[positionCount];
        int[] positions = getNonNullPositions(nullableBlocks, nullableBlocksCount, positionCount);
        long[] hashes = new long[positionCount];
        if (nullableBlocksCount > 0 && positions.length < positionCount) {
            Arrays.fill(joinPositionCache, -1);
            if (probeHashBlock != null) {
                for (int i = 0; i < positionCount; i++) {
                    hashes[i] = BIGINT.getLong(probeHashBlock, i);
                }
            }
            else {
                hashGenerator.hashNonNulls(probePage, positions, hashes);
            }
            lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionCache);
            return joinPositionCache;
        } // else fall back to non-null path

        if (probeHashBlock != null) {
            for (int i = 0; i < positionCount; i++) {
                hashes[i] = BIGINT.getLong(probeHashBlock, i);
            }
        }
        else {
            hashGenerator.hash(probePage, 0, positionCount, hashes);
        }
        lookupSource.getJoinPosition(positions, probePage, page, hashes, joinPositionCache);

        return joinPositionCache;
    }

    @VisibleForTesting
    static int[] getNonNullPositions(Block[] nullableBlocks, int nullableBlocksCount, int positionCount)
    {
        if (nullableBlocksCount == 0) {
            // no nullable blocks, all positions are non-null
            int[] positions = new int[positionCount];
            for (int position = 0; position < positionCount; position++) {
                positions[position] = position;
            }
            return positions;
        }
        if (nullableBlocksCount == 1) {
            // Special case for a single nullable block to avoid the need for explicit `boolean[] isNull`
            int[] outputPositions = new int[positionCount];
            int outputPositionsCount = getNonNullPositions(nullableBlocks[0], positionCount, outputPositions);
            if (outputPositionsCount == positionCount) {
                return outputPositions;
            }
            return Arrays.copyOf(outputPositions, outputPositionsCount);
        }

        boolean[] isNull = new boolean[positionCount];
        for (int i = 0; i < nullableBlocksCount - 1; i++) {
            Block nullableBlock = nullableBlocks[i];
            getNonNullPositions(nullableBlock, isNull);
        }

        int[] outputPositions = new int[positionCount];
        // For the last nullable block, we need to fill outputPositions and count non-null positions
        int outputPositionsCount = getNonNullPositionsLast(nullableBlocks[nullableBlocksCount - 1], isNull, outputPositions);
        if (outputPositionsCount == positionCount) {
            return outputPositions;
        }
        return Arrays.copyOf(outputPositions, outputPositionsCount);
    }

    private static int getNonNullPositions(Block nullableBlock, int positionCount, int[] outputPositions)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    return 0; // all positions are null
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<BooleanArrayBlock> dictionaryIsNullBlock = dictionary.getNulls();
                if (dictionaryIsNullBlock.isPresent()) {
                    int outputPositionCount = 0;
                    boolean[] dictionaryIsNull = dictionaryIsNullBlock.get().getRawValues();
                    int isNullOffset = dictionaryIsNullBlock.get().getRawValuesOffset();
                    for (int position = 0; position < positionCount; position++) {
                        boolean isNull = dictionaryIsNull[isNullOffset + dictionaryBlock.getId(position)];
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<BooleanArrayBlock> isNullsBlock = valueBlock.getNulls();
                if (isNullsBlock.isPresent()) {
                    int outputPositionCount = 0;
                    boolean[] isNulls = isNullsBlock.get().getRawValues();
                    int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                    for (int position = 0; position < positionCount; position++) {
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNulls[isNullOffset + position] ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
        }
        for (int position = 0; position < positionCount; position++) {
            outputPositions[position] = position;
        }
        return positionCount;
    }

    private static void getNonNullPositions(Block nullableBlock, boolean[] isNull)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    Arrays.fill(isNull, true);
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<BooleanArrayBlock> dictionaryIsNullBlock = dictionary.getNulls();
                if (dictionaryIsNullBlock.isPresent()) {
                    boolean[] dictionaryIsNull = dictionaryIsNullBlock.get().getRawValues();
                    int isNullOffset = dictionaryIsNullBlock.get().getRawValuesOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= dictionaryIsNull[isNullOffset + dictionaryBlock.getId(position)];
                    }
                }
            }
            case ValueBlock valueBlock -> {
                Optional<BooleanArrayBlock> isNullsBlock = valueBlock.getNulls();
                if (isNullsBlock.isPresent()) {
                    boolean[] isNulls = isNullsBlock.get().getRawValues();
                    int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= isNulls[isNullOffset + position];
                    }
                }
            }
        }
    }

    private static int getNonNullPositionsLast(Block nullableBlock, boolean[] isNull, int[] outputPositions)
    {
        int outputPositionCount = 0;
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    return 0;
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<BooleanArrayBlock> dictionaryIsNullBlock = dictionary.getNulls();
                if (dictionaryIsNullBlock.isPresent()) {
                    boolean[] dictionaryIsNull = dictionaryIsNullBlock.get().getRawValues();
                    int isNullOffset = dictionaryIsNullBlock.get().getRawValuesOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= dictionaryIsNull[isNullOffset + dictionaryBlock.getId(position)];
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull[position] ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<BooleanArrayBlock> isNullsBlock = valueBlock.getNulls();
                if (isNullsBlock.isPresent()) {
                    boolean[] isNulls = isNullsBlock.get().getRawValues();
                    int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= isNulls[isNullOffset + position];
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull[position] ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
        }
        for (int position = 0; position < isNull.length; position++) {
            outputPositions[outputPositionCount] = position;
            outputPositionCount += isNull[position] ? 0 : 1;
        }
        return outputPositionCount;
    }

    private static boolean hasOnlyRleBlocks(Page probePage)
    {
        if (probePage.getChannelCount() == 0) {
            return false;
        }

        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (!(probePage.getBlock(i) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }
}
