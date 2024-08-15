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
package io.trino.plugin.warp.juffer;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.util.SliceUtils.allocateOffsetsArray;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.Objects.requireNonNull;

@Singleton
public class DomainToMapBlockConvertor
{
    private final StorageEngineConstants storageEngineConstants;

    @Inject
    public DomainToMapBlockConvertor(StorageEngineConstants storageEngineConstants)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
    }

    Optional<Block> convert(Domain domain)
    {
        Block sortedRangesBlock = ((SortedRangeSet) domain.getValues()).getSortedRanges();
        SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
        int numValues = sortedRangeSet.getRangeCount();
        Type type = domain.getType();
        boolean[] nulls = null;
        Optional<Block> ret;
        int buffIx;
        int arrIx;

        if (domain.isNullAllowed()) {
            numValues++;
            nulls = new boolean[numValues];
            nulls[numValues - 1] = true;
        }

        if (TypeUtils.isStrType(type)) {
            List<Slice> slices = SliceUtils.getOrderedStringPredicateValues(sortedRangeSet, sortedRangeSet.getRangeCount(), storageEngineConstants);
            Pair<Slice, int[]> slicePair = combineSlices(slices, numValues, domain.isNullAllowed());
            ret = Optional.of(new VariableWidthBlock(numValues, slicePair.getLeft(), slicePair.getRight(), Optional.ofNullable(nulls)));
        }
        else if (TypeUtils.isSmallIntType(type)) {
            short[] shortValues = new short[numValues];

            for (arrIx = 0, buffIx = 0; buffIx < sortedRangesBlock.getPositionCount(); arrIx += 1, buffIx += 2) {
                shortValues[arrIx] = SMALLINT.getShort(sortedRangesBlock, buffIx);
            }
            ret = Optional.of(new ShortArrayBlock(numValues, Optional.ofNullable(nulls), shortValues));
        }
        else if (TypeUtils.isIntegerType(type) || TypeUtils.isRealType(type)) {
            int[] intValues = new int[numValues];

            for (arrIx = 0, buffIx = 0; buffIx < sortedRangesBlock.getPositionCount(); arrIx += 1, buffIx += 2) {
                intValues[arrIx] = INTEGER.getInt(sortedRangesBlock, buffIx);
            }
            ret = Optional.of(new IntArrayBlock(numValues, Optional.ofNullable(nulls), intValues));
        }
        else if (TypeUtils.isLongType(type) || TypeUtils.isDoubleType(type) || TypeUtils.isBigIntegerType(type) || TypeUtils.isShortDecimalType(type)) {
            long[] longValues = new long[numValues];

            LongArrayBlock block = (LongArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
            for (arrIx = 0, buffIx = 0; buffIx < sortedRangesBlock.getPositionCount(); arrIx += 1, buffIx += 2) {
                longValues[arrIx] = block.getLong(sortedRangesBlock.getUnderlyingValuePosition(buffIx));
            }
            ret = Optional.of(new LongArrayBlock(numValues, Optional.ofNullable(nulls), longValues));
        }
        else if (TypeUtils.isLongDecimalType(type)) {
            long[] longValues = new long[numValues * 2];
            Int128ArrayBlock int128Block = (Int128ArrayBlock) sortedRangesBlock.getUnderlyingValueBlock();
            for (arrIx = 0, buffIx = 0; buffIx < sortedRangesBlock.getPositionCount(); arrIx += 2, buffIx += 2) {
                longValues[arrIx] = int128Block.getInt128High(sortedRangesBlock.getUnderlyingValuePosition(buffIx));
                longValues[arrIx + 1] = int128Block.getInt128Low(sortedRangesBlock.getUnderlyingValuePosition(buffIx));
            }
            ret = Optional.of(new Int128ArrayBlock(numValues, Optional.ofNullable(nulls), longValues));
        }
        else {
            throw new UnsupportedOperationException();
        }

        return ret;
    }

    private Pair<Slice, int[]> combineSlices(List<Slice> slices, int numValues, boolean collectNulls)
    {
        int sliceLen = slices.stream().mapToInt(Slice::length).sum();
        byte[] values = new byte[sliceLen];
        Slice outputSlice = Slices.wrappedBuffer(values);
        int[] offsets = allocateOffsetsArray(numValues);
        for (int i = 0; i < slices.size(); i++) {
            Slice slice = slices.get(i);
            offsets[i + 1] = offsets[i] + slice.length();
            outputSlice.setBytes(offsets[i], slice);
        }
        if (collectNulls) {
            offsets[numValues] = offsets[numValues - 1];
        }
        return Pair.of(outputSlice, offsets);
    }
}
