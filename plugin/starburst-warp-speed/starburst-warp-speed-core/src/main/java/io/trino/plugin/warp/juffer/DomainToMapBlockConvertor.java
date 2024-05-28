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
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;

@Singleton
public class DomainToMapBlockConvertor
{
    @Inject
    public DomainToMapBlockConvertor()
    {
    }

    Optional<Block> convert(Domain values)
    {
        Block sortedRangesBlock = ((SortedRangeSet) values.getValues()).getSortedRanges();
        SortedRangeSet sortedRangeSet = (SortedRangeSet) values.getValues();
        int numValues = sortedRangeSet.getRangeCount();
        Type type = values.getType();
        boolean[] nulls = null;
        Optional<Block> ret;
        int buffIx;
        int arrIx;

        if (values.isNullAllowed()) {
            numValues++;
            nulls = new boolean[numValues];
            nulls[numValues - 1] = true;
        }

        if (TypeUtils.isSmallIntType(type)) {
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
}
