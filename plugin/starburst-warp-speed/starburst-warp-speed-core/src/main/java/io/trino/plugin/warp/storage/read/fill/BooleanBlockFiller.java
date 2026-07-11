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
package io.trino.plugin.warp.storage.read.fill;

import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.type.BooleanType;

import java.nio.ByteBuffer;
import java.util.Optional;

import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.wordsForBits;

public class BooleanBlockFiller
        extends BlockFiller<Boolean>
{
    public BooleanBlockFiller()
    {
        super(BooleanType.BOOLEAN, BlockFillerType.BOOLEAN);
    }

    @Override
    protected Boolean getSingleValue(ReadJuffersWarmUpElement juffersWE, int recLength, int currPos)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        return buff.get() == 1;
    }

    @Override
    protected Block createSingleWithNullBlock(ReadJuffersWarmUpElement juffersWE, RecTypeCode recTypeCode, int recLength, int rowsToFill)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        byte[] mappingValues = new byte[] {buff.get(), 0}; // including null
        long[] valueIsValid = Bitmap.allocateWords(2, true);
        Bitmap.clear(valueIsValid, 0, 1);
        Block dictionaryBlock = new BitArrayBlock(2, Optional.of(valueIsValid), toBitArray(mappingValues, 2));

        return wrapSingleWithNulls(juffersWE, rowsToFill, dictionaryBlock, 1);
    }

    @Override
    protected Block fillRawBlock(
            ReadJuffersWarmUpElement juffersWE,
            RecTypeCode recTypeCode,
            int recLength,
            int rowsToFill,
            boolean collectNulls)
    {
        ByteBuffer buff = (ByteBuffer) juffersWE.getRecordBuffer();
        byte[] values = new byte[rowsToFill];
        Optional<long[]> valueIsValidOptional;
        if (collectNulls) {
            ByteBuffer nullBuff = juffersWE.getNullBuffer();
            long[] valueIsValid = Bitmap.allocateWords(rowsToFill, true);
            for (int i = 0; i < rowsToFill; i++) {
                if (isNull(nullBuff, i)) {
                    Bitmap.clear(valueIsValid, 0, i);
                }
            }
            valueIsValidOptional = Optional.of(valueIsValid);
        }
        else {
            valueIsValidOptional = Optional.empty();
        }
        buff.get(values);
        return new BitArrayBlock(rowsToFill, valueIsValidOptional, toBitArray(values, rowsToFill));
    }

    // The storage engine keeps one byte per boolean; pack the first {@code length} bytes into the
    // bit representation that BitArrayBlock expects, treating any nonzero byte as true.
    static long[] toBitArray(byte[] values, int length)
    {
        long[] bits = new long[wordsForBits(length)];
        for (int position = 0; position < length; position++) {
            if (values[position] != 0) {
                set(bits, 0, position);
            }
        }
        return bits;
    }
}
