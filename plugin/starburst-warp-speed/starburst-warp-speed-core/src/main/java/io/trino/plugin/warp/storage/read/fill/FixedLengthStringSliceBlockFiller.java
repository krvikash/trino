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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dictionary.ReadDictionary;
import io.trino.plugin.warp.juffer.ByteBufferInputStream;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Optional;

public class FixedLengthStringSliceBlockFiller
        extends SliceBlockFiller
{
    public FixedLengthStringSliceBlockFiller(
            DictionaryCacheService dictionaryCacheService,
            StorageEngineConstants storageEngineConstants,
            NativeConfig nativeConfig)
    {
        super(dictionaryCacheService, CharType.createCharType(10), storageEngineConstants, nativeConfig); // need to check if length 10 is ok for all
    }

    @Override
    protected int copyFromJufferOneRecord(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int currPos,
            int recLength,
            int jufferOffset,
            Slice outputSlice,
            int outputSliceOffset)
            throws IOException
    {
        jufferOffset = currPos * recLength; // override the provided one
        int trimmedRecLength = SliceUtils.trimSlice(recordBuff, recLength, jufferOffset);
        outputSlice.setBytes(outputSliceOffset, new ByteBufferInputStream(recordBuff, jufferOffset), trimmedRecLength);
        return trimmedRecLength;
    }

    @Override
    protected void copyFromJufferMultipleRecords(ByteBuffer recordBuff,
            ShortBuffer lenBuff,
            int recLength,
            int numRecords,
            Slice outputSlice,
            int[] offsets)
            throws IOException
    {
        int jufferOffset = 0;
        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(recordBuff);
        for (int currPos = 0; currPos < numRecords; currPos++) {
            int trimmedRecLength = SliceUtils.trimSlice(recordBuff, recLength, jufferOffset);
            outputSlice.setBytes(offsets[currPos], byteBufferInputStream, trimmedRecLength);
            jufferOffset += recLength;
            long _ = byteBufferInputStream.skip(recLength - trimmedRecLength);
            offsets[currPos + 1] = offsets[currPos] + trimmedRecLength;
        }
    }

    @Override
    protected void copyFromDictionary(ShortBuffer buff,
            ReadDictionary readDictionary,
            int currPos,
            Slice outputSlice,
            int[] offsets)
    {
        Slice slice = (Slice) readDictionary.get(Short.toUnsignedInt(buff.get(currPos)));
        int trimmedRecLength = SliceUtils.trimSlice(slice.toByteBuffer(), slice.length(), 0);
        outputSlice.setBytes(offsets[currPos], slice, 0, trimmedRecLength);
        offsets[currPos + 1] = offsets[currPos] + trimmedRecLength;
    }

    @Override
    protected Block createSingleValueBlock(Type spiType, Slice slice, int rowsToFill)
    {
        if (slice == null) {
            return super.createSingleValueBlock(spiType, null, rowsToFill);
        }
        Block res;
        int trimmedRecLength = SliceUtils.trimSlice(slice.toByteBuffer(), slice.length(), 0);
        Slice outputSlice = Slices.wrappedBuffer(slice.byteArray(), slice.byteArrayOffset(), trimmedRecLength);
        if (outputSlice.length() > 0 && outputSlice.getByte(outputSlice.length() - 1) == ' ') {
            int[] dictOffsets = SliceUtils.allocateOffsetsArray(1);
            dictOffsets[1] = trimmedRecLength;
            Block dictionary = new VariableWidthBlock(1, outputSlice, dictOffsets, Optional.empty());
            int[] ids = new int[rowsToFill];
            res = DictionaryBlock.create(ids.length, dictionary, ids);
        }
        else {
            res = RunLengthEncodedBlock.create(spiBuilderType, outputSlice, rowsToFill);
        }
        return res;
    }
}
