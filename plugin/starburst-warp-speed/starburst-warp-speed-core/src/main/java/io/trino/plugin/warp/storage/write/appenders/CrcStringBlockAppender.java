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
package io.trino.plugin.warp.storage.write.appenders;

import io.airlift.slice.Slice;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class CrcStringBlockAppender
        extends CrcBlockAppender
{
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final Type filterType;
    private final boolean isFixedLength;

    public CrcStringBlockAppender(
            WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            Type filterType,
            boolean isFixedLength)
    {
        super(juffersWE);

        this.storageEngineConstants = storageEngineConstants;
        this.bufferAllocator = bufferAllocator;
        this.filterType = filterType;
        this.isFixedLength = isFixedLength;
    }

    @Override
    public AppendResult appendValues(
            int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        // string length must be taken form type since the warm up type length represents the index length (maximum is 8)
        int stringLength = TypeUtils.getTypeLength(filterType, storageEngineConstants.getVarcharMaxLen());

        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(
                filterType,
                stringLength,
                isFixedLength,
                false);

        return switch (blockPos.getBlock()) {
            case RunLengthEncodedBlock rleBlock -> appendRepeatedValue((VariableWidthBlock) rleBlock.getValue(), jufferPos, blockPos, warmupElementStatsBuilder, stringLength, sliceConverter);
            case DictionaryBlock dictionaryBlock -> appendDictionaryBlock(dictionaryBlock, jufferPos, blockPos, warmupElementStatsBuilder, stringLength, sliceConverter);
            case ValueBlock valueBlock -> appendValueBlock((VariableWidthBlock) valueBlock, jufferPos, blockPos, warmupElementStatsBuilder, stringLength, sliceConverter);
        };
    }

    private AppendResult appendValueBlock(
            VariableWidthBlock valueBlock,
            int jufferPos,
            BlockPosHolder blockPos,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            int stringLength,
            Function<Slice, Slice> sliceConverter)
    {
        int nullsCount = 0;
        for (; blockPos.inRange(); blockPos.advance()) {
            int position = blockPos.getBlockPosition();
            if (valueBlock.isNull(position)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                nullsCount += appendSlice(valueBlock.getSlice(position), jufferPos, blockPos, warmupElementStatsBuilder, stringLength, sliceConverter);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendDictionaryBlock(
            DictionaryBlock dictionaryBlock,
            int jufferPos,
            BlockPosHolder blockPos,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            int stringLength,
            Function<Slice, Slice> sliceConverter)
    {
        VariableWidthBlock dictionary = (VariableWidthBlock) dictionaryBlock.getDictionary();
        // entry-level min/max is exact for a compact dictionary; the whole-block check avoids rescanning on chunked appends
        boolean statsFromDictionary = dictionaryBlock.isCompact() && blockPos.getNumEntries() == dictionaryBlock.getPositionCount();
        if (statsFromDictionary) {
            for (int position = 0; position < dictionary.getPositionCount(); position++) {
                if (!dictionary.isNull(position)) {
                    Slice slice = getSlice(dictionary.getSlice(position));
                    if (slice != null) {
                        warmupElementStatsBuilder.updateMinMax(slice);
                    }
                }
            }
        }
        int nullsCount = 0;
        for (; blockPos.inRange(); blockPos.advance()) {
            int position = dictionaryBlock.getId(blockPos.getBlockPosition());
            if (dictionary.isNull(position)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else if (statsFromDictionary) {
                nullsCount += appendSliceWithoutStats(dictionary.getSlice(position), jufferPos, blockPos, stringLength, sliceConverter);
            }
            else {
                nullsCount += appendSlice(dictionary.getSlice(position), jufferPos, blockPos, warmupElementStatsBuilder, stringLength, sliceConverter);
            }
        }
        return new AppendResult(nullsCount);
    }

    private AppendResult appendRepeatedValue(
            VariableWidthBlock valueBlock,
            int jufferPos,
            BlockPosHolder blockPos,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            int stringLength,
            Function<Slice, Slice> sliceConverter)
    {
        int nullsCount = 0;
        if (valueBlock.isNull(0)) {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            return new AppendResult(nullsCount);
        }
        Slice slice = getSlice(valueBlock.getSlice(0));
        if (slice == null) {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            return new AppendResult(nullsCount);
        }
        warmupElementStatsBuilder.updateMinMax(slice);
        Slice value = sliceConverter.apply(slice);
        for (; blockPos.inRange(); blockPos.advance()) {
            nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
            writeValue(blockPos, jufferPos, stringLength, value);
        }
        return new AppendResult(nullsCount);
    }

    // the getSlice hook can turn a value into null (e.g. a missing json field); reports 1 for such rows
    private int appendSlice(
            Slice rawSlice,
            int jufferPos,
            BlockPosHolder blockPos,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            int stringLength,
            Function<Slice, Slice> sliceConverter)
    {
        Slice slice = getSlice(rawSlice);
        if (slice == null) {
            nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
            return 1;
        }
        nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
        warmupElementStatsBuilder.updateMinMax(slice);
        Slice value = sliceConverter.apply(slice);
        writeValue(blockPos, jufferPos, stringLength, value);
        return 0;
    }

    // variant for callers that already collected min/max at the dictionary level
    private int appendSliceWithoutStats(
            Slice rawSlice,
            int jufferPos,
            BlockPosHolder blockPos,
            int stringLength,
            Function<Slice, Slice> sliceConverter)
    {
        Slice slice = getSlice(rawSlice);
        if (slice == null) {
            nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
            return 1;
        }
        nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
        Slice value = sliceConverter.apply(slice);
        writeValue(blockPos, jufferPos, stringLength, value);
        return 0;
    }

    protected Slice getSlice(Slice slice)
    {
        return slice;
    }

    @Override
    protected AppendResult appendFromMapBlock(
            BlockPosHolder blockPos,
            int jufferPos,
            Object key)
    {
        int stringLength = TypeUtils.getTypeLength(filterType, storageEngineConstants.getVarcharMaxLen());
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(
                filterType,
                stringLength,
                isFixedLength,
                false);
        int nullsCount = 0;
        Type valueType = ((MapType) blockPos.getType()).getValueType();
        for (; blockPos.inRange(); blockPos.advance()) {
            SqlMap elementBlock = (SqlMap) blockPos.getObject();
            int pos = elementBlock.seekKey(key);
            if (pos == -1 || elementBlock.getRawValueBlock().isNull(elementBlock.getRawOffset() + pos)) {
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                nullsCount++;
            }
            else {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                Slice slice = valueType.getSlice(elementBlock.getRawValueBlock(), elementBlock.getRawOffset() + pos);
                Slice value = sliceConverter.apply(slice);
                writeValue(blockPos, jufferPos, stringLength, value);
            }
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(
            BlockPosHolder blockPos,
            int jufferPos,
            int stringLength,
            Slice value)
    {
        ByteBuffer byteBuffer = value.toByteBuffer();
        long stringVal = SliceUtils.calcStringValue(byteBuffer, value.length(), stringLength, false);

        int crcJufferOffset;
        int valLength = stringLength;
        long crc;
        // check for optimization of power of two length char
        if (stringLength == 1) {
            byte val = byteBuffer.get();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else if (stringLength == 2) {
            short val = bufferAllocator.createBuffView(byteBuffer).getShort();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else if (stringLength == 4) {
            int val = bufferAllocator.createBuffView(byteBuffer).getInt();
            crcJufferOffset = crcJuffers.put(val, jufferPos, blockPos);
            crc = val;
        }
        else {
            crc = SliceUtils.calcCrc(byteBuffer, value.length());
            crcJufferOffset = crcJuffers.put(crc, jufferPos, blockPos);
            valLength = Long.BYTES;
        }
        juffersWE.updateRecordBufferProps(stringVal, crc, valLength, crcJufferOffset);
    }
}
