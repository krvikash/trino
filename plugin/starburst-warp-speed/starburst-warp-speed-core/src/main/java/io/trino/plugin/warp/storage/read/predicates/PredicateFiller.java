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
package io.trino.plugin.warp.storage.read.predicates;

import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.PredicateInfo;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateHeaderFlags;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;

import static io.trino.plugin.warp.WarpErrorCode.WARP_CONTROL;
import static java.lang.String.format;

public abstract class PredicateFiller
{
    protected static final byte BOOLEAN_TRUE_VALUE = 1;
    protected static final byte BOOLEAN_FALSE_VALUE = 0;

    protected final BufferAllocator bufferAllocator;

    public PredicateFiller(BufferAllocator bufferAllocator)
    {
        this.bufferAllocator = bufferAllocator;
    }

    /**
     * fill column predicate buffers with values/ranges/crcs according to predicate type:
     * ALL - constant buffer is allcoated
     * NONE - constant buffer is allocated
     * RANGES - each range is set as two fields: low and high
     * VALUES - each value is set as one field
     * STRING - crcs and then lexicographic (str2int) min max for all values
     * LUCENE - constant buffer is allocated
     */
    public abstract void fillPredicate(Domain domain, ByteBuffer predicateBuffer, PredicateData predicateData);

    public abstract PredicateType getPredicateType();

    protected ByteBuffer writePredicateInfoToBuffer(ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        PredicateInfo predicateInfo = predicateData.getPredicateInfo();
        byte typeAndFlags = (byte) predicateInfo.predicateType().ordinal();
        boolean hasFunction = predicateInfo.functionType() != FunctionType.FUNCTION_TYPE_NONE;
        if (hasFunction) {
            typeAndFlags = (byte) (typeAndFlags | (1 << PredicateHeaderFlags.PREDICATE_FLAG_HAS_FUNCTION.offset()));
        }
        if (predicateData.isCollectNulls()) {
            typeAndFlags = (byte) (typeAndFlags | (1 << PredicateHeaderFlags.PREDICATE_FLAG_COLLECT_NULL.offset()));
        }
        predicateBuffer.put(typeAndFlags);
        predicateBuffer.putInt(predicateInfo.numValues());
        if (hasFunction) {
            predicateBuffer.put((byte) predicateInfo.functionType().ordinal());
            if (predicateData.getColumnType() instanceof TimestampType) {
                predicateBuffer.put((byte) ((TimestampType) predicateData.getColumnType()).getPrecision());
            }
        }

        for (Object functionParam : predicateInfo.functionParams()) {
            if (functionParam instanceof Integer) {
                predicateBuffer.putInt((int) functionParam);
            }
            else {
                throw new UnsupportedOperationException(format("invalid functionParam=%s, predicateInfo=%s", functionParam, predicateInfo));
            }
        }
        return bufferAllocator.createBuffView(predicateBuffer.slice());
    }

    public abstract void convertValues(Domain domain, ByteBuffer predicateBuffer);

    // reads values through the concrete value-block classes rather than generic Type accessors
    protected static void writeValues(Block sortedRangesBlock, Type type, ByteBuffer predicateBuffer, int startPosition, int endPosition)
    {
        ValueBlock valueBlock = sortedRangesBlock.getUnderlyingValueBlock();
        if (TypeUtils.isIntType(type) || TypeUtils.isRealType(type)) {
            IntArrayBlock intArrayBlock = (IntArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                predicateBuffer.putInt(intArrayBlock.getInt(sortedRangesBlock.getUnderlyingValuePosition(i)));
            }
        }
        else if (TypeUtils.isLongType(type) || TypeUtils.isShortDecimalType(type)) {
            LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                predicateBuffer.putLong(longArrayBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i)));
            }
        }
        else if (TypeUtils.isDoubleType(type)) {
            LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                predicateBuffer.putDouble(Double.longBitsToDouble(longArrayBlock.getLong(sortedRangesBlock.getUnderlyingValuePosition(i))));
            }
        }
        else if (TypeUtils.isSmallIntType(type)) {
            ShortArrayBlock shortArrayBlock = (ShortArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                predicateBuffer.putShort(shortArrayBlock.getShort(sortedRangesBlock.getUnderlyingValuePosition(i)));
            }
        }
        else if (TypeUtils.isTinyIntType(type)) {
            ByteArrayBlock byteArrayBlock = (ByteArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                predicateBuffer.put(byteArrayBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i)));
            }
        }
        else if (TypeUtils.isBooleanType(type)) {
            ByteArrayBlock byteArrayBlock = (ByteArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                if (byteArrayBlock.getByte(sortedRangesBlock.getUnderlyingValuePosition(i)) != 0) {
                    predicateBuffer.put(BOOLEAN_TRUE_VALUE);
                }
                else {
                    predicateBuffer.put(BOOLEAN_FALSE_VALUE);
                }
            }
        }
        else if (TypeUtils.isLongDecimalType(type)) {
            Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) valueBlock;
            for (int i = startPosition; i < endPosition; i += 2) {
                int position = sortedRangesBlock.getUnderlyingValuePosition(i);
                predicateBuffer.putLong(int128ArrayBlock.getInt128High(position));
                predicateBuffer.putLong(int128ArrayBlock.getInt128Low(position));
            }
        }
        else {
            throw new TrinoException(WARP_CONTROL, "unexpected ValType " + type);
        }
    }
}
