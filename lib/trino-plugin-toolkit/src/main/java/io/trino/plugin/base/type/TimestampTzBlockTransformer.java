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
package io.trino.plugin.base.type;

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static java.util.Objects.requireNonNull;

/**
 * Updates the timestamp to a new time zone without changing the underlying instant.
 */
public class TimestampTzBlockTransformer
        implements Function<Block, Block>
{
    private final Type sourceType;
    private final DateTimeZone dateTimeZone;

    public TimestampTzBlockTransformer(Type sourceType, DateTimeZone dateTimeZone)
    {
        this.sourceType = requireNonNull(sourceType, "sourceType is null");
        this.dateTimeZone = requireNonNull(dateTimeZone, "dateTimeZone is null");
    }

    @Override
    public Block apply(Block block)
    {
        return getModifiedTimestampTzBlock(block, sourceType);
    }

    private Block getModifiedTimestampTzBlock(Block block, Type sourceType)
    {
        switch (sourceType) {
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> {
                if (block instanceof DictionaryBlock dictionaryBlock) {
                    return DictionaryBlock.create(
                            dictionaryBlock.getPositionCount(),
                            modifyTimestampTzBlock(dictionaryBlock.getDictionary(), timestampWithTimeZoneType),
                            dictionaryBlock.getRawIds());
                }
                if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
                    return RunLengthEncodedBlock.create(
                            modifyTimestampTzBlock(runLengthEncodedBlock.getValue(), timestampWithTimeZoneType),
                            runLengthEncodedBlock.getPositionCount());
                }
                return modifyTimestampTzBlock(block, timestampWithTimeZoneType);
            }
            case RowType rowType -> {
                List<RowType.Field> fields = rowType.getFields();
                List<Block> fieldBlocks = getRowFieldsFromBlock(block);
                Block[] transformedFieldBlocks = new Block[fieldBlocks.size()];
                for (int i = 0; i < fields.size(); i++) {
                    transformedFieldBlocks[i] = getModifiedTimestampTzBlock(fieldBlocks.get(i), fields.get(i).getType());
                }
                return RowBlock.fromNotNullSuppressedFieldBlocks(block.getPositionCount(), getNulls(block), transformedFieldBlocks);
            }
            case ArrayType arrayType -> {
                ColumnarArray columnarArray = toColumnarArray(block);
                Block updatedElementsBlock = getModifiedTimestampTzBlock(columnarArray.getElementsBlock(), arrayType.getElementType());

                int positionCount = columnarArray.getPositionCount();
                int[] offsets = new int[positionCount + 1];
                for (int position = 0; position < positionCount; position++) {
                    offsets[position + 1] = columnarArray.getOffset(position + 1);
                }
                return ArrayBlock.fromElementBlock(columnarArray.getPositionCount(), getNulls(block), offsets, updatedElementsBlock);
            }
            case MapType mapType -> {
                ColumnarMap columnarMap = toColumnarMap(block);
                Block updatedKeysBlock = getModifiedTimestampTzBlock(columnarMap.getKeysBlock(), mapType.getKeyType());
                Block updatedValuesBlock = getModifiedTimestampTzBlock(columnarMap.getValuesBlock(), mapType.getValueType());

                int positionCount = columnarMap.getPositionCount();
                int[] offsets = new int[positionCount + 1];
                for (int position = 0; position < positionCount; position++) {
                    offsets[position + 1] = columnarMap.getOffset(position + 1);
                }
                return MapBlock.fromKeyValueBlock(getNulls(block), offsets, updatedKeysBlock, updatedValuesBlock, mapType);
            }
            default -> {
                return block;
            }
        }
    }

    private Block modifyTimestampTzBlock(Block block, TimestampWithTimeZoneType sourceType)
    {
        if (sourceType.isShort()) {
            return modifyShortTimestampTzBlock(block);
        }
        return modifyLongTimestampTzBlock(block);
    }

    private Block modifyShortTimestampTzBlock(Block block)
    {
        int positionCount = block.getPositionCount();
        LongArrayBlockBuilder builder = new LongArrayBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long timestampMillis = unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position));
            builder.writeLong(packDateTimeWithZone(timestampMillis, dateTimeZone.getID()));
        }
        return builder.build();
    }

    private Block modifyLongTimestampTzBlock(Block block)
    {
        int positionCount = block.getPositionCount();
        Fixed12BlockBuilder builder = new Fixed12BlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // getTimestampTzNanos extracts a LongTimestampWithTimeZone,
            // correctly handling both microsecond and nanosecond precision.
            // The extracted value is then used for time zone adjustment.
            LongTimestampWithTimeZone timestampWithTimeZone = getTimestampTzNanos(block, position);
            builder.writeFixed12(
                    packDateTimeWithZone(timestampWithTimeZone.getEpochMillis(), dateTimeZone.getID()),
                    timestampWithTimeZone.getPicosOfMilli());
        }
        return builder.build();
    }

    private static Optional<boolean[]> getNulls(Block block)
    {
        if (!block.mayHaveNull()) {
            return Optional.empty();
        }

        boolean[] valueIsNull = new boolean[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            valueIsNull[i] = block.isNull(i);
        }
        return Optional.of(valueIsNull);
    }

    public static boolean timestampTzBlockTransformationRequired(Type sourceType)
    {
        return switch (sourceType) {
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> TIMESTAMP_TZ_MILLIS.equals(timestampWithTimeZoneType)
                    || TIMESTAMP_TZ_MICROS.equals(timestampWithTimeZoneType)
                    || TIMESTAMP_TZ_NANOS.equals(timestampWithTimeZoneType);
            case RowType rowType -> rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .anyMatch(TimestampTzBlockTransformer::timestampTzBlockTransformationRequired);
            case ArrayType arrayType -> timestampTzBlockTransformationRequired(arrayType.getElementType());
            case MapType mapType -> timestampTzBlockTransformationRequired(mapType.getKeyType()) || timestampTzBlockTransformationRequired(mapType.getValueType());
            default -> false;
        };
    }

    private static LongTimestampWithTimeZone getTimestampTzNanos(Block block, int position)
    {
        return (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, position);
    }
}
