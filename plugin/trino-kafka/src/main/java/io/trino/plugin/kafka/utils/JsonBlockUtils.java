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
package io.trino.plugin.kafka.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.DefaultJsonFieldDecoder;
import io.trino.decoder.json.ISO8601JsonFieldDecoder;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.String.format;

public final class JsonBlockUtils
{
    private JsonBlockUtils() {}

    public static void write(BlockBuilder resultBuilder, JsonNode value, Type type)
    {
        if (value == null || value.isMissingNode() || value.isNull()) {
            resultBuilder.appendNull();
            return;
        }

        if (type instanceof ArrayType arrayType) {
            writeArray(resultBuilder, value, arrayType);
            return;
        }

        if (type instanceof RowType rowType) {
            writeRow(resultBuilder, value, rowType);
            return;
        }

        KafkaColumnHandle column = new KafkaColumnHandle("$internal", type, null, null, null, false, false, false);
        if (type == TIMESTAMP_TZ_MILLIS || type == TIME_TZ_MILLIS || type == DATE) {
            ISO8601JsonFieldDecoder iso8601JsonFieldDecoder = new ISO8601JsonFieldDecoder(column);
            FieldValueProvider provider = iso8601JsonFieldDecoder.decode(value);
            if (provider.isNull()) {
                resultBuilder.appendNull();
                return;
            }
            type.writeLong(resultBuilder, provider.getLong());
            return;
        }

        DefaultJsonFieldDecoder decoder = new DefaultJsonFieldDecoder(column);
        FieldValueProvider provider = decoder.decode(value);
        if (provider.isNull()) {
            resultBuilder.appendNull();
            return;
        }
        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(resultBuilder, provider.getBoolean());
            return;
        }

        if (type.getJavaType() == long.class) {
            type.writeLong(resultBuilder, provider.getLong());
            return;
        }

        if (type.getJavaType() == double.class) {
            type.writeDouble(resultBuilder, provider.getDouble());
            return;
        }

        if (type.getJavaType() == Slice.class) {
            type.writeSlice(resultBuilder, truncateToLength(provider.getSlice(), type));
            return;
        }

        throw new UnsupportedOperationException(format("Unsupported type %s for column %s", type.getDisplayName(), column.getName()));
    }

    private static void writeRow(BlockBuilder resultBuilder, JsonNode value, RowType type)
    {
        if (value == null || value.isMissingNode() || value.isNull()) {
            resultBuilder.appendNull();
            return;
        }

        checkArgument(value instanceof ObjectNode, "Expected ObjectNode, but got %s", value.getClass().getSimpleName());

        type.writeObject(resultBuilder, buildRowValue(type, fieldBuilders -> {
            List<RowType.Field> fields = type.getFields();
            ObjectNode objectNode = (ObjectNode) value;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                write(fieldBuilders.get(i), objectNode.get(field.getName().orElseThrow()), field.getType());
            }
        }));
    }

    private static void writeArray(BlockBuilder resultBuilder, JsonNode value, ArrayType type)
    {
        if (value == null || value.isMissingNode() || value.isNull()) {
            resultBuilder.appendNull();
            return;
        }

        checkArgument(value instanceof ArrayNode, "Expected ArrayNode, but got %s", value.getClass().getSimpleName());

        Type elementType = type.getElementType();
        BlockBuilder builder = elementType.createBlockBuilder(null, value.size());
        for (JsonNode element : value) {
            write(builder, element, elementType);
        }
        type.writeObject(resultBuilder, builder.build());
    }
}
