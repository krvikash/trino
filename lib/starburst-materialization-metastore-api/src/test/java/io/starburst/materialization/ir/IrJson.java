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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.starburst.materialization.ir.IrFixtures.FixtureColumnId;
import io.starburst.materialization.ir.IrFixtures.FixtureTableId;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.materialization.ir.IrJsonMapper.irJsonMapper;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;

/**
 * JSON codecs for IR tests. The engine resolves {@link Type} strings through its type manager, which this module
 * cannot reach, so a small fixed set of trino-spi types stands in for it.
 */
final class IrJson
{
    static final RowType PERSON_ROW = RowType.from(ImmutableList.of(
            RowType.field("name", VARCHAR),
            RowType.field("age", BIGINT)));

    private static final Map<String, Type> TYPES = ImmutableList.of(
                    BOOLEAN,
                    BIGINT,
                    INTEGER,
                    DOUBLE,
                    DATE,
                    VARCHAR,
                    createVarcharType(10),
                    createCharType(5),
                    TIMESTAMP_MILLIS,
                    TIMESTAMP_TZ_MILLIS,
                    PERSON_ROW,
                    new ArrayType(BIGINT))
            .stream()
            .collect(toImmutableMap(type -> type.getTypeId().getId(), type -> type));

    private static final JsonCodecFactory CODEC_FACTORY = new JsonCodecFactory(irJsonMapper(new JsonMapperProvider()
            .withJsonDeserializers(ImmutableMap.of(Type.class, new FixtureTypeDeserializer()))
            .withModules(ImmutableSet.of(new SimpleModule()
                    .setMixInAnnotation(ConnectorTableId.class, TypedByName.class)
                    .setMixInAnnotation(ConnectorColumnId.class, TypedByName.class)
                    .registerSubtypes(
                            new NamedType(FixtureTableId.class, "fixture_table"),
                            new NamedType(FixtureColumnId.class, "fixture_column"))))
            .get()));

    private IrJson() {}

    static <T> JsonCodec<T> codec(Class<T> type)
    {
        return CODEC_FACTORY.jsonCodec(type);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    private interface TypedByName {}

    private static final class FixtureTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        FixtureTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = TYPES.get(value);
            if (type == null) {
                throw new IllegalArgumentException("Unknown type: " + value);
            }
            return type;
        }
    }
}
