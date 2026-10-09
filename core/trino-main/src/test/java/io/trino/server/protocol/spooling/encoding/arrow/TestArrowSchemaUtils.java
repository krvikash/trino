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
package io.trino.server.protocol.spooling.encoding.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.server.protocol.spooling.encoding.arrow.ArrowSchemaUtils.toArrowField;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;

class TestArrowSchemaUtils
{
    private static final List<Type> TIME_ZONE_TYPES = IntStream.of(0, 3, 6, 9)
            .boxed()
            .flatMap(precision -> List.<Type>of(createTimeWithTimeZoneType(precision), createTimestampWithTimeZoneType(precision)).stream())
            .toList();

    // One type for each case in toArrowField(), plus a type that falls under the default case
    private static final List<Type> TEST_TYPES = ImmutableList.<Type>builder()
            .add(new ArrayType(BOOLEAN))
            .add(new MapType(BOOLEAN, BOOLEAN, new TypeOperators()))
            .add(RowType.anonymousRow(BOOLEAN, BOOLEAN))
            .addAll(TIME_ZONE_TYPES)
            .add(BOOLEAN)
            .build();

    @Test
    void testTopLevelNullabilityFollowsArgument()
    {
        for (Type type : TEST_TYPES) {
            assertThat(toArrowField("column", type, true).isNullable())
                    .as("nullable %s", type)
                    .isTrue();
            assertThat(toArrowField("column", type, false).isNullable())
                    .as("non-nullable %s", type)
                    .isFalse();
        }
    }

    @Test
    void testMapKeysAreNotNullable()
    {
        for (Type keyType : TEST_TYPES) {
            for (boolean nullable : List.of(true, false)) {
                Field map = toArrowField("column", new MapType(keyType, BOOLEAN, new TypeOperators()), nullable);
                Field entries = map.getChildren().getFirst();
                assertThat(entries.isNullable())
                        .as("entries of map(%s, boolean), nullable=%s", keyType, nullable)
                        .isFalse();
                assertThat(entries.getChildren().get(0).isNullable())
                        .as("key of map(%s, boolean), nullable=%s", keyType, nullable)
                        .isFalse();
            }
        }
    }

    @Test
    void testMapOfMapsNullability()
    {
        // map(map(timestamp(3) with time zone, boolean), map(timestamp(3) with time zone, boolean))
        MapType innerMap = new MapType(createTimestampWithTimeZoneType(3), BOOLEAN, new TypeOperators());
        for (boolean nullable : List.of(true, false)) {
            Field field = toArrowField("column", new MapType(innerMap, innerMap, new TypeOperators()), nullable);
            Field entries = field.getChildren().getFirst();

            Field key = entries.getChildren().get(0);
            Field value = entries.getChildren().get(1);
            assertThat(key.isNullable()).as("top-level key, nullable=%s", nullable).isFalse();
            assertThat(value.isNullable()).as("top-level value, nullable=%s", nullable).isTrue();

            // Both the key and the value are maps themselves
            for (Field nestedMap : List.of(key, value)) {
                Field nestedEntries = nestedMap.getChildren().getFirst();
                assertThat(nestedEntries.getChildren().get(0).isNullable())
                        .as("nested key of %s, nullable=%s", nestedMap.getName(), nullable)
                        .isFalse();
                assertThat(nestedEntries.getChildren().get(1).isNullable())
                        .as("nested value of %s, nullable=%s", nestedMap.getName(), nullable)
                        .isTrue();
            }
        }
    }

    @Test
    void testArrayElementsAreNullable()
    {
        // array(array(boolean)): both the outer and the nested elements
        Type type = new ArrayType(new ArrayType(BOOLEAN));
        for (boolean nullable : List.of(true, false)) {
            Field element = toArrowField("column", type, nullable).getChildren().getFirst();
            assertThat(element.isNullable()).as("element, nullable=%s", nullable).isTrue();
            assertThat(element.getChildren().getFirst().isNullable()).as("nested element, nullable=%s", nullable).isTrue();
        }
    }

    @Test
    void testRowFieldsAreNullable()
    {
        // row(boolean, row(boolean, boolean)): both the outer and the nested fields
        Type type = RowType.anonymousRow(BOOLEAN, RowType.anonymousRow(BOOLEAN, BOOLEAN));
        for (boolean nullable : List.of(true, false)) {
            Field row = toArrowField("column", type, nullable);
            assertThat(row.getChildren()).hasSize(2);
            for (Field field : row.getChildren()) {
                assertThat(field.isNullable()).as("field %s, nullable=%s", field.getName(), nullable).isTrue();
            }
            for (Field field : row.getChildren().getLast().getChildren()) {
                assertThat(field.isNullable()).as("nested field %s, nullable=%s", field.getName(), nullable).isTrue();
            }
        }
    }

    @Test
    void testTimeZoneTypeChildrenAreNullable()
    {
        // Only the struct itself follows the requested nullability; its children stay nullable
        for (Type type : TIME_ZONE_TYPES) {
            for (boolean nullable : List.of(true, false)) {
                for (Field child : toArrowField("column", type, nullable).getChildren()) {
                    assertThat(child.isNullable())
                            .as("child %s of %s, nullable=%s", child.getName(), type, nullable)
                            .isTrue();
                }
            }
        }
    }
}
