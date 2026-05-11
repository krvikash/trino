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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.FormatValidator;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.TrueSchema;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.schema.confluent.EmptyFieldStrategy.DUMMY_ROW_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

final class TestJsonSchemaConverter
{
    @Test
    void testConvertObjectSchema()
    {
        // The ObjectSchema not guarantees that the properties are in the same order as they were added
        ObjectSchema schema = ObjectSchema.builder()
                .addPropertySchema("bool_col", BooleanSchema.builder().build())
                .addPropertySchema("long_col", NumberSchema.builder().requiresInteger(true).build())
                .addPropertySchema("double_col", NumberSchema.builder().requiresInteger(false).build())
                .addPropertySchema("string_col", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("timestamp_col", StringSchema.builder().requiresString(false).formatValidator(FormatValidator.forFormat("date-time")).build())
                .addPropertySchema("date_col", StringSchema.builder().requiresString(false).formatValidator(new DateFormatValidator()).build())
                .addPropertySchema("time_col", StringSchema.builder().requiresString(false).formatValidator(new TimeFormatValidator()).build())
                .addPropertySchema("const_col", ConstSchema.builder().permittedValue("this_is_constant").build())
                .addPropertySchema("enum_col", EnumSchema.builder().possibleValues(ImmutableSet.of("blue", "red", "yellow")).build())
                .addPropertySchema("enum_col_num", EnumSchema.builder().possibleValues(ImmutableSet.of(1, 3, 3L)).build())
                .addPropertySchema("arr_col", ArraySchema.builder().addItemSchema(
                        ObjectSchema.builder().addPropertySchema("obj_int", NumberSchema.builder().requiresInteger(true).build()).build()).build())
                .addPropertySchema("combine_col", CombinedSchema.oneOf(ImmutableSet.of(
                        NumberSchema.builder().requiresInteger(true).build(),
                        NullSchema.builder().build())).build())
                .addPropertySchema("row_col", ObjectSchema.builder()
                        .addPropertySchema("one_of_int", CombinedSchema.oneOf(ImmutableSet.of(
                                NumberSchema.builder().requiresInteger(true).build(),
                                NullSchema.builder().build())).build())
                        .build())
                .build();

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        Map<String, Type> types = jsonSchemaConverter.convertJsonSchema(schema, "testSubject-key").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));

        assertThat(types).containsAllEntriesOf(ImmutableMap.<String, Type>builder()
                .put("bool_col", BOOLEAN)
                .put("long_col", BIGINT)
                .put("double_col", DOUBLE)
                .put("string_col", VARCHAR)
                .put("timestamp_col", TIMESTAMP_TZ_MILLIS)
                .put("date_col", DATE)
                .put("time_col", TIME_TZ_MILLIS)
                .put("const_col", VARCHAR)
                .put("enum_col", VARCHAR)
                .put("enum_col_num", BIGINT)
                .put("arr_col", new ArrayType(RowType.rowType(new RowType.Field(Optional.of("obj_int"), BIGINT))))
                .put("combine_col", BIGINT)
                .put("row_col", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("one_of_int"), BIGINT))
                        .build()))
                .buildOrThrow());
    }

    @Test
    void testConvertPrimitiveType()
    {
        assertType(BooleanSchema.builder().build(), BOOLEAN);
        assertType(NumberSchema.builder().requiresInteger(true).build(), BIGINT);
        assertType(NumberSchema.builder().requiresInteger(false).build(), DOUBLE);
        assertType(StringSchema.builder().requiresString(true).build(), VARCHAR);
        assertType(StringSchema.builder().requiresString(false).formatValidator(FormatValidator.forFormat("date-time")).build(), TIMESTAMP_TZ_MILLIS);
        assertType(StringSchema.builder().requiresString(false).formatValidator(new DateFormatValidator()).build(), DATE);
        assertType(StringSchema.builder().requiresString(false).formatValidator(new TimeFormatValidator()).build(), TIME_TZ_MILLIS);
        assertType(ConstSchema.builder().permittedValue("this_is_constant").build(), VARCHAR);
        assertType(EnumSchema.builder().possibleValues(ImmutableSet.of("blue", "red", "yellow")).build(), VARCHAR);
        assertType(EnumSchema.builder().possibleValues(ImmutableSet.of(1, 3, 3L)).build(), BIGINT);
        assertType(ArraySchema.builder().addItemSchema(NumberSchema.builder().requiresInteger(true).build()).build(), new ArrayType(BIGINT));
    }

    private static void assertType(Schema schema, Type expectedType)
    {
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        Map<String, Type> types = jsonSchemaConverter.convertJsonSchema(schema, "testSubject-key").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));
        assertThat(types).containsAllEntriesOf(ImmutableMap.<String, Type>builder().put("testSubject-key", expectedType).buildOrThrow());
    }

    @Test
    void testConvertObjectSchemaWithNullables()
    {
        // The ObjectSchema not guarantees that the properties are in the same order as they were added
        ObjectSchema schema = ObjectSchema.builder()
                .addPropertySchema("bool_col", BooleanSchema.builder().nullable(true).build())
                .addPropertySchema("long_col", NumberSchema.builder().requiresInteger(true).nullable(true).build())
                .addPropertySchema("double_col", NumberSchema.builder().requiresInteger(false).nullable(true).build())
                .addPropertySchema("string_col", StringSchema.builder().requiresString(true).nullable(true).build())
                .addPropertySchema("timestamp_col", StringSchema.builder().requiresString(false).formatValidator(FormatValidator.forFormat("date-time")).nullable(true).build())
                .addPropertySchema("date_col", StringSchema.builder().requiresString(false).formatValidator(new DateFormatValidator()).nullable(true).build())
                .addPropertySchema("time_col", StringSchema.builder().requiresString(false).formatValidator(new TimeFormatValidator()).nullable(true).build())
                .addPropertySchema("enum_col", EnumSchema.builder().possibleValues(ImmutableSet.of("blue", "red", "yellow")).nullable(true).build())
                .addPropertySchema("arr_col", ArraySchema.builder().addItemSchema(NumberSchema.builder().requiresInteger(true).build()).nullable(true).build())
                .addPropertySchema("combine_col", CombinedSchema.oneOf(ImmutableSet.of(
                        NumberSchema.builder().requiresInteger(true).nullable(true).build(),
                        NullSchema.builder().build())).build())
                .addPropertySchema("row_col", ObjectSchema.builder()
                        .addPropertySchema("int_col", NumberSchema.builder().requiresInteger(true).nullable(true).build())
                        .nullable(true)
                        .build())
                .build();

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        Map<String, Type> types = jsonSchemaConverter.convertJsonSchema(schema, "subject").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));
        assertThat(types).containsAllEntriesOf(ImmutableMap.<String, Type>builder()
                .put("bool_col", BOOLEAN)
                .put("long_col", BIGINT)
                .put("double_col", DOUBLE)
                .put("string_col", VARCHAR)
                .put("timestamp_col", TIMESTAMP_TZ_MILLIS)
                .put("date_col", DATE)
                .put("time_col", TIME_TZ_MILLIS)
                .put("enum_col", VARCHAR)
                .put("arr_col", new ArrayType(BIGINT))
                .put("combine_col", BIGINT)
                .put("row_col", RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("int_col"), BIGINT))
                        .build()))
                .buildOrThrow());
    }

    @Test
    void testConvertPrimitiveTypeWithNullables()
    {
        assertType(BooleanSchema.builder().nullable(true).build(), BOOLEAN);
        assertType(NumberSchema.builder().requiresInteger(true).nullable(true).build(), BIGINT);
        assertType(NumberSchema.builder().requiresInteger(false).nullable(true).build(), DOUBLE);
        assertType(StringSchema.builder().requiresString(true).nullable(true).build(), VARCHAR);
        assertType(StringSchema.builder().requiresString(false).formatValidator(FormatValidator.forFormat("date-time")).nullable(true).build(), TIMESTAMP_TZ_MILLIS);
        assertType(StringSchema.builder().requiresString(false).formatValidator(new DateFormatValidator()).nullable(true).build(), DATE);
        assertType(StringSchema.builder().requiresString(false).formatValidator(new TimeFormatValidator()).nullable(true).build(), TIME_TZ_MILLIS);
        assertType(ConstSchema.builder().permittedValue("this_is_constant").nullable(true).build(), VARCHAR);
        assertType(EnumSchema.builder().possibleValues(ImmutableSet.of("blue", "red", "yellow")).nullable(true).build(), VARCHAR);
        assertType(EnumSchema.builder().possibleValues(ImmutableSet.of(1, 3, 3L)).nullable(true).build(), BIGINT);
        assertType(ArraySchema.builder().addItemSchema(NumberSchema.builder().requiresInteger(true).build()).nullable(true).build(), new ArrayType(BIGINT));
    }

    @Test
    void testEmptyFieldStrategy()
    {
        ObjectSchema schema = ObjectSchema.builder()
                .addPropertySchema("int_col", NumberSchema.builder().requiresInteger(true).build())
                .addPropertySchema("row_col", ObjectSchema.builder()
                        .addPropertySchema("empty", ObjectSchema.builder().build())
                        .addPropertySchema("int_col", NumberSchema.builder().requiresInteger(true).build()).build())
                .addPropertySchema("arr_col", ArraySchema.builder()
                        .addItemSchema(ObjectSchema.builder().build())
                        .build())
                .build();

        JsonSchemaConverter jsonSchemaConverterIgnore = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        Map<String, Type> types = jsonSchemaConverterIgnore.convertJsonSchema(schema, "subject").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));
        assertThat(types).isEqualTo(ImmutableMap.of(
                "int_col", BIGINT,
                "row_col", RowType.from(ImmutableList.of(RowType.field("int_col", BIGINT), RowType.field("empty", RowType.rowType()))),
                "arr_col", new ArrayType(RowType.rowType())));

        JsonSchemaConverter jsonSchemaConverterFail = new JsonSchemaConverter(EmptyFieldStrategy.FAIL);
        assertThatThrownBy(() -> jsonSchemaConverterFail.convertJsonSchema(schema, "subject"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ObjectSchema type has no valid field");

        JsonSchemaConverter jsonSchemaConverterMark = new JsonSchemaConverter(EmptyFieldStrategy.MARK);
        ImmutableMap<String, Type> markedTypes = jsonSchemaConverterMark.convertJsonSchema(schema, "subject").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));
        assertThat(markedTypes).isEqualTo(ImmutableMap.of(
                "int_col", BIGINT,
                "row_col", RowType.from(ImmutableList.of(RowType.field("int_col", BIGINT), RowType.field("empty", DUMMY_ROW_TYPE))),
                "arr_col", new ArrayType(DUMMY_ROW_TYPE)));
    }

    @Test
    void testUnsupportedValidationSchemas()
    {
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);

        ObjectSchema notSupportedConditionalSchema = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                ConditionalSchema.builder().ifSchema(TrueSchema.builder().build()).thenSchema(TrueSchema.builder().build()).build()).build();

        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedConditionalSchema, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Schema ConditionalSchema not supported");

        ObjectSchema notSupportedNotSchema = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                NotSchema.builder().mustNotMatch(StringSchema.builder().build()).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedNotSchema, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Schema NotSchema not supported");

        assertThatThrownBy(() -> jsonSchemaConverter.convertJsonSchema(TrueSchema.builder().build(), "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Schema TrueSchema not supported");

        assertThatThrownBy(() -> jsonSchemaConverter.convertJsonSchema(FalseSchema.builder().build(), "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Schema FalseSchema not supported");
    }

    @Test
    void testAnyOfMergesIncompatibleRowTypes()
    {
        // anyOf with $ref to multiple object schemas with different fields
        // should merge into a superset RowType (union of all fields)
        ObjectSchema roleOnly = ObjectSchema.builder()
                .addPropertySchema("role_name", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("policy_id", StringSchema.builder().requiresString(true).build())
                .build();
        ObjectSchema roleWithGroup = ObjectSchema.builder()
                .addPropertySchema("role_name", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("ad_groups", ArraySchema.builder().allItemSchema(StringSchema.builder().requiresString(true).build()).build())
                .addPropertySchema("remove_user", CombinedSchema.anyOf(ImmutableList.of(
                        BooleanSchema.builder().build(), NullSchema.builder().build())).build())
                .addPropertySchema("policy_id", StringSchema.builder().requiresString(true).build())
                .build();
        ObjectSchema roleWithEntitlement = ObjectSchema.builder()
                .addPropertySchema("catalog_name", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("role_name", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("ad_groups", ArraySchema.builder().allItemSchema(StringSchema.builder().requiresString(true).build()).build())
                .addPropertySchema("grants", ArraySchema.builder().allItemSchema(
                        ObjectSchema.builder()
                                .addPropertySchema("entitlement_id", StringSchema.builder().requiresString(true).build())
                                .addPropertySchema("table_name", StringSchema.builder().requiresString(true).build())
                                .addPropertySchema("schema_name", StringSchema.builder().requiresString(true).build())
                                .build()).build())
                .addPropertySchema("policy_id", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("output_port_id", StringSchema.builder().requiresString(true).build())
                .build();

        ObjectSchema schema = ObjectSchema.builder()
                .addPropertySchema("correlationId", StringSchema.builder().requiresString(true).build())
                .addPropertySchema("entitlements", CombinedSchema.anyOf(ImmutableList.of(roleOnly, roleWithGroup, roleWithEntitlement)).build())
                .build();

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        Map<String, Type> types = jsonSchemaConverter.convertJsonSchema(schema, "subject").stream()
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));

        assertThat(types).hasSize(2);
        assertThat(types.get("correlationId")).isEqualTo(VARCHAR);

        RowType entitlementsType = (RowType) types.get("entitlements");
        assertThat(entitlementsType.getFields())
                .extracting(field -> field.getName().orElseThrow(), RowType.Field::getType)
                .containsExactlyInAnyOrder(
                        tuple("role_name", VARCHAR),
                        tuple("policy_id", VARCHAR),
                        tuple("ad_groups", new ArrayType(VARCHAR)),
                        tuple("remove_user", BOOLEAN),
                        tuple("catalog_name", VARCHAR),
                        tuple("grants", new ArrayType(RowType.from(ImmutableList.<RowType.Field>builder()
                                .add(new RowType.Field(Optional.of("entitlement_id"), VARCHAR))
                                .add(new RowType.Field(Optional.of("schema_name"), VARCHAR))
                                .add(new RowType.Field(Optional.of("table_name"), VARCHAR))
                                .build()))),
                        tuple("output_port_id", VARCHAR));
    }

    @Test
    void testAnyOfRowTypeMergeConflictingFieldTypes()
    {
        // Two objects with same field name but different types should fail
        ObjectSchema objA = ObjectSchema.builder()
                .addPropertySchema("value", StringSchema.builder().requiresString(true).build())
                .build();
        ObjectSchema objB = ObjectSchema.builder()
                .addPropertySchema("value", NumberSchema.builder().requiresInteger(true).build())
                .build();

        ObjectSchema schema = ObjectSchema.builder()
                .addPropertySchema("field", CombinedSchema.anyOf(ImmutableList.of(objA, objB)).build())
                .build();

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);
        assertThatThrownBy(() -> jsonSchemaConverter.convertJsonSchema(schema, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Field 'value' has conflicting types");
    }

    @Test
    void testUnsupportedCombineType()
    {
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE);

        ObjectSchema notSupportedIntegerBoolean = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(NumberSchema.builder().requiresInteger(true).build(), BooleanSchema.builder().build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedIntegerBoolean, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [bigint, boolean]");

        ObjectSchema notSupportedIntegerFloat = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(NumberSchema.builder().requiresInteger(true).build(), NumberSchema.builder().minimum(Float.MIN_VALUE).maximum(Float.MAX_VALUE).build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedIntegerFloat, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [bigint, double]");

        ObjectSchema notSupportedStringBoolean = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(StringSchema.builder().requiresString(true).build(), BooleanSchema.builder().build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedStringBoolean, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [varchar, boolean]");

        ObjectSchema notSupportedStringInteger = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(StringSchema.builder().requiresString(true).build(), NumberSchema.builder().requiresInteger(true).build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedStringInteger, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [varchar, bigint]");

        ObjectSchema notSupportedStringDouble = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(StringSchema.builder().requiresString(true).build(), NumberSchema.builder().requiresInteger(false).minimum(Double.MIN_VALUE).maximum(Double.MAX_VALUE).build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedStringDouble, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [varchar, double]");

        ObjectSchema notSupportedDoubleBoolean = ObjectSchema.builder().addPropertySchema(
                "unsupported",
                CombinedSchema.oneOf(ImmutableList.of(NumberSchema.builder().requiresInteger(false).minimum(Double.MIN_VALUE).maximum(Double.MAX_VALUE).build(), BooleanSchema.builder().build())).build()).build();
        assertThatThrownBy(() ->
                jsonSchemaConverter.convertJsonSchema(notSupportedDoubleBoolean, "subject"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Incompatible types: [double, boolean]");
    }
}
