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
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.TrueSchema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.schema.confluent.EmptyFieldStrategy.DUMMY_ROW_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonSchemaConverter
{
    private final SchemaConverter schemaConverter;

    public JsonSchemaConverter(EmptyFieldStrategy emptyFieldStrategy)
    {
        this.schemaConverter = new SchemaConverter(requireNonNull(emptyFieldStrategy, "emptyFieldStrategy is null"));
    }

    public List<KafkaTopicFieldDescription> convertJsonSchema(Schema schema, String subject)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(subject, "subject is null");

        if (!(schema instanceof ObjectSchema objectSchema)) {
            // this probably the subject key schema, which could be a non object schema
            Optional<Type> type = schemaConverter.convert(schema);
            if (type.isEmpty()) {
                throw new IllegalStateException(format("Schema '%s' has no valid type", schema));
            }
            return ImmutableList.of(new KafkaTopicFieldDescription(
                    subject,
                    type.get(),
                    "",
                    null,
                    getDataFormat(type.get()),
                    null,
                    false));
        }

        ImmutableList.Builder<KafkaTopicFieldDescription> fieldDescriptionBuilder = ImmutableList.builder();
        for (Map.Entry<String, Schema> entry : objectSchema.getPropertySchemas().entrySet()) {
            String fieldName = entry.getKey();
            Schema fieldSchema = entry.getValue();

            Type type = schemaConverter.convert(fieldSchema)
                    .orElseThrow(() -> new IllegalStateException(format("Field '%s' has no valid type for schema: '%s'", fieldName, schema)));
            fieldDescriptionBuilder.add(new KafkaTopicFieldDescription(
                    fieldName,
                    type,
                    fieldName,
                    null,
                    getDataFormat(type),
                    null,
                    false));
        }
        return fieldDescriptionBuilder.build();
    }

    private static String getDataFormat(Type type)
    {
        // use type instead of schema, because schema may be combined schema which is hard to determine data format
        if (type == DATE || type == TIME_TZ_MILLIS || type == TIMESTAMP_TZ_MILLIS) {
            // https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.7.3.1
            return "iso8601";
        }
        return null;
    }

    private static class SchemaConverter
    {
        private final EmptyFieldStrategy emptyFieldStrategy;

        public SchemaConverter(EmptyFieldStrategy emptyFieldStrategy)
        {
            this.emptyFieldStrategy = requireNonNull(emptyFieldStrategy, "emptyFieldStrategy is null");
        }

        private Optional<Type> convert(Schema schema)
        {
            if (schema == null) {
                return Optional.empty();
            }

            return switch (schema) {
                case BooleanSchema booleanSchema -> convertBooleanSchema(booleanSchema);
                case EnumSchema enumSchema -> convertEnumSchema(enumSchema);
                case ConstSchema constSchema -> convertConstSchema(constSchema);
                case NumberSchema numberSchema -> convertNumberSchema(numberSchema);
                case StringSchema stringSchema -> convertStringSchema(stringSchema);
                case ObjectSchema objectSchema -> convertObjectSchema(objectSchema);
                case ArraySchema arraySchema -> convertArraySchema(arraySchema);
                case CombinedSchema combinedSchema -> convertCombinedSchema(combinedSchema);
                case ReferenceSchema referenceSchema -> convertReferenceSchema(referenceSchema);
                case ConditionalSchema conditionalSchema -> convertConditionalSchema(conditionalSchema);
                case FalseSchema falseSchema -> convertFalseSchema(falseSchema);
                case TrueSchema trueSchema -> convertTrueSchema(trueSchema);
                case NotSchema notSchema -> convertNotSchema(notSchema);
                case NullSchema nullSchema -> convertNullSchema(nullSchema);
                case EmptySchema emptySchema -> convertEmptySchema(emptySchema);
                default -> throw new UnsupportedOperationException(format("Unsupported schema type: '%s'", schema.getClass().getSimpleName()));
            };
        }

        @SuppressWarnings("unused")
        private Optional<Type> convertBooleanSchema(BooleanSchema schema)
        {
            return Optional.of(BOOLEAN);
        }

        @SuppressWarnings("unused")
        private Optional<Type> convertNullSchema(NullSchema schema)
        {
            return Optional.empty();
        }

        private Optional<Type> convertConstSchema(ConstSchema schema)
        {
            Object permittedValue = schema.getPermittedValue();
            return getTypeFromValue(permittedValue);
        }

        private Optional<Type> convertEnumSchema(EnumSchema schema)
        {
            ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
            for (Object value : schema.getPossibleValues()) {
                getTypeFromValue(value).ifPresent(typeBuilder::add);
            }

            List<Type> types = typeBuilder.build();
            if (!types.isEmpty()) {
                return resolveTypes(types);
            }

            return emptySchemaType(schema.getClass().getSimpleName());
        }

        private Optional<Type> convertNumberSchema(NumberSchema schema)
        {
            if (schema.requiresInteger()) {
                return Optional.of(BIGINT);
            }

            // fallback to double if no other type is specified
            return Optional.of(DOUBLE);
        }

        private Optional<Type> convertReferenceSchema(ReferenceSchema schema)
        {
            return convert(schema.getReferredSchema());
        }

        @SuppressWarnings("unused")
        private Optional<Type> convertStringSchema(StringSchema schema)
        {
            String format = schema.getFormatValidator().formatName();

            Type type = switch (format) {
                case "date" -> DATE;
                case "time" -> TIME_TZ_MILLIS;
                case "date-time" -> TIMESTAMP_TZ_MILLIS;
                default -> VARCHAR;
            };
            return Optional.of(type);
        }

        private Optional<Type> convertObjectSchema(ObjectSchema schema)
        {
            Map<String, Schema> propertySchemas = schema.getPropertySchemas();
            ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
            for (Map.Entry<String, Schema> entry : propertySchemas.entrySet()) {
                String fieldName = entry.getKey();
                Schema fieldSchema = entry.getValue();
                convert(fieldSchema).map(field -> RowType.field(fieldName, field)).ifPresent(fieldBuilder::add);
            }
            List<RowType.Field> fields = fieldBuilder.build();
            if (fields.isEmpty()) {
                return emptySchemaType(schema.getClass().getSimpleName()).or(() -> Optional.of(RowType.from(ImmutableList.of())));
            }
            return Optional.of(RowType.from(fields));
        }

        private Optional<Type> convertArraySchema(ArraySchema schema)
        {
            Optional<Type> type = convert(schema.getAllItemSchema());
            if (type.isPresent()) {
                return Optional.of(new ArrayType(type.get()));
            }

            ImmutableList.Builder<Type> itemTypesBuilder = ImmutableList.builder();
            for (Schema itemSchema : schema.getItemSchemas()) {
                Optional<Type> itemType = convert(itemSchema);
                itemType.ifPresent(itemTypesBuilder::add);
            }
            List<Type> itemTypes = itemTypesBuilder.build();
            if (!itemTypes.isEmpty()) {
                return resolveTypes(itemTypes).map(ArrayType::new);
            }
            throw new IllegalStateException(format("Array schema '%s' has no valid item types", schema));
        }

        private Optional<Type> convertCombinedSchema(CombinedSchema schema)
        {
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            for (Schema subSchema : schema.getSubschemas()) {
                convert(subSchema).ifPresent(typesBuilder::add);
            }
            List<Type> types = typesBuilder.build();
            if (types.isEmpty()) {
                return emptySchemaType(schema.getClass().getSimpleName());
            }
            if (types.size() == 1) {
                return Optional.of(types.getFirst());
            }

            Optional<Type> type = resolveTypes(types);
            if (type.isPresent()) {
                return type;
            }

            throw new UnsupportedOperationException("Incompatible combined schema: " + schema);
        }

        private Optional<Type> convertFalseSchema(FalseSchema schema)
        {
            // TODO: handle false schema https://starburstdata.atlassian.net/browse/SEP-18209
            throw new UnsupportedOperationException("Schema %s not supported".formatted(schema.getClass().getSimpleName()));
        }

        private Optional<Type> convertTrueSchema(TrueSchema schema)
        {
            // TODO: handle true schema https://starburstdata.atlassian.net/browse/SEP-18209
            throw new UnsupportedOperationException("Schema %s not supported".formatted(schema.getClass().getSimpleName()));
        }

        private Optional<Type> convertNotSchema(NotSchema schema)
        {
            // TODO: handle not schema https://starburstdata.atlassian.net/browse/SEP-18209
            // See https://json-schema.org/understanding-json-schema/reference/combining#not
            throw new UnsupportedOperationException("Schema %s not supported".formatted(schema.getClass().getSimpleName()));
        }

        private Optional<Type> convertConditionalSchema(ConditionalSchema schema)
        {
            // TODO: handle conditional schema https://starburstdata.atlassian.net/browse/SEP-18209
            // See https://json-schema.org/understanding-json-schema/reference/conditionals
            throw new UnsupportedOperationException("Schema %s not supported".formatted(schema.getClass().getSimpleName()));
        }

        @SuppressWarnings("unused")
        private Optional<Type> convertEmptySchema(EmptySchema schema)
        {
            return Optional.empty();
        }

        private Optional<Type> emptySchemaType(String typeName)
        {
            return switch (emptyFieldStrategy) {
                case FAIL -> throw new IllegalStateException(format("%s type has no valid field", typeName));
                case IGNORE -> Optional.empty();
                case MARK -> Optional.of(DUMMY_ROW_TYPE);
            };
        }

        private static Optional<Type> getTypeFromValue(Object value)
        {
            if (value instanceof Boolean) {
                return Optional.of(BOOLEAN);
            }
            if (value instanceof Integer || value instanceof Long) {
                return Optional.of(BIGINT);
            }
            if (value instanceof Number) {
                return Optional.of(DOUBLE);
            }
            if (value instanceof String) {
                return Optional.of(VARCHAR);
            }
            throw new UnsupportedOperationException("Unsupported constant type: '%s'".formatted(value.getClass().getSimpleName()));
        }

        /**
         * Resolve multiple types to a single type if all the types are the same
         * Otherwise, throw exception, e.g. integer + boolean
         * If the input list is empty, return Optional.empty()
         */
        private static Optional<Type> resolveTypes(List<Type> types)
        {
            if (types.isEmpty()) {
                return Optional.empty();
            }

            Set<Type> uniqueTypes = Set.copyOf(types);

            if (uniqueTypes.size() == 1) {
                return Optional.of(getOnlyElement(uniqueTypes));
            }

            throw new UnsupportedOperationException("Incompatible types: " + types);
        }
    }
}
