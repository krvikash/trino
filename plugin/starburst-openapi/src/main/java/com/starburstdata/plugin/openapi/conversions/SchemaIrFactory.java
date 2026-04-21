/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.plugin.openapi.SpecException;
import com.starburstdata.plugin.openapi.conversions.ir.ArrayIr;
import com.starburstdata.plugin.openapi.conversions.ir.BooleanIr;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.NumberIr;
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.swagger.v3.oas.models.media.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.plugin.openapi.SpecUtil.castSchemaMap;
import static com.starburstdata.plugin.openapi.conversions.ReferenceUtil.extractRefKey;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.JSON;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.DOUBLE;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.FLOAT;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.INT32;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.INT64;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.NONE_INTEGER;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.NONE_NUMBER;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.BYTE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.DATE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.DATE_TIME;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.NONE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.UUID;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SchemaIrFactory
{
    private final CastPolicy castPolicy;
    private final Map<String, Schema<?>> referenceableSchemas;

    public SchemaIrFactory(
            CastPolicy castPolicy,
            Map<String, Schema<?>> referenceableSchemas)
    {
        this.castPolicy = requireNonNull(castPolicy, "castPolicy is null");
        this.referenceableSchemas = requireNonNull(referenceableSchemas, "referenceableSchemas is null");
    }

    public enum CastPolicy
    {
        DROP,
        ERROR,
        JSON,
    }

    public SchemaIr convert(Schema<?> schema)
            throws SpecException
    {
        return convert(schema, ImmutableSet.of());
    }

    private SchemaIr convert(
            Schema<?> schema,
            Set<String> visitedReferences)
            throws SpecException
    {
        // https://spec.openapis.org/oas/v3.0.4.html#schema-object
        // Schema is a union of keywords to use to _validate_ JSON objects.
        // Using select keywords we will derive the "meaning" of JSON objects.
        // Ignoring keywords ...
        // title, multipleOf, maximum, exclusiveMaximum, minimum, exclusiveMinimum, maxLength
        // minLength, pattern, maxItems, minItems, uniqueItems, maxProperties, minProperties, required
        // description, default, nullable, discriminator, xml, externalDocs,
        // example, deprecated, x-[EXTENSIONS]

        List<String> unsupportedBooleanKeywords = Stream.<Optional<String>>of(
                        schema.getAllOf() != null ? Optional.of("allOf") : Optional.empty(),
                        schema.getOneOf() != null ? Optional.of("oneOf") : Optional.empty(),
                        schema.getAnyOf() != null ? Optional.of("anyOf") : Optional.empty(),
                        schema.getNot() != null ? Optional.of("not") : Optional.empty())
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        if (!unsupportedBooleanKeywords.isEmpty() && !castPolicy.equals(JSON)) {
            throw new SpecException(format(
                    "Schema uses unsupported boolean keywords [%s]",
                    join(",", unsupportedBooleanKeywords)));
        }
        if (!unsupportedBooleanKeywords.isEmpty()) {
            return new JsonIr();
        }

        String ref = schema.get$ref();
        if (ref != null && visitedReferences.contains(ref)) {
            throw new SpecException("Cyclic reference detected").fromMember("$ref");
        }

        if (ref != null) {
            // https://spec.openapis.org/oas/v3.0.4.html#reference-object
            final String key;
            try {
                key = extractRefKey(ImmutableList.of("components", "schemas"), ref);
            }
            catch (IllegalArgumentException e) {
                throw new SpecException(e.getMessage()).withCause(e).fromMember("$ref");
            }
            if (!referenceableSchemas.containsKey(key)) {
                throw new SpecException("References non-existent schema %s".formatted(key))
                        .withCause(new NoSuchElementException())
                        .fromMember("$ref");
            }
            try {
                return convert(
                        referenceableSchemas.get(key),
                        ImmutableSet.<String>builderWithExpectedSize(visitedReferences.size() + 1)
                                .addAll(visitedReferences)
                                .add(ref)
                                .build());
            }
            catch (SpecException e) {
                throw e.fromPath(ImmutableList.of("$ref", key));
            }
        }

        String type = schema.getType();
        if (type != null) {
            return switch (type) {
                case "boolean" -> new BooleanIr();
                case "object" -> convertObject(schema, visitedReferences);
                case "array" -> convertArray(schema, visitedReferences);
                case "number" -> convertNumber(schema, false);
                case "integer" -> convertNumber(schema, true);
                case "string" -> convertString(schema);
                default -> throw new UnsupportedOperationException("Unsupported OAS type %s".formatted(type));
            };
        }

        if (schema.getEnum() != null && !castPolicy.equals(JSON)) {
            throw new SpecException("Enum keyword without type keyword is unsupported")
                    .fromMember("enum");
        }
        if (schema.getEnum() != null) {
            return new JsonIr();
        }

        // Lack of keywords means any JSON value allowed.
        return new JsonIr();
    }

    private ObjectIr convertObject(
            Schema<?> schema,
            Set<String> visitedReferences)
            throws SpecException
    {
        if (schema.getFormat() != null) {
            throw new SpecException("Schema uses unsupported combination of object type and format keyword")
                    .fromMember("format");
        }

        // Either Boolean or Schema<?> see ...
        // https://github.com/swagger-api/swagger-parser/blob/df3b7796f9301ae3b0729080db8153055b72a0db/modules/swagger-parser-v3/src/main/java/io/swagger/v3/parser/util/OpenAPIDeserializer.java#L2776
        Object additionalProperties = schema.getAdditionalProperties();
        Map<String, Schema<?>> properties = castSchemaMap(schema.getProperties());

        Optional<SchemaIr> additionalPropertiesIr = Optional.empty();
        // additionalProperties if unset is supposed to be interpreted as true,
        // but in real world scenarios many schema authors skip the keyword,
        // to provide the most useful conversion we will ignore these potential extra columns.
        if (TRUE.equals(additionalProperties) || (additionalProperties == null && properties == null)) {
            additionalPropertiesIr = Optional.of(new JsonIr());
        }
        else if (additionalProperties instanceof Schema<?> valueSchema) {
            try {
                SchemaIr valueIr = convert(
                        valueSchema,
                        visitedReferences);
                additionalPropertiesIr = Optional.of(valueIr);
            }
            catch (SpecException e) {
                additionalPropertiesIr = switch (castPolicy) {
                    case ERROR -> throw e.fromMember("additionalProperties");
                    case DROP -> Optional.empty();
                    case JSON -> Optional.of(new JsonIr());
                };
            }
        }

        final Map<String, SchemaIr> propertiesIr;
        if (properties != null) {
            validatePropertyKeysUnique(properties);
            propertiesIr = convertPropertiesToIr(
                    properties,
                    visitedReferences);
        }
        else {
            propertiesIr = emptyMap();
        }

        return new ObjectIr(propertiesIr, additionalPropertiesIr);
    }

    private static void validatePropertyKeysUnique(Map<String, Schema<?>> properties)
            throws SpecException
    {
        Set<String> mutableLowercaseKeys = new HashSet<>();
        List<String> conflictingProperties = properties.keySet()
                .stream()
                .filter(key -> !mutableLowercaseKeys.add(key.toLowerCase(ENGLISH)))
                .collect(toImmutableList());
        if (!conflictingProperties.isEmpty()) {
            throw new SpecException(format(
                    "Uses keys that cannot be referenced unambiguously with case-insensitivity: %s",
                    join(", ", conflictingProperties)))
                    .fromMember("properties");
        }
    }

    private Map<String, SchemaIr> convertPropertiesToIr(
            Map<String, Schema<?>> properties,
            Set<String> visitedReferences)
            throws SpecException
    {
        ImmutableMap.Builder<String, SchemaIr> keyToIrBuilder = ImmutableMap.builder();
        for (Entry<String, Schema<?>> entry : properties.entrySet()) {
            String key = entry.getKey();
            Schema<?> valueSchema = entry.getValue();
            try {
                SchemaIr valueIr = convert(
                        valueSchema,
                        visitedReferences);
                keyToIrBuilder.put(key, valueIr);
            }
            catch (SpecException e) {
                switch (castPolicy) {
                    case DROP -> {}
                    case ERROR -> throw e.fromMember("\"%s\"".formatted(key)).fromMember("properties");
                    case JSON -> keyToIrBuilder.put(key, new JsonIr());
                }
            }
        }
        return keyToIrBuilder.buildOrThrow();
    }

    private ArrayIr convertArray(
            Schema<?> schema,
            Set<String> visitedReferences)
            throws SpecException
    {
        if (schema.getFormat() != null) {
            throw new SpecException(
                    "Schema uses unsupported combination of array type and format keyword")
                    .fromMember("format");
        }

        Schema<?> itemSchema = schema.getItems();
        if (itemSchema == null) {
            throw new SpecException("Items keyword must be present in array type schema")
                    .fromMember("items");
        }

        final SchemaIr itemIr;
        try {
            itemIr = convert(
                    itemSchema,
                    visitedReferences);
        }
        catch (SpecException e) {
            return switch (castPolicy) {
                case DROP, ERROR -> throw e.fromMember("items");
                case JSON -> new ArrayIr(new JsonIr());
            };
        }
        return new ArrayIr(itemIr);
    }

    private SchemaIr convertNumber(
            Schema<?> schema,
            boolean isInteger)
            throws SpecException
    {
        // https://spec.openapis.org/oas/v3.0.4.html#data-type-format
        String format = schema.getFormat();
        if (format == null && isInteger) {
            return new NumberIr(NONE_INTEGER);
        }
        if (format == null) {
            return new NumberIr(NONE_NUMBER);
        }
        final Optional<NumberIr.Format> numberFormat;
        if (isInteger) {
            numberFormat = convertIntegerFormat(format);
        }
        else {
            numberFormat = convertIntegerFormat(format).or(() -> convertNonIntegerFormat(format));
        }
        if (numberFormat.isEmpty()) {
            return switch (castPolicy) {
                case DROP, ERROR -> throw new SpecException(
                        "Unsupported number/integer format: %s".formatted(format))
                        .fromMember("format");
                case JSON -> new JsonIr();
            };
        }
        return new NumberIr(numberFormat.get());
    }

    private Optional<NumberIr.Format> convertNonIntegerFormat(String format)
    {
        return Optional.ofNullable(switch (format) {
            case "float" -> FLOAT;
            case "double" -> DOUBLE;
            default -> null;
        });
    }

    private Optional<NumberIr.Format> convertIntegerFormat(String format)
    {
        requireNonNull(format, "format is null");
        return Optional.ofNullable(switch (format) {
            case "int32" -> INT32;
            case "int64" -> INT64;
            default -> null;
        });
    }

    private SchemaIr convertString(
            Schema<?> schema)
            throws SpecException
    {
        // https://spec.openapis.org/oas/v3.0.4.html#data-type-format
        return switch (schema.getFormat()) {
            case null -> new StringIr(NONE);
            case "byte" -> new StringIr(BYTE);
            case "uuid" -> new StringIr(UUID);
            case "date" -> new StringIr(DATE);
            case "date-time" -> new StringIr(DATE_TIME);
            case String other -> switch (castPolicy) {
                case DROP, ERROR -> throw new SpecException(
                        "Unsupported string format %s".formatted(other))
                        .fromMember("format");
                case JSON -> new JsonIr();
            };
        };
    }
}
