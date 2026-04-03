/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TestOpenApiWithTypesServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TypesServer typesServer = closeAfterClass(new TypesServer());
        typesServer.start();
        String specificationLocation = requireNonNull(
                OpenApiQueryRunner.class.getClassLoader().getResource("java_server/types.3.0.4.json"),
                "Expected java_server specification was present")
                .getFile();
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", specificationLocation)
                        .put("openapi.base-uri", typesServer.getBaseUri().toString())
                        .buildOrThrow())
                .build();
    }

    @ParameterizedTest
    @MethodSource("nullFunctions")
    void testNullValues(String nullFunction)
    {
        assertThat(computeActual(
                "SELECT * FROM TABLE(openapi.default.%s())".formatted(nullFunction))
                .getOnlyValue())
                .isEqualTo(null);
    }

    public static List<String> nullFunctions()
    {
        return ImmutableList.<String>builder()
                .add("boolean_null")
                .add("string_none_null")
                .add("string_byte_null")
                .add("string_uuid_null")
                .add("number_none_null")
                .add("integer_none_null")
                .add("integer_int32_null")
                .add("integer_int64_null")
                .add("number_float_null")
                .add("number_double_null")
                .add("object_map_null")
                .build();
    }

    @Test
    void testArrayNull()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.array_null())",
                "Expected JSON ARRAY but was NULL");
    }

    @Test
    void testObjectRowNull()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.object_row_null())",
                "Expected JSON OBJECT but was NULL");
    }

    @Test
    void testJsonNull()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.json_null())"))
                .matches("VALUES JSON_PARSE('null')");
    }

    @ParameterizedTest
    @MethodSource("objectErrorFunctions")
    void testObjectError(String errorFunction)
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.%s())".formatted(errorFunction),
                "Expected JSON \\w+ but was OBJECT");
    }

    public static List<String> objectErrorFunctions()
    {
        return ImmutableList.<String>builder()
                .add("boolean_unexpectedobject")
                .add("string_none_unexpectedobject")
                .add("string_byte_unexpectedobject")
                .add("string_uuid_unexpectedobject")
                .add("number_none_unexpectedobject")
                .add("integer_none_unexpectedobject")
                .add("integer_int32_unexpectedobject")
                .add("integer_int64_unexpectedobject")
                .add("number_float_unexpectedobject")
                .add("number_double_unexpectedobject")
                .add("array_unexpectedobject")
                .build();
    }

    @Test
    public void testRowUnexpectedArray()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.object_row_unexpectedarray())",
                "Expected JSON OBJECT but was ARRAY");
    }

    @Test
    public void testMapUnexpectedArray()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.object_map_unexpectedarray())",
                "Expected JSON OBJECT but was ARRAY");
    }

    @ParameterizedTest
    @MethodSource("primitiveValidValues")
    void testValidValues(String validFunction, String validValue)
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.%s())".formatted(validFunction)))
                .matches("VALUES " + validValue);
    }

    public static Stream<Arguments> primitiveValidValues()
    {
        return ImmutableMap.<String, String>builder()
                .put("boolean_valid", "TRUE")
                .put("string_none_valid", "CAST('Hello World!' AS VARCHAR)")
                .put("string_byte_valid", "to_utf8('Hello World!')")
                .put("string_uuid_valid", "UUID '687978a3-2c79-4a1f-81b9-e64dfe355737'")
                .put("number_none_valid", "JSON_PARSE('4.9E-325')")
                .put("integer_none_valid", "JSON_PARSE('9223372036854775808.0')")
                .buildOrThrow()
                .entrySet()
                .stream()
                .map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    @Test
    public void testValidJson()
    {
        assertQuerySucceeds("SELECT * FROM TABLE(openapi.default.\"json_value\"())");
    }

    @Test
    void testInvalidByte()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.string_byte_invalid())",
                "JSON STRING did not contain valid base64 encoded data");
    }

    @Test
    void testInvalidUuid()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.string_uuid_invalid())",
                "String was not a valid UUID");
    }

    @Test
    void testInvalidInteger()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.integer_none_invalid())",
                "Expected JSON NUMBER to be integral number");
    }

    @ParameterizedTest
    @MethodSource("inBoundNumbers")
    void testInBoundNumbers(String validFunction, Object value)
    {
        assertThat(computeActual("SELECT * FROM TABLE(openapi.default.%s())".formatted(validFunction)).getOnlyValue())
                .isEqualTo(value);
    }

    public static Stream<Arguments> inBoundNumbers()
    {
        return ImmutableMap.<String, Object>builder()
                .put("integer_int32_style_min", Integer.MIN_VALUE)
                .put("integer_int32_style_max", Integer.MAX_VALUE)
                .put("integer_int64_style_min", Long.MIN_VALUE)
                .put("integer_int64_style_max", Long.MAX_VALUE)
                .put("number_float_style_min", -Float.MAX_VALUE)
                .put("number_float_style_max", Float.MAX_VALUE)
                .put("number_float_style_precise", Float.MIN_VALUE)
                .put("number_double_style_min", -Double.MAX_VALUE)
                .put("number_double_style_max", Double.MAX_VALUE)
                .put("number_double_style_precise", Double.MIN_VALUE)
                .buildOrThrow()
                .entrySet()
                .stream()
                .map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    @ParameterizedTest
    @MethodSource("outOfBoundNumbers")
    void testOutOfBoundNumbers(String function, String error)
    {
        assertQueryFails("SELECT * FROM TABLE(openapi.default.%s())".formatted(function), error);
    }

    public static Stream<Arguments> outOfBoundNumbers()
    {
        return ImmutableMap.<String, Object>builder()
                .put("integer_int32_style_toonegative", "Failed to fit JSON NUMBER in integer type")
                .put("integer_int32_style_toopositive", "Failed to fit JSON NUMBER in integer type")
                .put("integer_int64_style_toonegative", "Failed to fit JSON NUMBER in bigint type")
                .put("integer_int64_style_toopositive", "Failed to fit JSON NUMBER in bigint type")
                .put("number_float_style_toonegative", "Would lose floating point precision fitting decimal value in real type")
                .put("number_float_style_toopositive", "Would lose floating point precision fitting decimal value in real type")
                .put("number_float_style_tooprecise", "Would lose floating point precision fitting decimal value in real type")
                .put("number_double_style_toonegative", "Would lose floating point precision fitting decimal value in double type")
                .put("number_double_style_toopositive", "Would lose floating point precision fitting decimal value in double type")
                // TODO under double precision deserializes as zero, may need ObjectMapper adjustments.
                // .put("number_double_tooprecise", "Would lose floating point precision fitting decimal value in double type")
                .buildOrThrow()
                .entrySet()
                .stream()
                .map(entry -> Arguments.of(entry.getKey(), entry.getValue()));
    }

    @Test
    void testInvalidArray()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.array_invalid())",
                "Expected JSON BOOLEAN but was NUMBER");
    }

    @Test
    void testValidArray()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.array_valid())"))
                .matches("VALUES TRUE, FALSE, NULL");
    }

    @Test
    void testInvalidMap()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.object_map_invalid())",
                "Expected JSON BOOLEAN but was NUMBER");
    }

    @Test
    void testValidMap()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.object_map_valid())"))
                .matches("""
                    VALUES MAP(CAST(ARRAY['valid', 'also_valid', 'also_also_valid'] AS ARRAY<VARCHAR>), ARRAY[TRUE, FALSE, NULL])""");
    }

    @Test
    void testInvalidRow()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.object_row_invalid())",
                "Expected JSON BOOLEAN but was NUMBER");
    }

    @Test
    void testValidRow()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.object_row_valid())"))
                .result()
                .hasColumnNames(ImmutableList.<String>builder()
                        .add("missing")
                        .add("null")
                        .add("present")
                        .build())
                .hasTypes(ImmutableList.<Type>builder()
                        .add(BOOLEAN)
                        .add(BOOLEAN)
                        .add(BOOLEAN)
                        .build());

        assertThat(query("SELECT missing, \"null\", present FROM TABLE(openapi.default.object_row_valid())"))
                .matches("VALUES (CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN), TRUE)");
    }
}
