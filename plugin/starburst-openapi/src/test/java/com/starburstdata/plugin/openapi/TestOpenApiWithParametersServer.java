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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiWithParametersServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ParametersServer parametersServer = closeAfterClass(new ParametersServer());
        parametersServer.start();
        String specificationLocation = requireNonNull(
                OpenApiQueryRunner.class.getClassLoader().getResource("java_server/parameters.3.0.4.json"),
                "Expected java_server specification was present")
                .getFile();
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", specificationLocation)
                        .put("openapi.base-uri", parametersServer.getBaseUri().toString())
                        .buildOrThrow())
                .build();
    }

    @ParameterizedTest
    @MethodSource("queryParameterSerializations")
    public void testQueryParameterSerialization(String argumentName, String inputSQL, String outputString)
    {
        assertThat(query("SELECT value FROM TABLE(openapi.default.repeat_query_params(%s => %s))".formatted(
                argumentName,
                inputSQL)))
                .matches("VALUES CAST('%s' AS VARCHAR)".formatted(outputString));
    }

    public static Stream<Arguments> queryParameterSerializations()
    {
        return ImmutableList.<Arguments>builder()
                .add(Arguments.of("string_none", "'Goodbye World!'", "Goodbye World!"))
                .add(Arguments.of("string_byte", "to_utf8('Encode me!')", "RW5jb2RlIG1lIQ=="))
                .add(Arguments.of("string_uuid", "UUID '904046ea-90cd-4ffb-8a71-5b2ba0c8b74c'", "904046ea-90cd-4ffb-8a71-5b2ba0c8b74c"))
                .add(Arguments.of("string_date", "DATE '0000-01-01'", "0000-01-01"))
                .add(Arguments.of("string_date", "DATE '1970-01-01'", "1970-01-01"))
                .add(Arguments.of("string_date", "DATE '1969-12-31'", "1969-12-31"))
                .add(Arguments.of("string_date", "DATE '2024-01-15'", "2024-01-15"))
                .add(Arguments.of("string_date", "DATE '9999-12-31'", "9999-12-31"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '0000-01-01 00:00:00 UTC'", "0000-01-01T00:00:00Z"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '1970-01-01 00:00:00.000000000000 UTC'", "1970-01-01T00:00:00Z"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '2024-01-15 13:14:15.000000000000 UTC'", "2024-01-15T13:14:15Z"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '2024-01-15 13:14:15.123456789000 UTC'", "2024-01-15T13:14:15.123456789Z"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '2024-01-15 13:14:15.123456789012 UTC'", "2024-01-15T13:14:15.123456789012Z"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '2024-01-15 13:14:15.000000000000 +05:30'", "2024-01-15T13:14:15+05:30"))
                .add(Arguments.of("string_date_time", "TIMESTAMP '9999-12-31 23:59:59.999999999999 UTC'", "9999-12-31T23:59:59.999999999999Z"))
                .add(Arguments.of("number_none", "NUMBER '3.1415926535897932384626433832795028841971693993751'", "3.1415926535897932384626433832795028841971693993751"))
                .add(Arguments.of("integer_none", "NUMBER '9223372036854775807000000'", "9223372036854775807000000"))
                .add(Arguments.of("integer_int32", "2147483647", "2147483647"))
                .add(Arguments.of("integer_int64", "-9223372036854775808", "-9223372036854775808"))
                .add(Arguments.of("number_float", "REAL '1.1'", "1.100000023841858"))
                .add(Arguments.of("number_double", "1.0E-10", "0.00000000010"))
                .add(Arguments.of("boolean", "TRUE", "true"))
                .build()
                .stream();
    }

    @Test
    public void testUnexplodedQueryParameterSerialization()
    {
        assertThat(query(
                """
                SELECT value
                FROM TABLE(openapi.default.repeat_query_params(list_string_explode => ARRAY['H,e,l,l,o', 'T,h,e,r,e!']))"""))
                .matches("VALUES CAST('H,e,l,l,o' AS VARCHAR), CAST('T,h,e,r,e!' AS VARCHAR)");
    }

    @Test
    public void testExplodedQueryParameterSerialization()
    {
        assertThat(query(
                """
                SELECT value
                FROM TABLE(openapi.default.repeat_query_params(list_string_explode => ARRAY['Hello', 'There!']))"""))
                .matches("VALUES CAST('Hello' AS VARCHAR), CAST('There!' AS VARCHAR)");
    }

    @Test
    public void testMultiPathParameterSerialization()
    {
        assertThat(query(
                """
                SELECT paramOne, paramTwo FROM TABLE(openapi.default.repeat_path_param_one_param_two(
                        param_one => 'TEST',
                        param_two => 'ING'))"""))
                .matches("VALUES (CAST('TEST' AS VARCHAR), CAST('ING' AS VARCHAR))");
    }

    @Test
    public void testDateValidation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.repeat_query_params(string_date => DATE '-0001-12-31'))",
                "\\QYear -1 must be greater than or equal to 0 and less than or equal to 9999\\E");
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.repeat_query_params(string_date => DATE '10000-12-31'))",
                "\\QYear 10000 must be greater than or equal to 0 and less than or equal to 9999\\E");
    }

    @Test
    public void testDateTimeValidation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.repeat_query_params(string_date_time => TIMESTAMP '-0001-12-31 23:59:59.999999999 UTC'))",
                "\\QYear -1 must be greater than or equal to 0 and less than or equal to 9999\\E");
        assertQueryFails(
                "SELECT * FROM TABLE(openapi.default.repeat_query_params(string_date_time => TIMESTAMP '10000-01-01 00:00:00 UTC'))",
                "\\QYear 10000 must be greater than or equal to 0 and less than or equal to 9999\\E");
    }
}
