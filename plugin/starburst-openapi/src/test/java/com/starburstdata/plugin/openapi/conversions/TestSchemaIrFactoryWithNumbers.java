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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.plugin.openapi.OpenApiDescription;
import com.starburstdata.plugin.openapi.SpecException;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.NumberIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.JSON;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.DOUBLE;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.FLOAT;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.INT32;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.INT64;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.NONE_INTEGER;
import static com.starburstdata.plugin.openapi.conversions.ir.NumberIr.Format.NONE_NUMBER;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

/**
 * Tests {@link SchemaIrFactory} with schemas from the numbers.json description.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithNumbers
{
    private static final SchemaIrFactory DROP_FACTORY = new SchemaIrFactory(DROP, emptyMap());
    private static final SchemaIrFactory ERROR_FACTORY = new SchemaIrFactory(ERROR, emptyMap());
    private static final SchemaIrFactory JSON_FACTORY = new SchemaIrFactory(JSON, emptyMap());

    private static Map<String, Schema> schemas;

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiDescription.parse("test_schema_ir_factory/numbers.json");
        schemas = openAPI.getComponents().getSchemas();
    }

    @Test
    public void testSupportedNumberFormats()
            throws SpecException
    {
        assertSupportedNumberFormat(schemas.get("numberNoFormat"), NONE_NUMBER);
        assertSupportedNumberFormat(schemas.get("integerNoFormat"), NONE_INTEGER);
        assertSupportedNumberFormat(schemas.get("numberInt32"), INT32);
        assertSupportedNumberFormat(schemas.get("numberInt64"), INT64);
        assertSupportedNumberFormat(schemas.get("integerInt32"), INT32);
        assertSupportedNumberFormat(schemas.get("integerInt64"), INT64);
        assertSupportedNumberFormat(schemas.get("numberFloat"), FLOAT);
        assertSupportedNumberFormat(schemas.get("numberDouble"), DOUBLE);
    }

    private static void assertSupportedNumberFormat(
            Schema<?> schema,
            NumberIr.Format format)
            throws SpecException
    {
        assertThat(ERROR_FACTORY.convert(schema)).isEqualTo(new NumberIr(format));
    }

    @Test
    public void testUnsupportedNumberFormats()
            throws SpecException
    {
        assertOnUnsupportedNumberFormat(schemas.get("integerFloat"));
        assertOnUnsupportedNumberFormat(schemas.get("integerDouble"));
        assertOnUnsupportedNumberFormat(schemas.get("numberUnknownFormat"));
        assertOnUnsupportedNumberFormat(schemas.get("integerUnknownFormat"));
    }

    private static void assertOnUnsupportedNumberFormat(Schema<?> schema)
            throws SpecException
    {
        ImmutableMap.<String, SchemaIrFactory>builder()
                .put("ERROR_FACTORY", ERROR_FACTORY)
                .put("DROP_FACTORY", DROP_FACTORY)
                .buildOrThrow()
                .forEach((factoryName, factory) ->
                        assertThatThrownBy(() -> factory.convert(schema))
                                .as("Expect %s to throw SpecException with message", factoryName)
                                .asInstanceOf(throwable(SpecException.class))
                                .hasMessageContaining("Unsupported number/integer format")
                                .extracting(SpecException::path)
                                .asInstanceOf(list(String.class))
                                .as("Expect %s's SpecException to link to format property")
                                .contains("format"));

        assertThat(JSON_FACTORY.convert(schema))
                .as("JSON_POLICY shouldn't throw SpecException but return JsonIr")
                .isEqualTo(new JsonIr());
    }
}
