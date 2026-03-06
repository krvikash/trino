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
import com.starburstdata.plugin.openapi.OpenApiSpec;
import com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.SchemaException;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.JSON;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.BYTE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.NONE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.UUID;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

/**
 * Tests {@link SchemaIrFactory} with schemas from the strings.json specification.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithStrings
{
    private static Map<String, Schema> schemas;
    private static final SchemaIrFactory DROP_FACTORY = new SchemaIrFactory(DROP, emptyMap());
    private static final SchemaIrFactory ERROR_FACTORY = new SchemaIrFactory(ERROR, emptyMap());
    private static final SchemaIrFactory JSON_FACTORY = new SchemaIrFactory(JSON, emptyMap());

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiSpec.parse("test_schema_ir_factory/strings.json");
        schemas = openAPI.getComponents().getSchemas();
    }

    @Test
    public void testSupportedStringFormats()
            throws SchemaException
    {
        assertSupportedStringFormat(schemas.get("stringNoFormat"), NONE);
        assertSupportedStringFormat(schemas.get("stringByteFormat"), BYTE);
        assertSupportedStringFormat(schemas.get("stringUuidFormat"), UUID);
    }

    private static void assertSupportedStringFormat(
            Schema<?> schema,
            StringIr.Format format)
            throws SchemaException
    {
        assertThat(ERROR_FACTORY.convert(schema)).isEqualTo(new StringIr(format));
    }

    @Test
    public void testUnsupportedStringFormat()
            throws SchemaException
    {
        ImmutableMap.<String, SchemaIrFactory>builder()
                .put("ERROR_FACTORY", ERROR_FACTORY)
                .put("DROP_FACTORY", DROP_FACTORY)
                .buildOrThrow()
                .forEach((factoryName, factory) ->
                        assertThatThrownBy(() -> factory.convert(schemas.get("stringUnknownFormat")))
                                .as("Expect %s to throw SchemaException with message", factoryName)
                                .asInstanceOf(throwable(SchemaException.class))
                                .hasMessageContaining("Unsupported string format")
                                .extracting(SchemaException::getPath)
                                .asInstanceOf(list(String.class))
                                .as("Expect %s's SchemaException to link to format property")
                                .contains("format"));

        assertThat(JSON_FACTORY.convert(schemas.get("stringUnknownFormat")))
                .as("JSON_POLICY shouldn't throw SchemaException but return JsonIr")
                .isEqualTo(new JsonIr());
    }
}
