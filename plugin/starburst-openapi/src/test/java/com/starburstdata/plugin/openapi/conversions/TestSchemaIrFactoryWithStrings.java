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
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.FALLBACK;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.BYTE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.DATE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.DATE_TIME;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.NONE;
import static com.starburstdata.plugin.openapi.conversions.ir.StringIr.Format.UUID;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

/**
 * Tests {@link SchemaIrFactory} with schemas from the strings.json description.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithStrings
{
    private static Map<String, Schema> schemas;
    private static final SchemaIrFactory DROP_FACTORY = new SchemaIrFactory(DROP, emptyMap());
    private static final SchemaIrFactory ERROR_FACTORY = new SchemaIrFactory(ERROR, emptyMap());
    private static final SchemaIrFactory JSON_FACTORY = new SchemaIrFactory(FALLBACK, emptyMap());

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiDescription.parse("test_schema_ir_factory/strings.json");
        schemas = openAPI.getComponents().getSchemas();
    }

    @Test
    public void testSupportedStringFormats()
            throws SpecException
    {
        assertSupportedStringFormat(schemas.get("stringNoFormat"), NONE);
        assertSupportedStringFormat(schemas.get("stringByteFormat"), BYTE);
        assertSupportedStringFormat(schemas.get("stringUuidFormat"), UUID);
        assertSupportedStringFormat(schemas.get("stringDateFormat"), DATE);
        assertSupportedStringFormat(schemas.get("stringDateTimeFormat"), DATE_TIME);
    }

    private static void assertSupportedStringFormat(
            Schema<?> schema,
            StringIr.Format format)
            throws SpecException
    {
        assertThat(ERROR_FACTORY.convert(schema)).isEqualTo(new StringIr(format));
    }

    @Test
    public void testUnsupportedStringFormat()
            throws SpecException
    {
        ImmutableMap.<String, SchemaIrFactory>builder()
                .put("ERROR_FACTORY", ERROR_FACTORY)
                .put("DROP_FACTORY", DROP_FACTORY)
                .buildOrThrow()
                .forEach((factoryName, factory) ->
                        assertThatThrownBy(() -> factory.convert(schemas.get("stringUnknownFormat")))
                                .as("Expect %s to throw SpecException with message", factoryName)
                                .asInstanceOf(throwable(SpecException.class))
                                .hasMessageContaining("Unsupported string format")
                                .extracting(SpecException::path)
                                .asInstanceOf(list(String.class))
                                .as("Expect %s's SpecException to link to format property")
                                .contains("format"));

        assertThat(JSON_FACTORY.convert(schemas.get("stringUnknownFormat")))
                .as("JSON_POLICY shouldn't throw SpecException but return JsonIr")
                .isEqualTo(new JsonIr());
    }
}
