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
import com.starburstdata.plugin.openapi.conversions.ir.ArrayIr;
import com.starburstdata.plugin.openapi.conversions.ir.BooleanIr;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.FALLBACK;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

/**
 * Tests {@link SchemaIrFactory} with schemas from the arrays.json description.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithArrays
{
    private static Map<String, Schema> schemas;

    private static final SchemaIrFactory DROP_FACTORY = new SchemaIrFactory(DROP, emptyMap());
    private static final SchemaIrFactory ERROR_FACTORY = new SchemaIrFactory(ERROR, emptyMap());
    private static final SchemaIrFactory FALLBACK_FACTORY = new SchemaIrFactory(FALLBACK, emptyMap());

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiDescription.parse("test_schema_ir_factory/arrays.json");
        schemas = openAPI.getComponents().getSchemas();
    }

    @Test
    public void testTypedArray()
            throws SpecException
    {
        assertThat(DROP_FACTORY.convert(schemas.get("typedArray")))
                .isEqualTo(new ArrayIr(new BooleanIr()));
    }

    @Test
    public void testArrayOfError()
            throws SpecException
    {
        ImmutableMap.<String, SchemaIrFactory>builder()
                .put("ERROR_FACTORY", ERROR_FACTORY)
                .put("DROP_FACTORY", DROP_FACTORY)
                .buildOrThrow()
                .forEach((factoryName, factory) ->
                        assertThatThrownBy(() -> factory.convert(schemas.get("arrayOfError")))
                                .as("Using %s should throw SpecException with the following properties", factoryName)
                                .asInstanceOf(throwable(SpecException.class))
                                .extracting(SpecException::path)
                                .asInstanceOf(list(String.class))
                                .containsExactly("items", "format"));

        assertThat(FALLBACK_FACTORY.convert(schemas.get("arrayOfError")))
                .as("FALLBACK_FACTORY shouldn't throw SpecException and cast values to JSON")
                .isEqualTo(new ArrayIr(new JsonIr()));
    }
}
