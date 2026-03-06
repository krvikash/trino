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
import com.starburstdata.plugin.openapi.conversions.ir.BooleanIr;
import com.starburstdata.plugin.openapi.conversions.ir.JsonIr;
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.plugin.openapi.SpecUtil.castSchemaMap;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

/**
 * Tests {@link SchemaIrFactory} with schemas from the misc.json specification.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithMisc
{
    private static SchemaIrFactory errorFactory;
    private static SchemaIrFactory dropFactory;

    private static Map<String, Schema<?>> schemas;

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiSpec.parse("test_schema_ir_factory/misc.json");
        schemas = castSchemaMap(openAPI.getComponents().getSchemas());
        errorFactory = new SchemaIrFactory(ERROR, schemas);
        dropFactory = new SchemaIrFactory(DROP, schemas);
    }

    @Test
    public void testEmptySchema()
            throws SchemaException
    {
        assertThat(errorFactory.convert(schemas.get("empty"))).isEqualTo(new JsonIr());
    }

    @Test
    public void testEmptySchemaWithIgnoredKeywords()
            throws SchemaException
    {
        assertThat(errorFactory.convert(schemas.get("emptyWithIgnoredKeywords"))).isEqualTo(new JsonIr());
    }

    @Test
    public void testIllegalRefs()
    {
        ImmutableMap.<String, Schema<?>>builder()
                .put("ILLEGAL_EXTERNAL_REF", schemas.get("illegalExternalRef"))
                .put("ILLEGAL_FILE_REF", schemas.get("illegalFileRef"))
                .put("ILLEGAL_RELATIVE_REF", schemas.get("illegalRelativeRef"))
                .buildOrThrow()
                .forEach((var, schema) -> assertThatThrownBy(() ->
                        errorFactory.convert(schema))
                        .as("%s throws SchemaException", var)
                        .asInstanceOf(throwable(SchemaException.class))
                        .as("%s throws SchemaException from ref's IllegalArgumentException", var)
                        .hasCauseInstanceOf(IllegalArgumentException.class)
                        .extracting(SchemaException::getPath)
                        .asInstanceOf(list(String.class))
                        .as("%s throws SchemaException linking $ref property", var)
                        .containsExactly("$ref"));
    }

    @Test
    public void testRecursiveLinkedList()
            throws SchemaException
    {
        assertThatThrownBy(() -> errorFactory.convert(schemas.get("recursiveLinkedList")))
                .asInstanceOf(throwable(SchemaIrFactory.SchemaException.class))
                .hasMessageContaining("Cyclic reference detected")
                .extracting(SchemaIrFactory.SchemaException::getPath)
                .asInstanceOf(list(String.class))
                .containsExactly("properties", "\"tail\"", "$ref", "\"recursiveLinkedList\"", "properties", "\"tail\"", "$ref");

        assertThat(dropFactory.convert(schemas.get("recursiveLinkedList")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("value", new StringIr(StringIr.Format.NONE))
                                .put("tail", new ObjectIr(
                                        ImmutableMap.of("value", new StringIr(StringIr.Format.NONE)),
                                        Optional.empty()))
                                .buildOrThrow(),
                        Optional.empty()));
    }

    @Test
    public void testBooleanSchema()
            throws SchemaException
    {
        assertThat(errorFactory.convert(schemas.get("booleanSchema"))).isEqualTo(new BooleanIr());

        assertThat(errorFactory.convert(schemas.get("booleanEnum"))).isEqualTo(new BooleanIr());
    }

    @Test
    public void testBooleanKeywords()
    {
        assertThatThrownBy(() -> errorFactory.convert(schemas.get("booleanKeywords")))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("Schema uses unsupported boolean keywords");
    }

    @Test
    public void testLoneEnum()
    {
        assertThatThrownBy(() -> errorFactory.convert(schemas.get("loneEnum")))
                .asInstanceOf(throwable(SchemaException.class))
                .hasMessageContaining("Enum keyword without type keyword is unsupported")
                .extracting(SchemaException::getPath)
                .asInstanceOf(list(String.class))
                .containsExactly("enum");
    }
}
