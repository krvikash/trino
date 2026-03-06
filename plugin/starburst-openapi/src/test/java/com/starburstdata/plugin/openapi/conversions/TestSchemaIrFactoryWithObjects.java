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
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.DROP;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy.JSON;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * Tests {@link SchemaIrFactory} with schemas from the objects.json specification.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithObjects
{
    private static Map<String, Schema> schemas;
    private static final SchemaIrFactory DROP_FACTORY = new SchemaIrFactory(DROP, emptyMap());
    private static final SchemaIrFactory ERROR_FACTORY = new SchemaIrFactory(ERROR, emptyMap());
    private static final SchemaIrFactory JSON_FACTORY = new SchemaIrFactory(JSON, emptyMap());

    @BeforeAll
    public static void init()
    {
        OpenAPI openAPI = OpenApiSpec.parse("test_schema_ir_factory/objects.json");
        schemas = openAPI.getComponents().getSchemas();
    }

    @Test
    public void testConflictingObjectSchema()
    {
        assertThatThrownBy(() -> JSON_FACTORY.convert(schemas.get("conflictingProperties")))
                .asInstanceOf(throwable(SchemaException.class))
                .hasMessageMatching("\\Qproperties: Uses keys that cannot be referenced unambiguously with case-insensitivity: conflict\\E(ing|ING)")
                .extracting(SchemaException::getPath)
                .asInstanceOf(list(String.class))
                .containsExactly("properties");
    }

    @Test
    public void testObjectIrSchemas()
            throws SchemaException
    {
        assertThat(ERROR_FACTORY.convert(schemas.get("strictProperties")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("foo", new BooleanIr())
                                .put("bar", new BooleanIr())
                                .buildOrThrow(),
                        Optional.empty()));

        assertThat(ERROR_FACTORY.convert(schemas.get("nonStrictProperties")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("foo", new BooleanIr())
                                .put("bar", new BooleanIr())
                                .buildOrThrow(),
                        Optional.empty()));
    }

    @Test
    public void testMixedPropertiesAdditionalPropertiesSchemas()
            throws SchemaException
    {
        assertThat(ERROR_FACTORY.convert(schemas.get("mixedNamedUntypedProperties")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("foo", new BooleanIr())
                                .buildOrThrow(),
                        Optional.of(new JsonIr())));

        assertThat(ERROR_FACTORY.convert(schemas.get("mixedNamedUnnamedProperties")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("foo", new BooleanIr())
                                .buildOrThrow(),
                        Optional.of(new BooleanIr())));
    }

    @Test
    public void testNoPropertiesSchemas()
            throws SchemaException
    {
        assertThat(ERROR_FACTORY.convert(schemas.get("justObject")))
                .isEqualTo(new ObjectIr(ImmutableMap.of(), Optional.of(new JsonIr())));

        assertThat(ERROR_FACTORY.convert(schemas.get("untypedMap")))
                .isEqualTo(new ObjectIr(ImmutableMap.of(), Optional.of(new JsonIr())));

        assertThat(ERROR_FACTORY.convert(schemas.get("typedMap")))
                .isEqualTo(new ObjectIr(ImmutableMap.of(), Optional.of(new BooleanIr())));
    }

    @Test
    public void testObjectOfErroringProperties()
            throws SchemaException
    {
        assertThatThrownBy(() -> ERROR_FACTORY.convert(schemas.get("objectOfErroringProperties")))
                .asInstanceOf(type(SchemaException.class))
                .extracting(SchemaException::getPath)
                .asInstanceOf(list(String.class))
                .containsExactly("additionalProperties", "format");

        assertThat(DROP_FACTORY.convert(schemas.get("objectOfErroringProperties")))
                .isEqualTo(new ObjectIr(ImmutableMap.of("valid", new BooleanIr()), Optional.empty()));

        assertThat(JSON_FACTORY.convert(schemas.get("objectOfErroringProperties")))
                .isEqualTo(new ObjectIr(
                        ImmutableMap.<String, SchemaIr>builder()
                                .put("valid", new BooleanIr())
                                .put("invalid", new JsonIr())
                                .buildOrThrow(),
                        Optional.of(new JsonIr())));
    }

    @Test
    public void testMapOfErroringValuesCastPolicy()
            throws SchemaException
    {
        Schema<?> mapOfErroringValues = schemas.get("mapOfErroringValues");
        assertThatThrownBy(() -> ERROR_FACTORY.convert(mapOfErroringValues))
                .asInstanceOf(throwable(SchemaException.class))
                .extracting(SchemaException::getPath)
                .asInstanceOf(list(String.class))
                .containsExactly("additionalProperties", "format");
        assertThat(DROP_FACTORY.convert(mapOfErroringValues))
                .isEqualTo(new ObjectIr(ImmutableMap.of(), Optional.empty()));
        assertThat(JSON_FACTORY.convert(mapOfErroringValues))
                .isEqualTo(new ObjectIr(ImmutableMap.of(), Optional.of(new JsonIr())));
    }
}
