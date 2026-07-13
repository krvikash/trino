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
import com.starburstdata.plugin.openapi.conversions.ir.NumberIr;
import com.starburstdata.plugin.openapi.conversions.ir.ObjectIr;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.conversions.ir.StringIr;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy.ERROR;
import static com.starburstdata.plugin.openapi.OpenApiDescription.MIME_JSON;
import static com.starburstdata.plugin.openapi.SpecUtil.castSchemaMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link SchemaIrFactory} with schemas from the petstore description.
 *
 * @see test_schema_ir_factory
 */
public class TestSchemaIrFactoryWithPetstore
{
    private static OpenAPI openApi;
    private static SchemaIrFactory schemaIrFactory;

    @BeforeAll
    public static void init()
    {
        openApi = OpenApiDescription.parse("petstore.yaml");
        schemaIrFactory = new SchemaIrFactory(ERROR, castSchemaMap(openApi.getComponents().getSchemas()));
    }

    @Test
    public void testPetstoreFindByStatusParameters()
            throws SpecException
    {
        Operation getFindByStatus = openApi.getPaths().get("/pet/findByStatus").getGet();
        Parameter statusParameter = getFindByStatus.getParameters().getFirst();
        assertThat(statusParameter.getName()).isEqualTo("status");
        SchemaIr statusIr = schemaIrFactory.convert(statusParameter.getSchema());
        assertThat(statusIr).isEqualTo(new StringIr(StringIr.Format.NONE));
    }

    @Test
    public void testPetstoreFindByStatusResponse()
            throws SpecException
    {
        Operation getFindByStatus = openApi.getPaths().get("/pet/findByStatus").getGet();
        Schema<?> okResponseSchema = getFindByStatus.getResponses().get("200").getContent().get(MIME_JSON).getSchema();
        SchemaIr responseIr = schemaIrFactory.convert(okResponseSchema);
        assertThat(responseIr).isEqualTo(new ArrayIr(new ObjectIr(
                ImmutableMap.<String, SchemaIr>builder()
                        .put("id", new NumberIr(NumberIr.Format.INT64))
                        .put("name", new StringIr(StringIr.Format.NONE))
                        .put("category", new ObjectIr(
                                ImmutableMap.<String, SchemaIr>builder()
                                        .put("id", new NumberIr(NumberIr.Format.INT64))
                                        .put("name", new StringIr(StringIr.Format.NONE))
                                        .buildOrThrow(),
                                Optional.empty()))
                        .put("photoUrls", new ArrayIr(new StringIr(StringIr.Format.NONE)))
                        .put("tags", new ArrayIr(new ObjectIr(
                                ImmutableMap.<String, SchemaIr>builder()
                                        .put("id", new NumberIr(NumberIr.Format.INT64))
                                        .put("name", new StringIr(StringIr.Format.NONE))
                                        .buildOrThrow(),
                                Optional.empty())))
                        .put("status", new StringIr(StringIr.Format.NONE))
                        .buildOrThrow(),
                Optional.empty())));
    }
}
