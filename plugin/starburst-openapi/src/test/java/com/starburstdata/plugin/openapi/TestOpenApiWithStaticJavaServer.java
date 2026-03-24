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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TestOpenApiWithStaticJavaServer
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        StaticJavaServer staticJavaServer = closeAfterClass(new StaticJavaServer());
        staticJavaServer.start();
        String specificationLocation = requireNonNull(
                OpenApiQueryRunner.class.getClassLoader().getResource("java_server/static.3.0.4.json"),
                "Expected java_server specification was present")
                .getFile();
        return OpenApiQueryRunner.builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("openapi.spec-location", specificationLocation)
                        .put("openapi.base-uri", staticJavaServer.getBaseUri().toString())
                        .buildOrThrow())
                .build();
    }

    @Test
    void testStubsFunctions()
    {
        assertThat(query("SELECT * FROM TABLE(openapi.default.static_boolean())"))
                .result()
                .onlyColumnAsSet()
                .singleElement()
                .isEqualTo("true");
    }
}
