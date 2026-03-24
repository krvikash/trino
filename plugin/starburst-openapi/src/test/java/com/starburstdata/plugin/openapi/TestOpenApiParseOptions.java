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

import com.google.common.io.Resources;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiParseOptions
{
    // Like in this example ... https://docs.starburst.io/latest/security/password-file.html
    private static final String SECRET_JSON = """
            { "my_super_secret": "access_key" }""";
    private static LifeCycleManager lifeCycleManager;
    private static String exfiltrationSpecificationLocation;

    @BeforeAll
    static void setUp() throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("OpenApiSecretServer"),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(OpenApiSecretServer.class));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();
        URI baseUri = server.getBaseUrl();
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        String exfiltrationSpecification = Resources
                .toString(Resources.getResource("exfiltration.json"), UTF_8)
                .replace("http://replaceme.com", baseUri.toString());
        java.nio.file.Path specificationDirectory = Files.createTempDirectory("files");
        File newSpecificationFile = File.createTempFile(
                "exfiltration",
                ".json",
                specificationDirectory.toFile());
        Files.writeString(newSpecificationFile.toPath(), exfiltrationSpecification);
        exfiltrationSpecificationLocation = newSpecificationFile.getPath();
        Files.writeString(
                specificationDirectory.resolve("secrets.json").toAbsolutePath(),
                SECRET_JSON);
    }

    @Test
    public void testOpenApiLocalOrRemoteAccess()
    {
        OpenAPI openAPI = OpenApiSpec.parse(exfiltrationSpecificationLocation);
        Map<String, Schema> schemas = assertThat(openAPI.getComponents())
                .isNotNull()
                .extracting(Components::getSchemas)
                .isNotNull()
                .actual();
        // If references aren't controlled then they can be used to steal values from JSON documents,
        // at URLs or at filesystem locations.
        assertThat(schemas.get("external"))
                .isNotNull()
                .extracting(Schema::getJsonSchema)
                .isNull();
        assertThat(schemas.get("filesystem"))
                .isNotNull()
                .extracting(Schema::getJsonSchema)
                .isNull();
    }

    @AfterAll
    static void close()
    {
        lifeCycleManager.stop();
    }

    @Path("/")
    public static class OpenApiSecretServer
    {
        @GET
        @Path("secrets")
        @Produces(MediaType.APPLICATION_JSON)
        public String getSecretJson()
        {
            return SECRET_JSON;
        }
    }
}
