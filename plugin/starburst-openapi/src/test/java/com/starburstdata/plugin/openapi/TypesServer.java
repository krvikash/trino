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
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;

import java.io.Closeable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.Map;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Runs a server that returns static strings to test different types of schema deserializations.
 */
public class TypesServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final URI baseUrl;

    public TypesServer()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new JsonModule(),
                new TestingHttpServerModule("TypesServer"),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(Server.class));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        server = injector.getInstance(TestingHttpServer.class);
        baseUrl = server.getBaseUrl();
    }

    public void start()
            throws Exception
    {
        server.start();
    }

    public URI getBaseUri()
    {
        return baseUrl;
    }

    @Override
    public void close()
    {
        lifeCycleManager.stop();
    }

    @Path("/")
    public static class Server
    {
        @Path("/{path: .*}/null")
        @GET
        @Produces(APPLICATION_JSON)
        public String writesNull()
        {
            return "null";
        }

        @Path("/{path: .*}/unexpectedobject")
        @GET
        @Produces(APPLICATION_JSON)
        public String writesObject()
        {
            return "{}";
        }

        @Path("/{path: .*}/unexpectedarray")
        @GET
        @Produces(APPLICATION_JSON)
        public String writesArray()
        {
            return "[]";
        }

        @Path("/{path: .*}/valid")
        @GET
        @Produces(APPLICATION_JSON)
        public String valid(@PathParam("path") String path)
        {
            return PATH_TO_VALID_VALUE.get(path);
        }

        private static final Map<String, String> PATH_TO_VALID_VALUE = ImmutableMap.<String, String>builder()
                .put("boolean", "true")
                .put("string/none", "\"Hello World!\"")
                .put("string/byte", "\"SGVsbG8gV29ybGQh\"")
                .put("string/uuid", "\"687978a3-2c79-4a1f-81b9-e64dfe355737\"")
                .put("number/none", "4.9E-325") // Double.MIN_VALUE / 10
                .put("integer/none", "9223372036854775808.0") // Long.MAX_VALUE + 1
                .buildOrThrow();

        @Path("/json/value")
        @GET
        @Produces(APPLICATION_JSON)
        public String any()
        {
            return """
                    { "whole": { "bunch": [ { "of": "types", "test": true, "testing": 1 } ] } }""";
        }

        @Path("/string/byte/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidByte()
        {
            return "\"@\"";
        }

        @Path("/string/uuid/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidUuid()
        {
            return "\"ZZZZ\"";
        }

        @Path("/integer/none/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidInteger()
        {
            return "1.5";
        }

        @Path("/integer/int32/style/{style}")
        @GET
        @Produces(APPLICATION_JSON)
        public String int32ByStyle(@PathParam("style") String style)
        {
            return switch (style) {
                case "invalid" -> "0.5";
                case "min" -> Integer.toString(Integer.MIN_VALUE);
                case "max" -> Integer.toString(Integer.MAX_VALUE);
                case "toonegative" -> BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.ONE).toString();
                case "toopositive" -> BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE).toString();
                default -> throw new UnsupportedOperationException(style);
            };
        }

        @Path("/integer/int64/style/{style}")
        @GET
        @Produces(APPLICATION_JSON)
        public String int64ByStyle(@PathParam("style") String style)
        {
            return switch (style) {
                case "invalid" -> "0.5";
                case "min" -> Long.toString(Long.MIN_VALUE);
                case "max" -> Long.toString(Long.MAX_VALUE);
                case "toonegative" -> BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toString();
                case "toopositive" -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toString();
                default -> throw new UnsupportedOperationException(style);
            };
        }

        @Path("/number/float/style/{style}")
        @GET
        @Produces(APPLICATION_JSON)
        public String floatByStyle(@PathParam("style") String style)
        {
            return switch (style) {
                case "min" -> BigDecimal.valueOf(-Float.MAX_VALUE).toPlainString();
                case "precise" -> BigDecimal.valueOf(Float.MIN_VALUE).toPlainString();
                case "max" -> BigDecimal.valueOf(Float.MAX_VALUE).toPlainString();
                case "toonegative" -> BigDecimal.valueOf(-Float.MAX_VALUE).subtract(BigDecimal.ONE).toPlainString();
                case "tooprecise" -> BigDecimal.valueOf(Float.MIN_VALUE).movePointLeft(1).toPlainString();
                case "toopositive" -> BigDecimal.valueOf(Float.MAX_VALUE).add(BigDecimal.ONE).toPlainString();
                default -> throw new UnsupportedOperationException(style);
            };
        }

        @Path("/number/double/style/{style}")
        @GET
        @Produces(APPLICATION_JSON)
        public String doubleByStyle(@PathParam("style") String style)
        {
            return switch (style) {
                case "min" -> BigDecimal.valueOf(-Double.MAX_VALUE).toPlainString();
                case "precise" -> BigDecimal.valueOf(Double.MIN_VALUE).toPlainString();
                case "max" -> BigDecimal.valueOf(Double.MAX_VALUE).toPlainString();
                case "toonegative" -> BigDecimal.valueOf(-Double.MAX_VALUE).subtract(BigDecimal.ONE).toPlainString();
                case "tooprecise" -> BigDecimal.valueOf(Double.MIN_VALUE).movePointLeft(1).toPlainString();
                case "toopositive" -> BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE).toPlainString();
                default -> throw new UnsupportedOperationException(style);
            };
        }

        @Path("/array/valid")
        @GET
        @Produces(APPLICATION_JSON)
        public String validArray()
        {
            return "[true, false, null]";
        }

        @Path("/array/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidArray()
        {
            return "[true, false, null, 0]";
        }

        @Path("/object/map/valid")
        @GET
        @Produces(APPLICATION_JSON)
        public String validMap()
        {
            return """
                    { "valid": true, "also_valid": false, "also_also_valid": null }
                    """;
        }

        @Path("/object/map/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidMap()
        {
            return """
                    { "valid": true, "invalid": 0 }
                    """;
        }

        @Path("/object/row/valid")
        @GET
        @Produces(APPLICATION_JSON)
        public String validRow()
        {
            return """
                    { "present": true, "null": null, "dropped": true }
                    """;
        }

        @Path("/object/row/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidRow()
        {
            return """
                    { "typed": 0 }
                    """;
        }
    }

    static void main()
            throws Exception
    {
        TypesServer server = new TypesServer();
        server.start();
        System.out.println("== SERVER STARTED ==");
        System.out.println(server.getBaseUri());
        System.out.println("====================");
    }
}
