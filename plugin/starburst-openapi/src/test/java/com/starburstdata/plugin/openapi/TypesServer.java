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
                .put("string/date", "\"2024-01-15\"")
                .put("string/date/early", "\"0001-01-01\"")
                .put("string/date/late", "\"9999-12-31\"")
                .put("string/date/epoch", "\"1970-01-01\"")
                .put("string/date/pre-epoch", "\"1969-12-31\"")
                .put("string/date-time", "\"2024-01-15T13:14:15Z\"")
                .put("string/date-time/early", "\"0001-01-01T01:01:01.123456789123Z\"")
                .put("string/date-time/late", "\"9999-12-31T01:01:01.123456000999Z\"")
                .put("string/date-time/lowercase-t", "\"2024-03-15t10:30:00.123456789Z\"")
                .put("string/date-time/lowercase-z", "\"2024-03-15T10:30:00.123456789z\"")
                .put("string/date-time/positive/offset", "\"2024-03-15T10:30:00.123456789+05:30\"")
                .put("string/date-time/negative/offset", "\"2024-03-15T10:30:00.123456789-04:30\"")
                .put("string/date-time/precision0", "\"2024-03-15T10:30:00Z\"")
                .put("string/date-time/precision3", "\"2024-03-15T10:30:00.123Z\"")
                .put("string/date-time/precision6", "\"2024-03-15T10:30:00.123456Z\"")
                .put("string/date-time/precision9", "\"2024-03-15T10:30:00.123456789Z\"")
                .put("string/date-time/precision10", "\"2024-03-15T10:30:00.1234567895Z\"")
                .put("string/date-time/precision11", "\"2024-03-15T10:30:00.12345678956Z\"")
                .put("string/date-time/precision12", "\"2024-01-15T13:14:15.123456789012Z\"")
                .put("string/date-time/precision-overflow", "\"2024-03-15T10:30:00.123456789012999Z\"")
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

        @Path("/string/date/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidDate()
        {
            return "\"not-a-date\"";
        }

        @Path("/string/date/{path: .*}/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidDate(@PathParam("path") String path)
        {
            return PATH_TO_INVALID_DATE_VALUE.get(path);
        }

        private static final Map<String, String> PATH_TO_INVALID_DATE_VALUE = ImmutableMap.<String, String>builder()
                .put("negative-year", "\"-9999-12-31\"")
                .put("incorrect-date-separator", "\"2024_02_03\"")
                .put("incorrect-format", "\"02-03-2024\"")
                .put("non-string", "20240203")
                .buildOrThrow();

        @Path("/string/date-time/{path: .*}/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidDateTime(@PathParam("path") String path)
        {
            return PATH_TO_INVALID_DATE_TIME_VALUE.get(path);
        }

        private static final Map<String, String> PATH_TO_INVALID_DATE_TIME_VALUE = ImmutableMap.<String, String>builder()
                .put("negative-year", "\"-0001-02-03T10:30:00.123456+05:30\"")
                .put("no-t-separator", "\"2024-02-03 10:30:00.123456+05:30\"")
                .put("no-offset", "\"2024-02-03T10:30:00.123456\"")
                .put("no-seconds", "\"2024-02-03T10:30Z\"")
                .put("incorrect-seconds", "\"2024-02-03T10:3Z\"")
                .put("incorrect-date-separator", "\"2024_02_03T10:30:00.123456Z\"")
                .put("incorrect-time-separator", "\"2024-02-03T10-30-00.123456+05:30\"")
                .put("incorrect-format", "\"02-03-2024 10:30:00.123456\"")
                .put("incorrect-fraction", "\"2024_02_03T10:30:00.Z\"")
                .put("non-string", "20240203103000")
                .buildOrThrow();

        @Path("/string/date-time/invalid")
        @GET
        @Produces(APPLICATION_JSON)
        public String invalidDateTime()
        {
            return "\"not-a-date-time\"";
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
