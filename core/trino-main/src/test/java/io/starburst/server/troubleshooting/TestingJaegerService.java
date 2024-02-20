/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.opentelemetry.exporter.internal.grpc.GrpcExporter;
import io.opentelemetry.exporter.internal.grpc.GrpcExporterBuilder;
import io.opentelemetry.exporter.internal.marshal.Marshaler;
import io.opentelemetry.exporter.internal.marshal.Serializer;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.trino.spi.QueryId;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TestingJaegerService
        implements Closeable
{
    private static final Logger log = Logger.get(TestingJaegerService.class);
    private static final int GRPC_PORT = 4317;
    private static final int UI_PORT = 16686;
    private final ObjectMapper mapper = new ObjectMapper();
    private final GenericContainer<?> jaegerDockerContainer;
    private final OkHttpClient jaegerHttpClient;

    public TestingJaegerService()
    {
        jaegerDockerContainer = new GenericContainer<>(DockerImageName.parse("jaegertracing/all-in-one:1.54"))
                .withExposedPorts(GRPC_PORT, UI_PORT)
                .withEnv("COLLECTOR_OTLP_ENABLED", "true");
        jaegerHttpClient = new OkHttpClient();
    }

    public void start()
    {
        jaegerDockerContainer.start();
        log.info("jaeger docker container started, ui url: %s", getUiUri());
    }

    @Override
    public void close()
    {
        jaegerDockerContainer.stop();
    }

    public URI getGrpcUri()
    {
        try {
            return new URI("http://localhost:" + jaegerDockerContainer.getMappedPort(GRPC_PORT));
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public URI getUiUri()
    {
        try {
            return new URI("http://localhost:" + jaegerDockerContainer.getMappedPort(UI_PORT));
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean exportOpenTelemetryData(byte[] data)
    {
        requireNonNull(data, "data is null");

        GrpcExporterBuilder<RawMarshaler> grpcExporterBuilder = new GrpcExporterBuilder<>(
                "otlp",
                "span",
                10L,
                getGrpcUri(),
                () -> null,
                "/opentelemetry.proto.collector.trace.v1.TraceService/Export");
        GrpcExporter<RawMarshaler> grpcExporter = grpcExporterBuilder.build();

        // 10 seconds timeout left for CI environment
        CompletableResultCode exportResult = grpcExporter.export(new RawMarshaler(data), 1)
                .join(10, TimeUnit.SECONDS);

        return exportResult.isDone() && exportResult.isSuccess();
    }

    public JsonNode getTraces()
    {
        JsonNode tracesRootNode = callJaegerApi(getUiUri().resolve("/api/traces?service=trino"));
        return tracesRootNode.get("data");
    }

    public JsonNode getTraceSpans(QueryId queryId)
    {
        String traceId = findTraceId(queryId);
        if (traceId != null) {
            JsonNode traceRootNode = callJaegerApi(getUiUri().resolve("/api/traces/" + traceId));
            if (traceRootNode.has("data")) {
                JsonNode dataNode = traceRootNode.get("data");
                if (dataNode.size() == 1) {
                    return dataNode.get(0).get("spans");
                }
            }
        }
        return null;
    }

    private String findTraceId(QueryId queryId)
    {
        JsonNode traceNodes = getTraces();
        for (JsonNode traceNode : traceNodes) {
            if (!traceNode.has("traceID") || !traceNode.has("spans")) {
                continue;
            }

            String traceId = traceNode.get("traceID").asText();
            for (JsonNode spanNode : traceNode.get("spans")) {
                if (!spanNode.has("tags")) {
                    continue;
                }

                for (JsonNode tagNode : spanNode.get("tags")) {
                    if (!tagNode.has("key")) {
                        continue;
                    }

                    if (tagNode.get("key").asText().equals("trino.query_id")) {
                        String tagNodeValue = tagNode.get("value").asText();
                        if (tagNodeValue.equals(queryId.getId())) {
                            return traceId;
                        }
                    }
                }
            }
        }
        return null;
    }

    private JsonNode callJaegerApi(URI uri)
    {
        Request request;
        try {
            request = new Request.Builder().url(uri.toURL()).get().build();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        try (Response response = jaegerHttpClient.newCall(request).execute()) {
            ResponseBody body = response.body();
            if (body == null) {
                throw new RuntimeException("empty response body");
            }
            return mapper.readTree(body.bytes());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class RawMarshaler
            extends Marshaler
    {
        private final byte[] data;

        public RawMarshaler(byte[] data)
        {
            requireNonNull(data, "data is null");
            this.data = data;
        }

        @Override
        public int getBinarySerializedSize()
        {
            return data.length;
        }

        @Override
        protected void writeTo(Serializer serializer)
                throws IOException
        {
            serializer.writeSerializedMessage(data, "");
        }
    }
}
