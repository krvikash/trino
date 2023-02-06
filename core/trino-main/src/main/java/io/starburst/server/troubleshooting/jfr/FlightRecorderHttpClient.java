/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.common.collect.ImmutableMap;
import io.starburst.server.troubleshooting.ForTroubleshooting;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseHandlerUtils;
import io.trino.server.InternalCommunicationConfig;
import io.trino.spi.QueryId;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.server.troubleshooting.jfr.FlightRecorderHttpClient.InputStreamResponseHandler.createInputStreamResponseHandler;
import static io.starburst.server.troubleshooting.jfr.FlightRecorderHttpClient.StatusCheckingResponseHandler.createStatusCheckingHandler;
import static io.starburst.server.troubleshooting.jfr.FlightRecorderWorkerResource.BASE_PATH_API_V1;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.readResponseBytes;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

class FlightRecorderHttpClient
{
    private final HttpClient client;
    private final QueryId queryId;
    private final WorkerNodesProvider workerNodesProvider;
    private final FailsafeExecutor<Object> failsafeExecutor;

    private FlightRecorderHttpClient(QueryId queryId, HttpClient client, ScheduledExecutorService executorService, WorkerNodesProvider workerNodesProvider)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.client = requireNonNull(client, "client is null");
        this.workerNodesProvider = requireNonNull(workerNodesProvider, "workerNodesProvider is null");

        this.failsafeExecutor = Failsafe.with(RetryPolicy.builder()
                .withMaxDuration(Duration.of(3, SECONDS))
                .withMaxAttempts(-1)
                .withBackoff(10, 250, MILLIS)
                .handleIf(FlightRecorderHttpClient::requestCanBeRetried)
                .build())
            .with(executorService);
    }

    public void start(Set<String> nodeIds)
    {
        HttpResponses<String, RuntimeException> results = paralellExecute(nodeIds, this::startRequest, createStatusCheckingHandler());
        if (!results.exceptions().isEmpty()) {
            throw new RuntimeException("Could not start recordings on nodes: %s due to: %s".formatted(results.exceptions().keySet(), results.exceptions.values()));
        }
    }

    public void remove(Set<String> nodeIds)
    {
        HttpResponses<String, RuntimeException> results = paralellExecute(nodeIds, this::removeRequest, createStatusCheckingHandler());
        if (!results.exceptions().isEmpty()) {
            throw new RuntimeException("Could not remove recordings on nodes: %s due to: %s".formatted(results.exceptions().keySet(), results.exceptions.values()));
        }
    }

    public void finish(Set<String> nodeIds)
    {
        HttpResponses<String, RuntimeException> results = paralellExecute(nodeIds, this::finishRequest, createStatusCheckingHandler());
        if (!results.exceptions().isEmpty()) {
            throw new RuntimeException("Could not finish recordings on nodes: %s due to: %s".formatted(results.exceptions().keySet(), results.exceptions.values()));
        }
    }

    public Map<String, InputStream> getInputStreams(Set<String> nodeIds)
    {
        HttpResponses<InputStreamResponseHandler.InputStreamResponse, RuntimeException> results = paralellExecute(nodeIds, this::inputStreamRequest, createInputStreamResponseHandler());
        if (!results.exceptions().isEmpty()) {
            throw new RuntimeException("Could not get recordings from nodes: %s due to: %s".formatted(results.exceptions().keySet(), results.exceptions.values()));
        }

        return results.responses().entrySet().stream()
                .map(entry -> Map.entry("recordings/worker-" + entry.getKey() + ".jfr", entry.getValue().inputStream()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Request startRequest(String nodeId)
    {
        return prepareGet()
                .setUri(baseUri(nodeId).resolve("start"))
                .build();
    }

    private Request removeRequest(String nodeId)
    {
        return prepareDelete()
                .setUri(baseUri(nodeId))
                .build();
    }

    private Request finishRequest(String nodeId)
    {
        return prepareGet()
                .setUri(baseUri(nodeId).resolve("finish"))
                .build();
    }

    private Request inputStreamRequest(String nodeId)
    {
        return prepareGet()
                .setUri(baseUri(nodeId).resolve("download"))
                .build();
    }

    private URI baseUri(String nodeId)
    {
        return workerNodesProvider.getWorkerURI(nodeId).resolve(BASE_PATH_API_V1.replace("{queryId}", queryId.getId()));
    }

    private static boolean requestCanBeRetried(Throwable throwable)
    {
        if (throwable instanceof HttpStatusException hex) {
            return hex.getStatusCode() > 499 && hex.getStatusCode() < 600;
        }

        return false;
    }

    private <R, E extends RuntimeException> HttpResponses<R, E> paralellExecute(Set<String> nodeIds, Function<String, Request> requestFactory, ResponseHandler<R, E> handler)
    {
        ImmutableMap.Builder<String, R> responses = ImmutableMap.builder();
        ImmutableMap.Builder<String, E> exceptions = ImmutableMap.builder();
        CountDownLatch latch = new CountDownLatch(nodeIds.size()); // wait for all nodes to either respond or fail

        for (String nodeId : nodeIds) {
            Request request = requestFactory.apply(nodeId);
            final String currentNodeId = nodeId;

            failsafeExecutor.onComplete(event -> {
                if (event.getException() != null) {
                    exceptions.put(currentNodeId, (E) event.getException());
                }
                else {
                    responses.put(currentNodeId, (R) event.getResult());
                }
                latch.countDown();
            }).get(() -> client.execute(request, handler));
        }

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return new HttpResponses<>(responses.buildOrThrow(), exceptions.buildOrThrow());
    }

    @SuppressWarnings("UnusedVariable") // error-prone is too dumb to see access to both responses and exceptions fields
    private record HttpResponses<T, E extends Throwable>(Map<String, T> responses, Map<String, E> exceptions)
    {
    }

    static final class InputStreamResponseHandler
            implements ResponseHandler<InputStreamResponseHandler.InputStreamResponse, RuntimeException>
    {
        private static final InputStreamResponseHandler INPUT_STREAM_RESPONSE_HANDLER = new InputStreamResponseHandler();

        public static InputStreamResponseHandler createInputStreamResponseHandler()
        {
            return INPUT_STREAM_RESPONSE_HANDLER;
        }

        private InputStreamResponseHandler() {}

        @Override
        public InputStreamResponse handleException(Request request, Exception exception)
        {
            throw ResponseHandlerUtils.propagate(request, exception);
        }

        @Override
        public InputStreamResponse handle(Request request, Response response)
        {
            if (response.getStatusCode() >= 300) {
                throw new HttpStatusException("Expected status code to be 2xx but got %d".formatted(response.getStatusCode()), response);
            }

            return new InputStreamResponse(
                    response.getStatusCode(),
                    new ByteArrayInputStream(readResponseBytes(request, response)));
        }

        private record InputStreamResponse(int statusCode, InputStream inputStream)
        {
            public InputStreamResponse
            {
                requireNonNull(inputStream, "inputStream is null");
            }
        }
    }

    static final class StatusCheckingResponseHandler
            implements ResponseHandler<String, RuntimeException>
    {
        private static final StatusCheckingResponseHandler STATUS_CHECKING_HANDLER = new StatusCheckingResponseHandler();

        public static StatusCheckingResponseHandler createStatusCheckingHandler()
        {
            return STATUS_CHECKING_HANDLER;
        }

        private StatusCheckingResponseHandler() {}

        @Override
        public String handleException(Request request, Exception exception)
        {
            throw ResponseHandlerUtils.propagate(request, exception);
        }

        @Override
        public String handle(Request request, Response response)
        {
            if (response.getStatusCode() < 300) {
                return new String(readResponseBytes(request, response), UTF_8);
            }

            throw new HttpStatusException("Expected error code 2xx, got: %d".formatted(response.getStatusCode()), response);
        }
    }

    public static class HttpStatusException
            extends RuntimeException
    {
        private final Response response;

        private HttpStatusException(String message, Response response)
        {
            super(message);
            this.response = requireNonNull(response, "response is null");
        }

        public Response getResponse()
        {
            return response;
        }

        public int getStatusCode()
        {
            return response.getStatusCode();
        }
    }

    public static class Factory
    {
        private final HttpClient client;
        private final ScheduledExecutorService executorService;
        private final WorkerNodesProvider workerNodesProvider;

        @Inject
        public Factory(@ForTroubleshooting HttpClient client, @ForTroubleshooting ScheduledExecutorService executorService, WorkerNodesProvider workerNodesProvider)
        {
            this.client = requireNonNull(client, "client is null");
            this.executorService = requireNonNull(executorService, "executorService is null");
            this.workerNodesProvider = requireNonNull(workerNodesProvider, "workerNodesProvider is null");
        }

        public FlightRecorderHttpClient create(QueryId queryId)
        {
            return new FlightRecorderHttpClient(queryId, client, executorService, workerNodesProvider);
        }
    }

    public static class WorkerNodesProvider
    {
        private final ServiceSelector selector;
        private final boolean isHttpsRequired;

        @Inject
        public WorkerNodesProvider(@ServiceType("trino") ServiceSelector selector, InternalCommunicationConfig internalCommunicationConfig)
        {
            this.selector = requireNonNull(selector, "selector is null");
            this.isHttpsRequired = requireNonNull(internalCommunicationConfig, "internalCommunicationConfig is null").isHttpsRequired();
        }

        public Map<String, ServiceDescriptor> getWorkerNodes()
        {
            return selector.selectAllServices().stream()
                    .filter(WorkerNodesProvider::isWorker)
                    .map(descriptor -> Map.entry(descriptor.getNodeId(), descriptor))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public Set<String> getWorkerNodesIds()
        {
            return getWorkerNodes().keySet();
        }

        public URI getWorkerURI(String nodeId)
        {
            return getWorkerNodeURI(getWorkerNodes().get(nodeId));
        }

        private static boolean isWorker(ServiceDescriptor descriptor)
        {
            return !parseBoolean(descriptor.getProperties().getOrDefault("coordinator", "false"));
        }

        private URI getWorkerNodeURI(ServiceDescriptor descriptor)
        {
            return URI.create(descriptor.getProperties().get(isHttpsRequired ? "https" : "http"));
        }
    }
}
