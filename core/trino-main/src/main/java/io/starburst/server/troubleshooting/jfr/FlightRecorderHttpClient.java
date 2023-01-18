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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.server.troubleshooting.jfr.FlightRecorderWorkerResource.BASE_PATH_API_V1;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePatch;
import static io.airlift.http.client.ResponseHandlerUtils.readResponseBytes;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.Boolean.parseBoolean;
import static java.util.Objects.requireNonNull;

class FlightRecorderHttpClient
{
    private final HttpClient client;
    private final QueryId queryId;
    private final WorkerNodesProvider workerNodesProvider;
    private final ExecutorService executorService;

    private FlightRecorderHttpClient(QueryId queryId, HttpClient client, ExecutorService executorService, WorkerNodesProvider workerNodesProvider)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.client = requireNonNull(client, "client is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.workerNodesProvider = requireNonNull(workerNodesProvider, "workerNodesProvider is null");
    }

    public void start(Set<String> nodeIds)
    {
        for (String nodeId : nodeIds) {
            Request request = prepareDelete()
                    .setUri(workerNodesProvider.getWorkerURI(nodeId).resolve(BASE_PATH_API_V1.replace("{queryId}", queryId.getId())).resolve("/start"))
                    .build();

            // TODO: https://starburstdata.atlassian.net/browse/SEP-10546
            client.execute(request, createStringResponseHandler());
        }
    }

    public void remove(Set<String> nodeIds)
    {
        for (String nodeId : nodeIds) {
            Request request = prepareDelete()
                    .setUri(workerNodesProvider.getWorkerURI(nodeId).resolve(BASE_PATH_API_V1.replace("{queryId}", queryId.getId())))
                    .build();

            // TODO: https://starburstdata.atlassian.net/browse/SEP-10546
            client.execute(request, createStringResponseHandler());
        }
    }

    public void finish(Set<String> nodeIds)
    {
        for (String nodeId : nodeIds) {
            Request request = preparePatch()
                    .setUri(workerNodesProvider.getWorkerURI(nodeId).resolve(BASE_PATH_API_V1.replace("{queryId}", queryId.getId())).resolve("/finish"))
                    .build();

            // TODO: https://starburstdata.atlassian.net/browse/SEP-10546
            client.execute(request, createStringResponseHandler());
        }
    }

    public Map<String, InputStream> getInputStreams(Set<String> nodeIds)
    {
        ImmutableMap.Builder<String, InputStream> inputStreams = ImmutableMap.builder();

        for (String nodeId : nodeIds) {
            Request request = prepareGet()
                    .setUri(workerNodesProvider.getWorkerURI(nodeId).resolve(BASE_PATH_API_V1.replace("{queryId}", queryId.getId())).resolve("/download"))
                    .build();
            // TODO: https://starburstdata.atlassian.net/browse/SEP-10546
            InputStreamResponseHandler.InputStreamResponse response = client.execute(request, InputStreamResponseHandler.createInputStreamResponseHandler());
            inputStreams.put("recordings/worker-" + nodeId + ".jfr", response.inputStream());
        }

        return inputStreams.buildOrThrow();
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

    public static class Factory
    {
        private final HttpClient client;
        private final ExecutorService executorService;
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
