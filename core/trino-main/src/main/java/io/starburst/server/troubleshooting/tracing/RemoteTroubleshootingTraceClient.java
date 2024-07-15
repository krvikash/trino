/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.tracing;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.starburst.server.troubleshooting.DownloadResult;
import io.starburst.server.troubleshooting.ForTroubleshooting;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.starburst.server.troubleshooting.tracing.RemoteTroubleshootingTraceClient.StatusCodeCheckResponseHandler.checkResponseStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static java.util.Objects.requireNonNull;

public class RemoteTroubleshootingTraceClient
{
    private static final Logger log = Logger.get(RemoteTroubleshootingTraceClient.class);
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;

    @Inject
    public RemoteTroubleshootingTraceClient(InternalNodeManager nodeManager, @ForTroubleshooting HttpClient httpClient)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public void start(QueryId queryId)
    {
        callOnOtherNodes(node -> start(queryId, node));
    }

    public void remove(QueryId queryId)
    {
        callOnOtherNodes(node -> remove(queryId, node));
    }

    public Map<Node, DownloadResult> download(QueryId queryId, Set<String> processingNodesForQuery)
    {
        List<Map.Entry<InternalNode, Future<InputStream>>> futures = getNodes()
                .filter(node -> processingNodesForQuery.contains(node.getNodeIdentifier()))
                .map(node -> Map.entry(node, download(queryId, node)))
                .collect(toImmutableList());
        return futures.stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> {
                    try {
                        return DownloadResult.ofInputStream(entry.getValue().get());
                    }
                    catch (Exception e) {
                        return DownloadResult.ofException(e);
                    }
                }));
    }

    public void retain(QueryId queryId, Set<String> processingNodesForQuery)
    {
        callOnOtherNodes(node -> remove(queryId, node), node -> !processingNodesForQuery.contains(node.getNodeIdentifier()));
    }

    private Future<Void> start(QueryId queryId, InternalNode node)
    {
        log.info("start node: %s, queryId: %s", node, queryId);
        Request request = preparePost()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath("/api/v1/troubleshooting/trace")
                        .appendPath(queryId.toString())
                        .appendPath("start")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<Void> remove(QueryId queryId, InternalNode node)
    {
        log.info("remove node: %s, queryId: %s", node, queryId);
        Request request = prepareDelete()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath("/api/v1/troubleshooting/trace")
                        .appendPath(queryId.toString())
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.executeAsync(request, checkResponseStatusCode());
    }

    private Future<InputStream> download(QueryId queryId, InternalNode node)
    {
        log.info("download node: %s, queryId: %s", node, queryId);
        Request request = prepareGet()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath("/api/v1/troubleshooting/trace")
                        .appendPath(queryId.toString())
                        .appendPath("download")
                        .build())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.executeAsync(request, new InputStreamResponseHandler());
    }

    private void callOnOtherNodes(Function<InternalNode, Future<Void>> call)
    {
        callOnOtherNodes(call, node -> true);
    }

    private void callOnOtherNodes(Function<InternalNode, Future<Void>> call, Predicate<InternalNode> nodePredicate)
    {
        List<Future<Void>> futures = getNodes()
                .filter(nodePredicate)
                .map(call)
                .collect(toImmutableList());
        // Wait for all to finish. Throw if any request fails
        futures.forEach(Futures::getUnchecked);
    }

    private Stream<InternalNode> getNodes()
    {
        return nodeManager.getAllNodes()
                .getActiveNodes()
                .stream()
                .filter(node -> !node.equals(nodeManager.getCurrentNode()));
    }

    private static class InputStreamResponseHandler
            implements ResponseHandler<InputStream, RuntimeException>
    {
        @Override
        public InputStream handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public InputStream handle(Request request, Response response)
        {
            try {
                if (!(response.getStatusCode() == HttpStatus.OK.code())) {
                    throw new TrinoException(
                            StandardErrorCode.GENERIC_INTERNAL_ERROR,
                            "request failed with http status code: %s, request %s, response: %s".formatted(response.getStatusCode(), request, response));
                }
                return response.getInputStream();
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to read response from worker", e);
            }
        }
    }

    public static class StatusCodeCheckResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        public static StatusCodeCheckResponseHandler checkResponseStatusCode()
        {
            return new StatusCodeCheckResponseHandler();
        }

        @Override
        public Void handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public Void handle(Request request, Response response)
                throws RuntimeException
        {
            if (!isOk(response)) {
                throw new TrinoException(
                        StandardErrorCode.GENERIC_INTERNAL_ERROR,
                        "request failed with http status code: %s, request %s, response: %s".formatted(response.getStatusCode(), request, response));
            }
            return null;
        }

        private static boolean isOk(Response response)
        {
            return (response.getStatusCode() == HttpStatus.OK.code()) ||
                    (response.getStatusCode() == HttpStatus.NO_CONTENT.code());
        }
    }
}
