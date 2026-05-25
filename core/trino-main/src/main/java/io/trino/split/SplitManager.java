/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.split;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.cache.CacheMetadata;
import io.trino.cache.CacheSplitSource;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.scheduler.StableHostAddressProvider;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.subquery.cache.CacheTableId;
import io.trino.spi.subquery.cache.PlanSignature;
import io.trino.split.remote.CreateRemoteSplitsTaskRequest;
import io.trino.split.remote.CreateRemoteSplitsTaskResponse;
import io.trino.split.remote.GetRemoteSplitsTaskRequest;
import io.trino.split.remote.RemoteSplitsSource;
import io.trino.split.remote.RemoteSplitsTaskResponse;
import io.trino.tracing.TrinoAttributes;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.node.NodeState.ACTIVE;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toCollection;

public class SplitManager
{
    private final CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider;
    private final Tracer tracer;
    private final int minScheduleSplitBatchSize;
    private final ExecutorService executorService;
    private final Executor executor;
    private final CatalogManager catalogManager;
    private final HttpClient httpClient;
    private final ScheduledExecutorService remoteSplitsTaskExecutor;
    private final JsonCodec<CreateRemoteSplitsTaskRequest> splitsTaskRequestCodec;
    private final JsonCodec<CreateRemoteSplitsTaskResponse> splitsCreateTaskResponseCodec;
    private final JsonCodec<RemoteSplitsTaskResponse> splitsTaskResponseCodec;
    private final JsonCodec<GetRemoteSplitsTaskRequest> splitsGetTaskRequestCodec;
    private final Metadata metadata;
    private final CacheMetadata cacheMetadata;
    private final Optional<StableHostAddressProvider> hostAddressProvider;
    private final InternalNodeManager internalNodeManager;
    private final int remoteSplitsGenerationBatchSize;
    private final Duration remoteTaskMaxErrorDuration;

    @Inject
    public SplitManager(
            CatalogServiceProvider<ConnectorSplitManager> splitManagerProvider,
            Tracer tracer,
            QueryManagerConfig config,
            CatalogManager catalogManager,
            @ForRemoteSplitsTask HttpClient httpClient,
            @ForRemoteSplitsTask ScheduledExecutorService remoteSplitsTaskExecutor,
            JsonCodec<CreateRemoteSplitsTaskRequest> splitsTaskRequestCodec,
            JsonCodec<CreateRemoteSplitsTaskResponse> splitsCreateTaskResponseCodec,
            JsonCodec<RemoteSplitsTaskResponse> splitsTaskResponseCodec,
            JsonCodec<GetRemoteSplitsTaskRequest> splitsGetTaskRequestCodec,
            Metadata metadata,
            CacheMetadata cacheMetadata,
            Optional<StableHostAddressProvider> hostAddressProvider,
            InternalNodeManager internalNodeManager)
    {
        this.splitManagerProvider = requireNonNull(splitManagerProvider, "splitManagerProvider is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.executorService = newCachedThreadPool(daemonThreadsNamed("splits-manager-callback-%s"));
        this.executor = new BoundedExecutor(executorService, config.getMaxSplitManagerCallbackThreads());
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.remoteSplitsTaskExecutor = requireNonNull(remoteSplitsTaskExecutor, "remoteSplitsTaskExecutor is null");
        this.splitsTaskRequestCodec = requireNonNull(splitsTaskRequestCodec, "splitsTaskRequestCodec is null");
        this.splitsCreateTaskResponseCodec = requireNonNull(splitsCreateTaskResponseCodec, "splitsCreateTaskResponseCodec is null");
        this.splitsTaskResponseCodec = requireNonNull(splitsTaskResponseCodec, "splitsTaskResponseCodec is null");
        this.splitsGetTaskRequestCodec = requireNonNull(splitsGetTaskRequestCodec, "splitsGetTaskRequestCodec is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
        this.hostAddressProvider = requireNonNull(hostAddressProvider, "hostAddressProvider is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.remoteSplitsGenerationBatchSize = config.getRemoteSplitsGenerationBatchSize();
        this.remoteTaskMaxErrorDuration = config.getRemoteTaskMaxErrorDuration();
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdown();
    }

    public SplitSource getSplits(
            Session session,
            Span parentSpan,
            TableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        if (!isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        SplitSource splitSource;
        try (var ignore = scopedSpan(tracer.spanBuilder("SplitManager.getSplits")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.TABLE, table.connectorHandle().toString())
                .startSpan())) {
            splitSource = createSplitSource(
                    session,
                    parentSpan,
                    table,
                    dynamicFilter,
                    constraint);
        }

        Span span = splitSourceSpan(parentSpan, catalogHandle);

        if (minScheduleSplitBatchSize > 1) {
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.empty(), "split-batch");
            splitSource = new BufferingSplitSource(splitSource, executor, minScheduleSplitBatchSize);
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-buffer");
        }
        else {
            splitSource = new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-batch");
        }

        return splitSource;
    }

    private SplitSource createSplitSource(
            Session session,
            Span parentSpan,
            TableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);

        if (metadata.useRemoteSplitsGeneration(session, table)) {
            List<InternalNode> workerNodes = internalNodeManager.getNodes(ACTIVE).stream()
                    .filter(node -> !node.isCoordinator())
                    .collect(toImmutableList());
            if (!workerNodes.isEmpty()) {
                List<HostAddress> rankedWorkers = hostAddressProvider
                        .flatMap(provider -> cacheMetadata.getCacheTableId(session, cacheMetadata.getCanonicalTableHandle(session, table))
                                .map(CacheTableId::toString)
                                .map(provider::getHosts))
                        .orElseGet(ImmutableList::of);
                Optional<CatalogProperties> catalogProperties = catalogManager.getCatalogProperties(catalogHandle);
                RemoteSplitsSource remoteSplitsSource = new RemoteSplitsSource(
                        session.toSessionRepresentation(),
                        table,
                        tracer,
                        parentSpan,
                        catalogProperties,
                        dynamicFilter,
                        constraint,
                        resolveWorkers(workerNodes, rankedWorkers),
                        httpClient,
                        remoteSplitsTaskExecutor,
                        remoteSplitsGenerationBatchSize,
                        remoteTaskMaxErrorDuration,
                        splitsTaskRequestCodec,
                        splitsCreateTaskResponseCodec,
                        splitsTaskResponseCodec,
                        splitsGetTaskRequestCodec);
                // Remote sources learn their DF-wait timeout from the create round-trip; hand its future
                // to ConnectorAwareSplitSource so the first getNextBatch chains on it without blocking.
                return new ConnectorAwareSplitSource(catalogHandle, remoteSplitsSource, dynamicFilter, remoteSplitsSource.getRequestedDynamicFilterWaitTimeoutMillisFuture());
            }
        }

        ConnectorSplitSource source = splitManager.getSplits(
                table.transaction(),
                session.toConnectorSession(catalogHandle),
                table.connectorHandle(),
                dynamicFilter.getColumnsCovered(),
                constraint);
        return new ConnectorAwareSplitSource(catalogHandle, source, dynamicFilter);
    }

    public SplitSource getSplits(Session session, Span parentSpan, TableFunctionHandle function)
    {
        CatalogHandle catalogHandle = function.catalogHandle();
        ConnectorSplitManager splitManager = splitManagerProvider.getService(catalogHandle);

        ConnectorSplitSource source;
        try (var ignore = scopedSpan(tracer.spanBuilder("SplitManager.getSplits")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.FUNCTION, function.functionHandle().toString())
                .startSpan())) {
            source = splitManager.getSplits(
                    function.transactionHandle(),
                    session.toConnectorSession(catalogHandle),
                    function.functionHandle());
        }

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogHandle, source, DynamicFilter.EMPTY);

        Span span = splitSourceSpan(parentSpan, catalogHandle);
        return new TracingSplitSource(splitSource, tracer, Optional.of(span), "split-buffer");
    }

    /**
     * Resolves the consistent-hash ring's ranking to live worker URIs for a remote split
     * planning task. The order is the ring's own: the task lands on the worker with warm
     * metadata and manifest caches, and each create retry moves to the node the ring itself
     * would promote next. Ranked hosts that are no longer active drop out, and workers the
     * lazily refreshed ring has not observed yet are appended in stable order as a fail-safe.
     * Without affinity information the workers are shuffled.
     */
    static List<URI> resolveWorkers(List<InternalNode> workerNodes, List<HostAddress> rankedWorkers)
    {
        if (rankedWorkers.isEmpty()) {
            List<URI> shuffled = workerNodes.stream()
                    .map(InternalNode::getInternalUri)
                    .collect(toCollection(ArrayList::new));
            Collections.shuffle(shuffled);
            return ImmutableList.copyOf(shuffled);
        }

        Map<HostAddress, URI> remaining = new LinkedHashMap<>();
        for (InternalNode workerNode : workerNodes) {
            remaining.putIfAbsent(workerNode.getHostAndPort(), workerNode.getInternalUri());
        }
        ImmutableList.Builder<URI> ordered = ImmutableList.builderWithExpectedSize(remaining.size());
        for (HostAddress rankedWorker : rankedWorkers) {
            // the ring refreshes lazily, so it may briefly rank hosts that are no longer active
            URI worker = remaining.remove(rankedWorker);
            if (worker != null) {
                ordered.add(worker);
            }
        }
        remaining.values().stream()
                .sorted(comparing(URI::toString))
                .forEach(ordered::add);
        return ordered.build();
    }

    public ConnectorSplitManager getConnectorSplitManager(TableHandle tableHandle)
    {
        return splitManagerProvider.getService(tableHandle.catalogHandle());
    }

    public CacheSplitSource getCacheSplitSource(
            PlanSignature signature,
            TableHandle tableHandle,
            SplitSource delegate,
            StableHostAddressProvider addressProvider)
    {
        return new CacheSplitSource(
                signature,
                getConnectorSplitManager(tableHandle),
                delegate,
                addressProvider);
    }

    private Span splitSourceSpan(Span parentSpan, CatalogHandle catalogHandle)
    {
        return tracer.spanBuilder("split-source")
                .setParent(Context.current().with(parentSpan))
                .setAttribute(TrinoAttributes.CATALOG, catalogHandle.getCatalogName().toString())
                .startSpan();
    }
}
