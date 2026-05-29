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
package io.trino.connector;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.SystemConnector;
import io.trino.connector.system.SystemTablesProvider;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.LocalMemoryManager;
import io.trino.metadata.InternalFunctionBundleFactory;
import io.trino.metadata.Metadata;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.security.AccessControl;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.connector.ManagedStatisticsClient;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.transaction.TransactionManager;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.connector.CatalogHandle.createInformationSchemaCatalogHandle;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.connector.CatalogHandle.createSystemTablesCatalogHandle;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultCatalogFactory
        implements CatalogFactory
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    private final InternalNode currentNode;
    private final InternalNodeManager nodeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final PageStreamFactory pageStreamFactory;
    private final VersionEmbedder versionEmbedder;
    private final OpenTelemetry openTelemetry;
    private final TransactionManager transactionManager;
    private final TypeManager typeManager;

    private final boolean schedulerIncludeCoordinator;
    private final int maxPrefetchedInformationSchemaPrefixes;
    private final AiModelAccessControl aiModelAccessControl;
    private final ModelConnectionSpecsLoader modelConnectionSpecsLoader;
    private final ManagedStatisticsClient managedStatisticsClient;

    private final ConcurrentMap<ConnectorName, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final LocalMemoryManager localMemoryManager;
    private final SecretsResolver secretsResolver;
    private final NodeInfo nodeInfo;

    @Inject
    public DefaultCatalogFactory(
            Metadata metadata,
            AccessControl accessControl,
            InternalNode currentNode,
            InternalNodeManager nodeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            PageStreamFactory pageStreamFactory,
            VersionEmbedder versionEmbedder,
            OpenTelemetry openTelemetry,
            TransactionManager transactionManager,
            TypeManager typeManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            AiModelAccessControl aiModelAccessControl,
            ModelConnectionSpecsLoader modelConnectionSpecsLoader,
            ManagedStatisticsClient managedStatisticsClient,
            OptimizerConfig optimizerConfig,
            LocalMemoryManager localMemoryManager,
            SecretsResolver secretsResolver,
            NodeInfo nodeInfo)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.pageStreamFactory = requireNonNull(pageStreamFactory, "pageStreamFactory is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.maxPrefetchedInformationSchemaPrefixes = optimizerConfig.getMaxPrefetchedInformationSchemaPrefixes();
        this.aiModelAccessControl = requireNonNull(aiModelAccessControl, "aiModelAccessControl is null");
        this.modelConnectionSpecsLoader = requireNonNull(modelConnectionSpecsLoader, "modelConnectionSpecsLoader is null");
        this.managedStatisticsClient = requireNonNull(managedStatisticsClient, "managedStatisticsClient is null");
        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
    }

    @Override
    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(
                new ConnectorName(connectorFactory.getName()), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector '%s' is already registered", connectorFactory.getName());
    }

    @Override
    public CatalogConnector createCatalog(CatalogProperties catalogProperties)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");

        ConnectorFactory connectorFactory = connectorFactories.get(catalogProperties.connectorName());
        checkArgument(connectorFactory != null, "No factory for connector '%s'. Available factories: %s", catalogProperties.connectorName(), connectorFactories.keySet());

        CatalogHandle catalogHandle = createRootCatalogHandle(catalogProperties.name(), catalogProperties.version());
        Connector connector = createConnector(
                catalogProperties.name(),
                catalogProperties.version(),
                connectorFactory,
                secretsResolver.getResolvedConfiguration(catalogProperties.properties()));

        return createCatalog(
                catalogHandle,
                catalogProperties.connectorName(),
                connector,
                Optional.of(catalogProperties));
    }

    @Override
    public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
    {
        return createCatalog(catalogHandle, connectorName, connector, Optional.empty());
    }

    private CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector, Optional<CatalogProperties> catalogProperties)
    {
        Tracer tracer = createTracer(catalogHandle.getCatalogName());

        ConnectorServices catalogConnector = new ConnectorServices(tracer, catalogHandle, connector);

        ConnectorServices informationSchemaConnector = new ConnectorServices(
                tracer,
                createInformationSchemaCatalogHandle(catalogHandle),
                new InformationSchemaConnector(
                        catalogHandle.getCatalogName().toString(),
                        currentNode,
                        metadata,
                        accessControl,
                        maxPrefetchedInformationSchemaPrefixes));

        SystemTablesProvider systemTablesProvider = new SystemTablesProvider(
                transactionManager,
                metadata,
                catalogHandle.getCatalogName().toString(),
                catalogConnector.getSystemTables());

        ConnectorServices systemConnector = new ConnectorServices(
                tracer,
                createSystemTablesCatalogHandle(catalogHandle),
                new SystemConnector(
                        currentNode,
                        nodeManager,
                        systemTablesProvider,
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalogHandle),
                        accessControl,
                        catalogHandle.getCatalogName().toString(),
                        catalogConnector.getPageSourceProviderFactory()));

        return new CatalogConnector(
                catalogHandle,
                connectorName,
                catalogConnector,
                informationSchemaConnector,
                systemConnector,
                localMemoryManager,
                catalogProperties);
    }

    private Connector createConnector(CatalogName catalogName, CatalogVersion catalogVersion, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                openTelemetry,
                createTracer(catalogName),
                new DefaultNodeManager(currentNode, nodeManager, schedulerIncludeCoordinator),
                versionEmbedder,
                typeManager,
                new InternalMetadataProvider(metadata, typeManager),
                aiModelAccessControl,
                modelConnectionSpecsLoader,
                pageSorter,
                pageIndexerFactory,
                pageStreamFactory,
                catalogVersion,
                nodeInfo.getEnvironment(),
                new InternalFunctionBundleFactory(),
                managedStatisticsClient);

        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            // TODO: connector factory should take CatalogName
            return connectorFactory.create(catalogName.toString(), properties, context);
        }
    }

    private Tracer createTracer(CatalogName catalogName)
    {
        return openTelemetry.getTracer("trino.catalog." + catalogName);
    }
}
