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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.FileSystemReadExecutor;
import io.trino.spi.connector.ManagedStatisticsClient;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import io.trino.spi.function.FunctionBundleFactory;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class ConnectorContextInstance
        implements ConnectorContext
{
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final NodeManager nodeManager;
    private final VersionEmbedder versionEmbedder;
    private final TypeManager typeManager;
    private final MetadataProvider metadataProvider;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final PageStreamFactory pageStreamFactory;
    private final AiModelAccessControl aiModelAccessControl;
    private final ModelConnectionSpecsLoader modelConnectionSpecsLoader;
    private final CatalogVersion catalogVersion;
    private final String nodeEnvironment;
    private final FunctionBundleFactory functionBundleFactory;
    private final ManagedStatisticsClient managedStatisticsClient;
    private final FileSystemReadExecutor fileSystemReadExecutor;

    public ConnectorContextInstance(
            OpenTelemetry openTelemetry,
            Tracer tracer,
            NodeManager nodeManager,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            AiModelAccessControl aiModelAccessControl,
            ModelConnectionSpecsLoader modelConnectionSpecsLoader,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            PageStreamFactory pageStreamFactory,
            CatalogVersion catalogVersion,
            String nodeEnvironment,
            FunctionBundleFactory functionBundleFactory,
            ManagedStatisticsClient managedStatisticsClient,
            FileSystemReadExecutor fileSystemReadExecutor)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.aiModelAccessControl = requireNonNull(aiModelAccessControl, "aiModelAccessControl is null");
        this.modelConnectionSpecsLoader = requireNonNull(modelConnectionSpecsLoader, "modelConnectionSpecsLoader is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.pageStreamFactory = requireNonNull(pageStreamFactory, "pageStreamFactory is null");
        this.catalogVersion = requireNonNull(catalogVersion, "catalogVersion is null");
        this.nodeEnvironment = requireNonNull(nodeEnvironment, "nodeEnvironment is null");
        this.functionBundleFactory = requireNonNull(functionBundleFactory, "functionBundleFactory is null");
        this.managedStatisticsClient = requireNonNull(managedStatisticsClient, "managedStatisticsClient is null");
        this.fileSystemReadExecutor = requireNonNull(fileSystemReadExecutor, "fileSystemReadExecutor is null");
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return openTelemetry;
    }

    @Override
    public Tracer getTracer()
    {
        return tracer;
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public VersionEmbedder getVersionEmbedder()
    {
        return versionEmbedder;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public MetadataProvider getMetadataProvider()
    {
        return metadataProvider;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public PageStreamFactory getPageStreamFactory()
    {
        return pageStreamFactory;
    }

    @Override
    public AiModelAccessControl getAiModelAccessControl()
    {
        return aiModelAccessControl;
    }

    @Override
    public ModelConnectionSpecsLoader getModelConnectionSpecsLoader()
    {
        return modelConnectionSpecsLoader;
    }

    @Override
    public CatalogVersion getCatalogVersion()
    {
        return catalogVersion;
    }

    @Override
    public String getNodeEnvironment()
    {
        return nodeEnvironment;
    }

    @Override
    public FunctionBundleFactory getFunctionBundleFactory()
    {
        return functionBundleFactory;
    }

    @Override
    public ManagedStatisticsClient getManagedStatisticsClient()
    {
        return managedStatisticsClient;
    }

    @Override
    public FileSystemReadExecutor getFileSystemReadExecutor()
    {
        return fileSystemReadExecutor;
    }
}
