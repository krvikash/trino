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
package io.trino.testing;

import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.FeaturesConfig;
import io.trino.connector.ThrowingManagedStatisticsClient;
import io.trino.execution.buffer.PagesSerdeStreamFactory;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundleFactory;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.NodeManager;
import io.trino.spi.NodeVersion;
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
import io.trino.spi.type.TypeOperators;
import io.trino.util.EmbedVersion;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.connector.ai.ModelConnectionSpecsLoader.EMPTY_LOADER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;

public final class TestingConnectorContext
        implements ConnectorContext
{
    private static final TestingConnectorContext DEFAULT_CONTEXT = builder().build();

    public static Builder builder()
    {
        return new Builder();
    }

    private final NodeManager nodeManager;
    private final VersionEmbedder versionEmbedder;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;

    public TestingConnectorContext()
    {
        this(DEFAULT_CONTEXT.getNodeManager(),
                DEFAULT_CONTEXT.getVersionEmbedder(),
                DEFAULT_CONTEXT.getTypeManager(),
                DEFAULT_CONTEXT.getPageSorter(),
                DEFAULT_CONTEXT.getPageIndexerFactory());
    }

    private TestingConnectorContext(NodeManager nodeManager, VersionEmbedder versionEmbedder, TypeManager typeManager, PageSorter pageSorter, PageIndexerFactory pageIndexerFactory)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return OpenTelemetry.noop();
    }

    @Override
    public Tracer getTracer()
    {
        return Tracing.noopTracer();
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
        return NOOP_METADATA_PROVIDER;
    }

    @Override
    public AiModelAccessControl getAiModelAccessControl()
    {
        return AiModelAccessControl.ALLOW_ALL;
    }

    @Override
    public ModelConnectionSpecsLoader getModelConnectionSpecsLoader()
    {
        return EMPTY_LOADER;
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
        return new PagesSerdeStreamFactory(new InternalBlockEncodingSerde(new BlockEncodingManager(new FeaturesConfig(), new BlockEncodingSimdSupport(true)), TESTING_TYPE_MANAGER));
    }

    @Override
    public CatalogVersion getCatalogVersion()
    {
        return new CatalogVersion("default");
    }

    @Override
    public ManagedStatisticsClient getManagedStatisticsClient()
    {
        return new ThrowingManagedStatisticsClient();
    }

    @Override
    public FileSystemReadExecutor getFileSystemReadExecutor()
    {
        return new FileSystemReadExecutor()
        {
            @Override
            public <T> CompletableFuture<T> submit(Callable<T> task)
            {
                CompletableFuture<T> future = new CompletableFuture<>();
                try {
                    future.complete(task.call());
                }
                catch (Throwable t) {
                    future.completeExceptionally(t);
                }
                return future;
            }
        };
    }

    public static final class Builder
    {
        private NodeManager nodeManager = TestingNodeManager.create();
        private VersionEmbedder versionEmbedder = new EmbedVersion(NodeVersion.UNKNOWN);
        private TypeManager typeManager = TESTING_TYPE_MANAGER;
        private PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        private PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators())));

        private Builder() {}

        public Builder withNodeManager(NodeManager nodeManager)
        {
            this.nodeManager = nodeManager;
            return this;
        }

        public Builder withVersionEmbedder(VersionEmbedder versionEmbedder)
        {
            this.versionEmbedder = versionEmbedder;
            return this;
        }

        public Builder withTypeManager(TypeManager typeManager)
        {
            this.typeManager = typeManager;
            return this;
        }

        public Builder withPageSorter(PageSorter pageSorter)
        {
            this.pageSorter = pageSorter;
            return this;
        }

        public Builder withPageIndexerFactory(PageIndexerFactory pageIndexerFactory)
        {
            this.pageIndexerFactory = pageIndexerFactory;
            return this;
        }

        public TestingConnectorContext build()
        {
            return new TestingConnectorContext(
                    nodeManager,
                    versionEmbedder,
                    typeManager,
                    pageSorter,
                    pageIndexerFactory);
        }
    }

    @Override
    public FunctionBundleFactory getFunctionBundleFactory()
    {
        return new InternalFunctionBundleFactory();
    }
}
