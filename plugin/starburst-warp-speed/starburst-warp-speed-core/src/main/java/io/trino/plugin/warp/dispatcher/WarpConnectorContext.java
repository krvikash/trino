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
package io.trino.plugin.warp.dispatcher;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ManagedStatisticsClient;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import io.trino.spi.function.FunctionBundleFactory;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class WarpConnectorContext
        implements ConnectorContext, WarpContext
{
    private final ConnectorContext connectorContext;
    private final WarpPluginSharedInstances sharedInstances;

    public WarpConnectorContext(ConnectorContext connectorContext, WarpPluginSharedInstances sharedInstances)
    {
        this.connectorContext = requireNonNull(connectorContext);
        this.sharedInstances = requireNonNull(sharedInstances);
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return connectorContext.getOpenTelemetry();
    }

    @Override
    public Tracer getTracer()
    {
        return connectorContext.getTracer();
    }

    @Override
    public Node getCurrentNode()
    {
        return connectorContext.getCurrentNode();
    }

    @Override
    public NodeManager getNodeManager()
    {
        return connectorContext.getNodeManager();
    }

    @Override
    public VersionEmbedder getVersionEmbedder()
    {
        return connectorContext.getVersionEmbedder();
    }

    @Override
    public String getSpiVersion()
    {
        return connectorContext.getSpiVersion();
    }

    @Override
    public TypeManager getTypeManager()
    {
        return connectorContext.getTypeManager();
    }

    @Override
    public MetadataProvider getMetadataProvider()
    {
        return connectorContext.getMetadataProvider();
    }

    @Override
    public PageSorter getPageSorter()
    {
        return connectorContext.getPageSorter();
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return connectorContext.getPageIndexerFactory();
    }

    @Override
    public PageStreamFactory getPageStreamFactory()
    {
        return connectorContext.getPageStreamFactory();
    }

    @Override
    public WarpPluginSharedInstances getWarpPluginSharedInstances()
    {
        return sharedInstances;
    }

    @Override
    public AiModelAccessControl getAiModelAccessControl()
    {
        return connectorContext.getAiModelAccessControl();
    }

    @Override
    public ModelConnectionSpecsLoader getModelConnectionSpecsLoader()
    {
        return connectorContext.getModelConnectionSpecsLoader();
    }

    @Override
    public CatalogVersion getCatalogVersion()
    {
        return connectorContext.getCatalogVersion();
    }

    @Override
    public String getNodeEnvironment()
    {
        return connectorContext.getNodeEnvironment();
    }

    @Override
    public FunctionBundleFactory getFunctionBundleFactory()
    {
        return connectorContext.getFunctionBundleFactory();
    }

    @Override
    public ManagedStatisticsClient getManagedStatisticsClient()
    {
        return connectorContext.getManagedStatisticsClient();
    }

    @Override
    public BlocksHashFactory getBlocksHashFactory()
    {
        return connectorContext.getBlocksHashFactory();
    }
}
