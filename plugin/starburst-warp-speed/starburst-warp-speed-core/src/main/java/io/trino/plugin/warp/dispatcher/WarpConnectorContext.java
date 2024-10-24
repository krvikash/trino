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
import io.trino.plugin.warp.dispatcher.warmup.demoter.DemoterSync;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class WarpConnectorContext
        implements ConnectorContext, WarpContext
{
    private final ConnectorContext connectorContext;
    private final DemoterSync demoterSync;

    public WarpConnectorContext(ConnectorContext connectorContext, DemoterSync demoterSync)
    {
        this.connectorContext = requireNonNull(connectorContext);
        this.demoterSync = requireNonNull(demoterSync);
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return connectorContext.getCatalogHandle();
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
    public DemoterSync getDemoterSync()
    {
        return demoterSync;
    }
}
