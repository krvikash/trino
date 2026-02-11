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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.inject.Inject;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.Node;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;

public class DefaultGlueClientProvider
        implements GlueClientProvider
{
    private final TrinoGlueClient glueClient;

    @Inject
    public DefaultGlueClientProvider(GlueClient glueClient, IcebergGlueCatalogConfig config, Node currentNode, CatalogName catalogName, GlueMetastoreStats stats)
    {
        TrinoGlueClient trinoGlueClient = new StatsRecordingGlueClient(glueClient, stats);

        // The Glue client is created on workers but never used there. We skip creating the caching
        // layer on workers to avoid allocating resources (e.g., cache refresh executor). Additionally,
        // if the cache were used on workers, there would be no way to invalidate it.
        boolean cacheEnabled = currentNode.isCoordinator() && !config.getMetastoreCacheTtl().isZero();
        if (cacheEnabled) {
            trinoGlueClient = new CachingTrinoGlueClient(catalogName, config, trinoGlueClient);
        }

        this.glueClient = trinoGlueClient;
    }

    @Override
    public TrinoGlueClient get(ConnectorIdentity connectorIdentity)
    {
        return glueClient;
    }

    @Override
    public void invalidateCache()
    {
        glueClient.invalidateCache();
    }

    @Override
    public void invalidateCache(SchemaTableName tableName)
    {
        glueClient.invalidateCache(tableName);
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        glueClient.close();
    }
}
