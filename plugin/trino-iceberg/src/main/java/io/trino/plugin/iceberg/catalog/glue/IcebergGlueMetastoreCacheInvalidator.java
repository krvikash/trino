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
import io.trino.plugin.iceberg.catalog.MetastoreCacheInvalidator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class IcebergGlueMetastoreCacheInvalidator
        implements MetastoreCacheInvalidator
{
    private final GlueClientProvider glueClientProvider;

    @Inject
    public IcebergGlueMetastoreCacheInvalidator(GlueClientProvider glueClientProvider)
    {
        this.glueClientProvider = requireNonNull(glueClientProvider, "glueClientProvider is null");
    }

    @Override
    public void invalidateCache(ConnectorSession session)
    {
        glueClientProvider.invalidateCache();
    }

    @Override
    public void invalidateCache(ConnectorSession session, SchemaTableName tableName)
    {
        glueClientProvider.invalidateCache(tableName);
    }
}
