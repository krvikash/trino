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
package io.trino.plugin.objectstore;

import io.trino.plugin.deltalake.DeltaLakeCacheMetadata;
import io.trino.plugin.hive.HiveCacheMetadata;
import io.trino.plugin.iceberg.IcebergCacheMetadata;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

import static io.trino.plugin.objectstore.ObjectStoreMetadata.tableType;
import static java.util.Objects.requireNonNull;

public class ObjectStoreCacheMetadata
        implements ConnectorCacheMetadata
{
    private final HiveCacheMetadata hiveMetadata;
    private final IcebergCacheMetadata icebergMetadata;
    private final DeltaLakeCacheMetadata deltaMetadata;

    public ObjectStoreCacheMetadata(
            ConnectorCacheMetadata hiveMetadata,
            ConnectorCacheMetadata icebergMetadata,
            ConnectorCacheMetadata deltaMetadata)
    {
        this.hiveMetadata = (HiveCacheMetadata) requireNonNull(hiveMetadata, "hiveMetadata is null");
        this.icebergMetadata = (IcebergCacheMetadata) requireNonNull(icebergMetadata, "icebergMetadata is null");
        this.deltaMetadata = (DeltaLakeCacheMetadata) requireNonNull(deltaMetadata, "deltaMetadata is null");
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        return delegate(tableType(tableHandle)).flatMap(delegate -> delegate.getCacheTableId(tableHandle));
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate(tableType(tableHandle)).flatMap(delegate -> delegate.getCacheColumnId(tableHandle, columnHandle));
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle tableHandle)
    {
        return delegate(tableType(tableHandle)).map(delegate -> delegate.getCanonicalTableHandle(tableHandle)).orElse(tableHandle);
    }

    Optional<ConnectorCacheMetadata> delegate(TableType tableType)
    {
        return switch (tableType) {
            case HIVE -> Optional.of(hiveMetadata);
            case ICEBERG -> Optional.of(icebergMetadata);
            case DELTA -> Optional.of(deltaMetadata);
            case HUDI -> Optional.empty();
        };
    }
}
