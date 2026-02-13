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
package io.trino.plugin.iceberg.catalog;

import com.google.inject.Inject;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.metastore.cache.SharedHiveMetastoreCache.ImpersonationCachingHiveMetastoreFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergHiveMetastoreCacheInvalidator
        implements MetastoreCacheInvalidator
{
    private final HiveMetastoreFactory hiveMetadataFactory;
    private final Optional<CachingHiveMetastore> cachingHiveMetastore;

    @Inject
    public IcebergHiveMetastoreCacheInvalidator(Optional<CachingHiveMetastore> cachingHiveMetastore, HiveMetastoreFactory hiveMetadataFactory)
    {
        this.cachingHiveMetastore = requireNonNull(cachingHiveMetastore, "cachingHiveMetastore is null");
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
    }

    @Override
    public void invalidateCache(ConnectorSession session)
    {
        cachingHiveMetastore.ifPresent(CachingHiveMetastore::flushCache);
        getImpersonationCachingHiveMetastore(session).ifPresent(CachingHiveMetastore::flushCache);
    }

    @Override
    public void invalidateCache(ConnectorSession session, SchemaTableName tableName)
    {
        cachingHiveMetastore.ifPresent(cache -> cache.invalidateTable(tableName.getSchemaName(), tableName.getTableName()));
        getImpersonationCachingHiveMetastore(session).ifPresent(cache -> cache.invalidateTable(tableName.getSchemaName(), tableName.getTableName()));
    }

    private Optional<CachingHiveMetastore> getImpersonationCachingHiveMetastore(ConnectorSession session)
    {
        if (hiveMetadataFactory instanceof ImpersonationCachingHiveMetastoreFactory impersonationCachingHiveMetastoreFactory) {
            return Optional.of((CachingHiveMetastore) impersonationCachingHiveMetastoreFactory.createMetastore(Optional.of(session.getIdentity())));
        }
        return Optional.empty();
    }
}
