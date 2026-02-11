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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metastore.cache.ReentrantBoundedExecutor;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingTrinoGlueClient
        implements TrinoGlueClient
{
    private final LoadingCache<SchemaTableName, Table> tableCache;
    private final Optional<ExecutorService> refreshExecutor;
    private final TrinoGlueClient delegate;

    public CachingTrinoGlueClient(CatalogName catalogName, IcebergGlueCatalogConfig config, TrinoGlueClient delegate)
    {
        requireNonNull(catalogName, "catalogName is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getMetastoreCacheMaximumSize())
                .expireAfterWrite(config.getMetastoreCacheTtl().toMillis(), MILLISECONDS);

        CacheLoader<SchemaTableName, Table> cacheLoader = CacheLoader.from(delegate::getTable);

        Optional<Duration> refreshInterval = config.getMetastoreCacheRefreshInterval();
        if (refreshInterval.isPresent()) {
            this.refreshExecutor = Optional.of(newCachedThreadPool(daemonThreadsNamed("glue-cache-" + catalogName + "-%s")));
            cacheBuilder.refreshAfterWrite(refreshInterval.get().toMillis(), MILLISECONDS);
            this.tableCache = cacheBuilder.build(asyncReloading(cacheLoader, new ReentrantBoundedExecutor(refreshExecutor.get(), config.getMetastoreCacheMaxRefreshThreads())));
        }
        else {
            this.refreshExecutor = Optional.empty();
            this.tableCache = cacheBuilder.build(cacheLoader);
        }
    }

    @Override
    public Database getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> listDatabases()
    {
        return delegate.listDatabases();
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        try {
            delegate.dropDatabase(databaseName);
        }
        finally {
            tableCache.invalidateAll();
        }
    }

    @Override
    public void createDatabase(DatabaseInput database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public Table getTable(SchemaTableName tableName)
    {
        try {
            return tableCache.getUnchecked(tableName);
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void deleteTable(String databaseName, String tableName)
    {
        try {
            delegate.deleteTable(databaseName, tableName);
        }
        finally {
            tableCache.invalidate(new SchemaTableName(databaseName, tableName));
        }
    }

    @Override
    public void updateTable(String databaseName, TableInput table, Optional<String> versionId)
    {
        try {
            delegate.updateTable(databaseName, table, versionId);
        }
        finally {
            tableCache.invalidate(new SchemaTableName(databaseName, table.name()));
        }
    }

    @Override
    public void createTable(String databaseName, TableInput table)
    {
        try {
            delegate.createTable(databaseName, table);
        }
        finally {
            tableCache.invalidate(new SchemaTableName(databaseName, table.name()));
        }
    }

    @Override
    public Stream<Table> streamTables(String databaseName)
    {
        return delegate.streamTables(databaseName);
    }

    @Override
    public void invalidateCache()
    {
        try {
            delegate.invalidateCache();
        }
        finally {
            tableCache.invalidateAll();
        }
    }

    @Override
    public void invalidateCache(SchemaTableName tableName)
    {
        try {
            delegate.invalidateCache(tableName);
        }
        finally {
            tableCache.invalidate(tableName);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            delegate.close();
        }
        finally {
            refreshExecutor.ifPresent(ExecutorService::shutdownNow);
        }
    }
}
