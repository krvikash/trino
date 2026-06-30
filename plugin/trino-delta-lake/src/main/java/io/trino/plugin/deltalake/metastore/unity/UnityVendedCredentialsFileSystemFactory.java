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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.DeltaLakeTableCredentialsProvider;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class UnityVendedCredentialsFileSystemFactory
        implements DeltaLakeFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeTableCredentialsProvider tableCredentialsProvider;
    private final Cache<CacheKey, DeltaLakeTableCredentials> credentialsCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(1_000)
            .expireAfterWrite(1, HOURS)
            .build();

    @Inject
    public UnityVendedCredentialsFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, DeltaLakeTableCredentialsProvider tableCredentialsProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableCredentialsProvider = requireNonNull(tableCredentialsProvider, "tableCredentialsProvider is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        requireNonNull(tableCredentials, "tableCredentials is null");
        verify(tableCredentials.isPresent(), "tableCredentials is missing");

        ConnectorIdentity identity = session.getIdentity();
        return new UnityVendedCredentialsFileSystem(() -> {
            DeltaLakeTableCredentials credentials = tableCredentials.get();
            FileSystemCredentials fileSystemCredentials = credentials.fileSystemCredentials();
            if (!fileSystemCredentials.isValid()) {
                DeltaLakeTableCredentials deltaLakeTableCredentials = getTableCredentials(session, credentials.vendedCredentialsHandle());
                fileSystemCredentials = deltaLakeTableCredentials.fileSystemCredentials();
            }
            return fileSystemFactory.create(createIdentityWithCredentials(identity, fileSystemCredentials));
        });
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, String tableLocation)
    {
        DeltaLakeTableCredentials tableCredentials = getTableCredentials(session, VendedCredentialsHandle.empty(tableLocation));
        return create(session, Optional.of(tableCredentials));
    }

    private DeltaLakeTableCredentials getTableCredentials(ConnectorSession session, VendedCredentialsHandle handle)
    {
        CacheKey cacheKey = new CacheKey(session.getQueryId(), handle);
        DeltaLakeTableCredentials cached = credentialsCache.getIfPresent(cacheKey);
        if (cached != null && !cached.fileSystemCredentials().isValid()) {
            credentialsCache.invalidate(cacheKey);
        }
        return uncheckedCacheGet(credentialsCache, cacheKey, () -> tableCredentialsProvider.getTableCredentials(session.getIdentity(), cacheKey.vendedCredentialsHandle()).orElseThrow());
    }

    private static ConnectorIdentity createIdentityWithCredentials(ConnectorIdentity identity, FileSystemCredentials fileSystemCredentials)
    {
        return ConnectorIdentity.forUser(identity.getUser())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withConnectorRole(identity.getConnectorRole())
                .withExtraCredentials(fileSystemCredentials.asExtraCredentials())
                .build();
    }

    private record CacheKey(String queryId, VendedCredentialsHandle vendedCredentialsHandle)
    {
        CacheKey
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(vendedCredentialsHandle, "vendedCredentialsHandle is null");
        }
    }
}
