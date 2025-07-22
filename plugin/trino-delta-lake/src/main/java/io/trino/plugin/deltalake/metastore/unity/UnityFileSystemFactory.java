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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.time.Instant;
import java.util.Map;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_OAUTH_TOKEN_EXPIRE_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_GCS_OAUTH_TOKEN;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_ACCESS_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SECRET_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SESSION_TOKEN;
import static java.util.Objects.requireNonNull;

public class UnityFileSystemFactory
        implements DeltaLakeFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final VendedCredentialsProvider vendedCredentialsProvider;

    @Inject
    public UnityFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, VendedCredentialsProvider vendedCredentialsProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.vendedCredentialsProvider = requireNonNull(vendedCredentialsProvider, "vendedCredentialsProvider is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, VendedCredentialsHandle vendedCredentialsHandle)
    {
        requireNonNull(vendedCredentialsHandle, "vendedCredentialsHandle is null");

        vendedCredentialsHandle = vendedCredentialsProvider.getFreshCredentials(vendedCredentialsHandle);
        Map<String, String> credentials = vendedCredentialsHandle.vendedCredentials().credentials();

        ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();
        if (credentials.containsKey(VENDED_S3_ACCESS_KEY)) {
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, credentials.get(VENDED_S3_ACCESS_KEY))
                    .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, credentials.get(VENDED_S3_SECRET_KEY))
                    .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, credentials.get(VENDED_S3_SESSION_TOKEN));
        }

        if (credentials.containsKey(VENDED_GCS_OAUTH_TOKEN)) {
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY, credentials.get(VENDED_GCS_OAUTH_TOKEN));
            Instant expireAt = vendedCredentialsHandle.vendedCredentials().expireAt();
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_OAUTH_TOKEN_EXPIRE_AT_PROPERTY, String.valueOf(expireAt.toEpochMilli()));
        }

        ConnectorIdentity identity = session.getIdentity();
        ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withConnectorRole(identity.getConnectorRole())
                .withExtraCredentials(extraCredentialsBuilder.buildOrThrow())
                .build();
        return fileSystemFactory.create(identityWithExtraCredentials);
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, String tableLocation)
    {
        return create(session, VendedCredentialsHandle.empty(tableLocation));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return fileSystemFactory.create(identity);
    }
}
