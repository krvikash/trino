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
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_OAUTH_TOKEN_EXPIRE_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class UnityVendedCredentialsProvider
        implements VendedCredentialsProvider
{
    private final UnityMetastore unityMetastore;

    @Inject
    public UnityVendedCredentialsProvider(UnityHiveMetastoreFactory metastoreFactory)
    {
        this.unityMetastore = (UnityMetastore) requireNonNull(metastoreFactory, "metastoreFactory is null").createMetastore(Optional.empty());
    }

    @Override
    public VendedCredentialsHandle getFreshCredentials(VendedCredentialsHandle handle)
    {
        if (handle.vendedCredentials().map(FileSystemCredentials::isValid).orElse(false)) {
            return handle;
        }

        Optional<String> tableId = handle.tableId();
        TemporaryCredentials temporaryCredentials;
        if (handle.catalogOwned()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ_WRITE);
        }
        else if (handle.managed()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ);
        }
        else { // external table
            temporaryCredentials = unityMetastore.getTemporaryPathCredentials(handle.tableLocation(), PathOperation.PATH_READ_WRITE);
        }

        Instant expireAt = Instant.ofEpochMilli(temporaryCredentials.getExpirationTime());

        ImmutableMap.Builder<String, String> credentialsBuilder = ImmutableMap.builder();
        AwsCredentials awsTempCredentials = temporaryCredentials.getAwsTempCredentials();
        if (awsTempCredentials != null) {
            credentialsBuilder.put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, awsTempCredentials.getAccessKeyId());
            credentialsBuilder.put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, awsTempCredentials.getSecretAccessKey());
            credentialsBuilder.put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, awsTempCredentials.getSessionToken());
        }

        AzureUserDelegationSAS azureUserDelegationSas = temporaryCredentials.getAzureUserDelegationSas();
        if (azureUserDelegationSas != null) {
            // TODO: support Azure credentials vending in Unity catalog
            throw new UnsupportedOperationException("Azure User Delegation SAS is not supported yet in Unity vended credentials");
        }

        GcpOauthToken gcpOauthToken = temporaryCredentials.getGcpOauthToken();
        if (gcpOauthToken != null) {
            credentialsBuilder.put(EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY, gcpOauthToken.getOauthToken());
            credentialsBuilder.put(EXTRA_CREDENTIALS_OAUTH_TOKEN_EXPIRE_AT_PROPERTY, String.valueOf(expireAt.toEpochMilli()));
        }

        verify(awsTempCredentials != null || gcpOauthToken != null, "No supported cloud credentials returned from Unity Catalog");

        return handle.withVendedCredentials(new FileSystemCredentials() {
            @Override
            public Map<String, String> asExtraCredentials()
            {
                return credentialsBuilder.buildOrThrow();
            }

            @Override
            public boolean isValid()
            {
                return Instant.now().isBefore(expireAt);
            }
        });
    }
}
