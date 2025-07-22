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
import io.trino.plugin.deltalake.metastore.VendedCredentials;
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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_GCS_OAUTH_TOKEN;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_ACCESS_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SECRET_KEY;
import static io.trino.plugin.hive.metastore.unity.UnityHiveMetastore.VENDED_S3_SESSION_TOKEN;
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
        VendedCredentials vendedCredentials = handle.vendedCredentials();
        if (vendedCredentials.isFresh()) {
            return handle;
        }

        Optional<String> tableId = vendedCredentials.tableId();
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
            credentialsBuilder.put(VENDED_S3_ACCESS_KEY, awsTempCredentials.getAccessKeyId());
            credentialsBuilder.put(VENDED_S3_SECRET_KEY, awsTempCredentials.getSecretAccessKey());
            credentialsBuilder.put(VENDED_S3_SESSION_TOKEN, awsTempCredentials.getSessionToken());
        }

        AzureUserDelegationSAS azureUserDelegationSas = temporaryCredentials.getAzureUserDelegationSas();
        if (azureUserDelegationSas != null) {
            // TODO: support Azure credentials vending in Unity catalog
            throw new UnsupportedOperationException("Azure User Delegation SAS is not supported yet in Unity vended credentials");
        }

        GcpOauthToken gcpOauthToken = temporaryCredentials.getGcpOauthToken();
        if (gcpOauthToken != null) {
            credentialsBuilder.put(VENDED_GCS_OAUTH_TOKEN, gcpOauthToken.getOauthToken());
        }

        verify(awsTempCredentials != null || gcpOauthToken != null, "No supported cloud credentials returned from Unity Catalog");

        VendedCredentials freshCredentials = new VendedCredentials(tableId, expireAt, credentialsBuilder.buildOrThrow());
        checkArgument(freshCredentials.isFresh(), "Unexpected stale credentials: %s", freshCredentials);
        return handle.withVendedCredentials(freshCredentials);
    }
}
