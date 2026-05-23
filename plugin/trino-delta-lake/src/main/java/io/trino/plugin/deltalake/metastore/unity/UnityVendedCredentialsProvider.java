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

import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.AwsVendedCredentials;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.GcsVendedCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.trino.spi.TrinoException;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.time.Instant;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
    public Optional<FileSystemCredentials> getVendedCredentials(VendedCredentialsHandle handle)
    {
        Optional<String> tableId = handle.tableId();
        TemporaryCredentials temporaryCredentials;
        if (handle.catalogManaged()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ_WRITE);
        }
        else if (handle.managed()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ);
        }
        else { // external table
            temporaryCredentials = unityMetastore.getTemporaryPathCredentials(handle.tableLocation(), PathOperation.PATH_READ_WRITE);
        }

        FileSystemCredentials credentials = fromTemporaryCredentials(temporaryCredentials);
        verify(credentials.isValid(), "vended credentials is not valid");
        return Optional.of(credentials);
    }

    private static FileSystemCredentials fromTemporaryCredentials(TemporaryCredentials credentials)
    {
        AzureUserDelegationSAS azureUserDelegationSas = credentials.getAzureUserDelegationSas();
        if (azureUserDelegationSas != null) {
            // TODO: support azure vended credentials https://starburstdata.atlassian.net/browse/SEP-18169
            throw new TrinoException(NOT_SUPPORTED, "Azure vended credentials are not supported yet");
        }

        AwsCredentials awsTempCredentials = credentials.getAwsTempCredentials();
        Instant expireAt = Optional.ofNullable(credentials.getExpirationTime()).map(Instant::ofEpochMilli).orElse(Instant.MAX);
        if (awsTempCredentials != null) {
            return new AwsVendedCredentials(
                    awsTempCredentials.getAccessKeyId(),
                    awsTempCredentials.getSecretAccessKey(),
                    awsTempCredentials.getSessionToken(),
                    expireAt);
        }

        GcpOauthToken gcpOauthToken = credentials.getGcpOauthToken();
        if (gcpOauthToken != null) {
            return new GcsVendedCredentials(
                    gcpOauthToken.getOauthToken(),
                    expireAt);
        }

        throw new TrinoException(NOT_SUPPORTED, "No supported cloud credentials returned from Unity Catalog");
    }
}
