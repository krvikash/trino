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

import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.DeltaLakeTableCredentialsProvider;
import io.trino.plugin.deltalake.metastore.AwsVendedCredentials;
import io.trino.plugin.deltalake.metastore.AzureVendedCredentials;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.GcsVendedCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.net.URI;
import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.unitycatalog.client.delta.model.DeltaCredentialOperation.READ;
import static io.unitycatalog.client.delta.model.DeltaCredentialOperation.READ_WRITE;
import static io.unitycatalog.client.model.PathOperation.PATH_READ_WRITE;
import static java.lang.String.format;

abstract class AbstractUnityDeltaLakeTableCredentialsProvider
        implements DeltaLakeTableCredentialsProvider
{
    @Override
    public Optional<DeltaLakeTableCredentials> getTableCredentials(ConnectorIdentity identity, VendedCredentialsHandle handle)
    {
        UnityMetastore unityMetastore = getUnityMetastore(identity);

        FileSystemCredentials credentials;
        // TODO: migrate external tables off the legacy credential API when they are supported by DeltaTemporaryCredentialsApi
        if (handle.materializedView()) {
            credentials = fromTemporaryCredentials(unityMetastore.getTemporaryTableCredentials(handle.tableId().orElseThrow(), TableOperation.READ), handle.tableLocation());
        }
        else if (handle.catalogManaged()) {
            credentials = fromDeltaCredentials(unityMetastore.getTemporaryTableCredentials(handle.schemaTableName().orElseThrow(), READ_WRITE), handle.tableLocation());
        }
        else if (handle.managed()) {
            credentials = fromDeltaCredentials(unityMetastore.getTemporaryTableCredentials(handle.schemaTableName().orElseThrow(), READ), handle.tableLocation());
        }
        else {
            // TODO: migrate external tables off the legacy credential API when they are supported by DeltaTemporaryCredentialsApi
            credentials = fromTemporaryCredentials(unityMetastore.getTemporaryPathCredentials(handle.tableLocation(), PATH_READ_WRITE), handle.tableLocation());
        }

        verify(credentials.isValid(), "vended credentials is not valid");
        return Optional.of(new DeltaLakeTableCredentials(handle, credentials));
    }

    protected abstract UnityMetastore getUnityMetastore(ConnectorIdentity identity);

    private static FileSystemCredentials fromDeltaCredentials(DeltaCredentialsResponse response, String tableLocation)
    {
        if (response.getStorageCredentials().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "No credentials returned from Unity Catalog for " + tableLocation);
        }

        DeltaStorageCredential credential = response.getStorageCredentials().stream()
                .filter(cred -> tableLocation.startsWith(cred.getPrefix()))
                .max(Comparator.comparingInt(cred -> cred.getPrefix().length()))
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "No matching credential prefix returned from Unity Catalog for " + tableLocation));
        Instant expireAt = Instant.ofEpochMilli(credential.getExpirationTimeMs());
        DeltaStorageCredentialConfig config = credential.getConfig();

        if (config.getAzureSasToken() != null) {
            return new AzureVendedCredentials(
                    config.getAzureSasToken(),
                    storageAccountFromLocation(tableLocation),
                    expireAt);
        }

        if (config.getS3AccessKeyId() != null) {
            return new AwsVendedCredentials(
                    config.getS3AccessKeyId(),
                    config.getS3SecretAccessKey(),
                    config.getS3SessionToken(),
                    expireAt);
        }

        if (config.getGcsOauthToken() != null) {
            return new GcsVendedCredentials(
                    config.getGcsOauthToken(),
                    expireAt);
        }

        throw new TrinoException(NOT_SUPPORTED, "No supported cloud credentials returned from Unity Catalog");
    }

    private static FileSystemCredentials fromTemporaryCredentials(TemporaryCredentials credentials, String tableLocation)
    {
        Instant expireAt = Optional.ofNullable(credentials.getExpirationTime()).map(Instant::ofEpochMilli).orElse(Instant.MAX);

        AzureUserDelegationSAS azureUserDelegationSas = credentials.getAzureUserDelegationSas();
        if (azureUserDelegationSas != null) {
            return new AzureVendedCredentials(
                    azureUserDelegationSas.getSasToken(),
                    storageAccountFromLocation(tableLocation),
                    expireAt);
        }

        AwsCredentials awsTempCredentials = credentials.getAwsTempCredentials();
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

    private static String storageAccountFromLocation(String tableLocation)
    {
        URI uri = URI.create(tableLocation);
        String host = uri.getHost();
        if (host == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Cannot extract storage account from Azure location: %s", tableLocation));
        }
        int dotIndex = host.indexOf('.');
        if (dotIndex <= 0) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Cannot extract storage account from Azure location: %s", tableLocation));
        }
        return host.substring(0, dotIndex);
    }
}
