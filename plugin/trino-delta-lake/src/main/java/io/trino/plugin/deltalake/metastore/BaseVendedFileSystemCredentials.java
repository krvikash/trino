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
package io.trino.plugin.deltalake.metastore;

import io.trino.spi.TrinoException;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.time.Instant;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public abstract class BaseVendedFileSystemCredentials
        implements FileSystemCredentials
{
    private static final int VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS = 120;

    private final Instant expireAt;

    public BaseVendedFileSystemCredentials(Instant expireAt)
    {
        this.expireAt = requireNonNull(expireAt, "expireAt is null");
    }

    @Override
    public boolean isValid()
    {
        // If the token expires after 2 mins, don't use it
        // TODO: make the time configurable
        return Instant.now().isBefore(expireAt().minusSeconds(VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS));
    }

    public Instant expireAt()
    {
        return expireAt;
    }

    public static FileSystemCredentials fromTemporaryCredentials(TemporaryCredentials credentials)
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
