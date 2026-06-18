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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Map;

import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static java.util.Objects.requireNonNull;

public record AzureVendedCredentials(
        @JsonProperty("sasToken") String sasToken,
        @JsonProperty("storageAccount") String storageAccount,
        @JsonProperty("expireAt") Instant expireAt)
        implements FileSystemCredentials
{
    // TODO: make the time configurable
    private static final int VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS = 120;

    public AzureVendedCredentials
    {
        requireNonNull(sasToken, "sasToken is null");
        requireNonNull(storageAccount, "storageAccount is null");
        requireNonNull(expireAt, "expireAt is null");
    }

    @Override
    public boolean isValid()
    {
        return Instant.now().isBefore(expireAt.minusSeconds(VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS));
    }

    @Override
    public Map<String, String> asExtraCredentials()
    {
        return ImmutableMap.of(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + storageAccount, sasToken);
    }
}
