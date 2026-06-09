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

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public record AwsVendedCredentials(
        @JsonProperty("s3AccessKey") String s3AccessKey,
        @JsonProperty("s3SecretKey") String s3SecretKey,
        @JsonProperty("s3SessionToken") String s3SessionToken,
        @JsonProperty("expireAt") Instant expireAt)
        implements FileSystemCredentials
{
    // TODO: make the time configurable
    private static final int VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS = 120;

    public AwsVendedCredentials
    {
        requireNonNull(s3AccessKey, "s3AccessKey is null");
        requireNonNull(s3SecretKey, "s3SecretKey is null");
        requireNonNull(s3SessionToken, "s3SessionToken is null");
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
        return ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, s3AccessKey)
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, s3SecretKey)
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, s3SessionToken)
                .buildOrThrow();
    }
}
