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

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Map;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class GcsVendedCredentials
        extends BaseVendedFileSystemCredentials
{
    private final String gcsOauthToken;

    public GcsVendedCredentials(String gcsOauthToken, Instant expiredAt)
    {
        super(expiredAt);
        this.gcsOauthToken = requireNonNull(gcsOauthToken, "gcsOauthToken is null");
    }

    @Override
    public Map<String, String> asExtraCredentials()
    {
        return ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, gcsOauthToken)
                .put(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, Long.toString(expireAt().toEpochMilli()))
                .buildOrThrow();
    }
}
