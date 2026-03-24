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
package io.trino.plugin.bigquery.dynamic;

import com.google.inject.Inject;
import io.trino.plugin.base.cache.identity.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class BigQueryDynamicConnectionBasedIdentityCacheMapping
        implements IdentityCacheMapping
{
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final MessageDigest sha256;
    private final String projectIdCredentialName;
    private final String parentProjectIdCredentialName;
    private final String credentialsKeyCredentialName;
    private final String viewMaterializationProjectCredentialName;
    private final String viewMaterializationDatasetCredentialName;

    @Inject
    public BigQueryDynamicConnectionBasedIdentityCacheMapping(BigQueryDynamicConnectionPassthroughConfig dynamicConnectionConfig)
    {
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        projectIdCredentialName = dynamicConnectionConfig.getProjectIdCredentialName();
        parentProjectIdCredentialName = dynamicConnectionConfig.getParentProjectIdCredentialName();
        credentialsKeyCredentialName = dynamicConnectionConfig.getCredentialsKeyCredentialName();
        viewMaterializationProjectCredentialName = dynamicConnectionConfig.getViewMaterializationProjectCredentialName();
        viewMaterializationDatasetCredentialName = dynamicConnectionConfig.getViewMaterializationDatasetCredentialName();
    }

    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        Map<String, String> extraCredentials = session.getIdentity().getExtraCredentials();
        return new ExtraCredentialsBasedIdentityCacheKey(
                Optional.ofNullable(extraCredentials.get(projectIdCredentialName))
                        .map(this::hash)
                        .orElse(EMPTY_BYTES),
                Optional.ofNullable(extraCredentials.get(parentProjectIdCredentialName))
                        .map(this::hash)
                        .orElse(EMPTY_BYTES),
                Optional.ofNullable(extraCredentials.get(credentialsKeyCredentialName))
                        .map(this::hash)
                        .orElse(EMPTY_BYTES),
                Optional.ofNullable(extraCredentials.get(viewMaterializationProjectCredentialName))
                        .map(this::hash)
                        .orElse(EMPTY_BYTES),
                Optional.ofNullable(extraCredentials.get(viewMaterializationDatasetCredentialName))
                        .map(this::hash)
                        .orElse(EMPTY_BYTES));
    }

    private byte[] hash(String value)
    {
        return sha256.digest(value.getBytes(UTF_8));
    }

    private static final class ExtraCredentialsBasedIdentityCacheKey
            extends IdentityCacheKey
    {
        private final byte[] projectIdHash;
        private final byte[] parentProjectIdHash;
        private final byte[] credentialsKeyHash;
        private final byte[] viewMaterializationProjectHash;
        private final byte[] viewMaterializationDatasetHash;

        public ExtraCredentialsBasedIdentityCacheKey(
                byte[] projectIdHash,
                byte[] parentProjectIdHash,
                byte[] credentialsKeyHash,
                byte[] viewMaterializationProjectHash,
                byte[] viewMaterializationDatasetHash)
        {
            this.projectIdHash = requireNonNull(projectIdHash, "projectIdHash is null");
            this.parentProjectIdHash = requireNonNull(parentProjectIdHash, "parentProjectIdHash is null");
            this.credentialsKeyHash = requireNonNull(credentialsKeyHash, "credentialsKeyHash is null");
            this.viewMaterializationProjectHash = requireNonNull(viewMaterializationProjectHash, "viewMaterializationProjectHash is null");
            this.viewMaterializationDatasetHash = requireNonNull(viewMaterializationDatasetHash, "viewMaterializationDatasetHash is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExtraCredentialsBasedIdentityCacheKey that = (ExtraCredentialsBasedIdentityCacheKey) o;
            return Arrays.equals(projectIdHash, that.projectIdHash)
                    && Arrays.equals(parentProjectIdHash, that.parentProjectIdHash)
                    && Arrays.equals(credentialsKeyHash, that.credentialsKeyHash)
                    && Arrays.equals(viewMaterializationProjectHash, that.viewMaterializationProjectHash)
                    && Arrays.equals(viewMaterializationDatasetHash, that.viewMaterializationDatasetHash);
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode(projectIdHash);
            result = 31 * result + Arrays.hashCode(parentProjectIdHash);
            result = 31 * result + Arrays.hashCode(credentialsKeyHash);
            result = 31 * result + Arrays.hashCode(viewMaterializationProjectHash);
            result = 31 * result + Arrays.hashCode(viewMaterializationDatasetHash);
            return result;
        }
    }
}
