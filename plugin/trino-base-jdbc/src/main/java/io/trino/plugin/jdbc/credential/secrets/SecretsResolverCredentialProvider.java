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
package io.trino.plugin.jdbc.credential.secrets;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SecretsResolverCredentialProvider
        implements CredentialProvider
{
    private static final Logger log = Logger.get(SecretsResolverCredentialProvider.class);

    private final SecretsResolver secretsResolver;
    private final String secretsProviderName;
    private final Supplier<String> usernameSupplier;
    private final Supplier<String> passwordSupplier;

    @Inject
    public SecretsResolverCredentialProvider(SecretsResolverCredentialProviderConfig config, SecretsResolver secretsResolver)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.secretsProviderName = config.getSecretsProviderName();
        // Since CredentialProvider uses different API for fetching username and password we use two suppliers.
        // TODO: Try to merge them into a single API to avoid race condition i.e either username or password could be stale in the mid of resolving credentials
        this.usernameSupplier = memoizeWithExpiration(() -> getResolvedSecrets(config.getUsernameKey()), config.getRefreshInterval().toMillis(), MILLISECONDS);
        this.passwordSupplier = memoizeWithExpiration(() -> getResolvedSecrets(config.getPasswordKey()), config.getRefreshInterval().toMillis(), MILLISECONDS);
    }

    @Override
    public Optional<String> getConnectionUser(Optional<ConnectorIdentity> jdbcIdentity)
    {
        return Optional.ofNullable(usernameSupplier.get());
    }

    @Override
    public Optional<String> getConnectionPassword(Optional<ConnectorIdentity> jdbcIdentity)
    {
        return Optional.ofNullable(passwordSupplier.get());
    }

    private String getResolvedSecrets(String credentialKey)
    {
        return secretsResolver.getResolvedConfiguration(
                ImmutableMap.of("key", "${%s:%s}".formatted(secretsProviderName, credentialKey)),
                        (value, throwable) -> log.error(throwable, "Unable to resolve secret for key '%s'", value))
                .get("key");
    }
}
