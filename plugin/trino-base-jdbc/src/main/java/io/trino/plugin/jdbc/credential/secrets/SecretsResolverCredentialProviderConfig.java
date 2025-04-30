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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

public class SecretsResolverCredentialProviderConfig
{
    private String secretsProviderName;
    private String usernameKey;
    private String passwordKey;
    private Duration refreshInterval = Duration.valueOf("1h");

    @Config("secrets-provider.name")
    public SecretsResolverCredentialProviderConfig setSecretsProviderName(String secretsProviderName)
    {
        this.secretsProviderName = secretsProviderName;
        return this;
    }

    @NotNull
    public String getSecretsProviderName()
    {
        return secretsProviderName;
    }

    @Config("secrets-provider.username-key")
    public SecretsResolverCredentialProviderConfig setUsernameKey(String usernameKey)
    {
        this.usernameKey = usernameKey;
        return this;
    }

    @NotNull
    public String getUsernameKey()
    {
        return usernameKey;
    }

    @Config("secrets-provider.password-key")
    public SecretsResolverCredentialProviderConfig setPasswordKey(String passwordKey)
    {
        this.passwordKey = passwordKey;
        return this;
    }

    @NotNull
    public String getPasswordKey()
    {
        return passwordKey;
    }

    @Config("secrets-provider.refresh-interval")
    public SecretsResolverCredentialProviderConfig setRefreshInterval(Duration refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }
}
