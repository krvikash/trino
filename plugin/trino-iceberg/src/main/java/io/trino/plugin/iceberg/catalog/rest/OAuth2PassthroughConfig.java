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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class OAuth2PassthroughConfig
{
    private boolean oauth2TokenExchangeEnabled;
    private Optional<String> oauth2TokenExchangeExtraCredentialsPrefix = Optional.empty();

    public boolean isOauth2TokenExchangeEnabled()
    {
        return this.oauth2TokenExchangeEnabled;
    }

    @Config("iceberg.rest-catalog.oauth2.token-exchange.enabled")
    @ConfigDescription("Whether to exchange the OIDC token for an OAuth2 token before forwarding it to the REST catalog. " +
            "If this is set to true, the OIDC token will be exchanged for an OAuth2 token using the configured OAuth2 provider.")
    public OAuth2PassthroughConfig setOauth2TokenExchangeEnabled(boolean oauth2TokenExchangeEnabled)
    {
        this.oauth2TokenExchangeEnabled = oauth2TokenExchangeEnabled;
        return this;
    }

    @NotNull
    public Optional<String> getOauth2TokenExchangeExtraCredentialsPrefix()
    {
        return this.oauth2TokenExchangeExtraCredentialsPrefix;
    }

    @Config("iceberg.rest-catalog.oauth2.token-exchange.extra-credentials.prefix")
    @ConfigDescription("The prefix for extra credential keys that should be forwarded as additional parameters during OAuth2 token exchange. " +
            "When oauth2.token-exchange is enabled, any extra credential with a key that starts with this prefix will be included as an additional parameter in the token exchange request. " +
            "This can be used to pass additional information required for token exchange, such as the service principal UUID (parameter: client_id) when exchanging tokens for Databricks.")
    public OAuth2PassthroughConfig setOauth2TokenExchangeExtraCredentialsPrefix(String oauth2TokenExchangeExtraCredentialsPrefix)
    {
        this.oauth2TokenExchangeExtraCredentialsPrefix = Optional.ofNullable(oauth2TokenExchangeExtraCredentialsPrefix);
        return this;
    }
}
