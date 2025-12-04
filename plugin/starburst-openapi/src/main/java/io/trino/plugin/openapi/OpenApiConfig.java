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
package io.trino.plugin.openapi;

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.trino.plugin.openapi.authentication.OpenApiAuthenticationScheme;
import io.trino.plugin.openapi.authentication.OpenApiAuthenticationType;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OpenApiConfig
{
    private String specLocation;
    private URI baseUri;
    private OpenApiAuthenticationType authenticationType = OpenApiAuthenticationType.NONE;

    private OpenApiAuthenticationScheme authenticationScheme = OpenApiAuthenticationScheme.BASIC;

    private String username;
    private String password;
    private String bearerToken;

    private Map<String, String> apiKeys = Map.of();
    private String apiKeyName;
    private String apiKeyValue;

    private String clientId;
    private String clientSecret;

    private double maxRequestsPerSecond = Double.MAX_VALUE;
    private double maxSplitsPerSecond = Double.MAX_VALUE;

    // Pushed domains are transformed into SQL IN lists
    // (or sequence of range predicates).
    // Too large IN lists cause too many requests being made, so a hard limit is required.
    private int domainExpansionLimit = 256;

    @AssertTrue(message = "At most one of the apiKeys property or both the apiKeyName and apiKeyValue properties must be set")
    public boolean isApiKeyConfigurationValid()
    {
        if (apiKeyName != null || apiKeyValue != null) {
            return apiKeys.isEmpty();
        }
        return true;
    }

    @NotNull
    public String getSpecLocation()
    {
        return specLocation;
    }

    @Config("openapi.spec-location")
    @ConfigDescription("Path to the OpenAPI spec file")
    public OpenApiConfig setSpecLocation(String value)
    {
        this.specLocation = value;
        return this;
    }

    @NotNull
    public URI getBaseUri()
    {
        return baseUri;
    }

    @Config("openapi.base-uri")
    @ConfigDescription("Base URI of the API")
    public OpenApiConfig setBaseUri(URI baseUri)
    {
        this.baseUri = baseUri;
        return this;
    }

    public OpenApiAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("openapi.authentication.type")
    @ConfigDescription("Default authentication type if not set in the API specification")
    public OpenApiConfig setAuthenticationType(OpenApiAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    @NotNull
    public OpenApiAuthenticationScheme getAuthenticationScheme()
    {
        return authenticationScheme;
    }

    @Config("openapi.authentication.scheme")
    @ConfigDescription("HTTP authentication scheme")
    public OpenApiConfig setAuthenticationScheme(OpenApiAuthenticationScheme authenticationScheme)
    {
        this.authenticationScheme = authenticationScheme;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("openapi.authentication.username")
    @ConfigDescription("Username")
    public OpenApiConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("openapi.authentication.password")
    @ConfigDescription("Password")
    @ConfigSecuritySensitive
    public OpenApiConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getBearerToken()
    {
        return bearerToken;
    }

    @Config("openapi.authentication.bearer-token")
    @ConfigDescription("Bearer token")
    @ConfigSecuritySensitive
    public OpenApiConfig setBearerToken(String bearerToken)
    {
        this.bearerToken = bearerToken;
        return this;
    }

    @NotNull
    public Map<String, String> getApiKeys()
    {
        return apiKeys;
    }

    @Config("openapi.authentication.api-keys")
    public OpenApiConfig setApiKeys(String apiKeys)
    {
        if (apiKeys.isEmpty()) {
            return this;
        }
        this.apiKeys = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .withKeyValueSeparator("=")
                .split(requireNonNull(apiKeys, "apiKeys is null"));
        return this;
    }

    public String getApiKeyName()
    {
        return apiKeyName;
    }

    @Config("openapi.authentication.api-key-name")
    @ConfigDescription("API key name")
    public OpenApiConfig setApiKeyName(String apiKeyName)
    {
        this.apiKeyName = apiKeyName;
        return this;
    }

    public String getApiKeyValue()
    {
        return apiKeyValue;
    }

    @Config("openapi.authentication.api-key-value")
    @ConfigDescription("API key value")
    @ConfigSecuritySensitive
    public OpenApiConfig setApiKeyValue(String apiKeyValue)
    {
        this.apiKeyValue = apiKeyValue;
        return this;
    }

    public String getClientId()
    {
        return clientId;
    }

    @Config("openapi.authentication.client-id")
    @ConfigDescription("OAuth client ID")
    public OpenApiConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("openapi.authentication.client-secret")
    @ConfigDescription("OAuth client secret")
    @ConfigSecuritySensitive
    public OpenApiConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    public double getMaxRequestsPerSecond()
    {
        return maxRequestsPerSecond;
    }

    @Config("openapi.max-requests-per-second")
    public OpenApiConfig setMaxRequestsPerSecond(double maxRequestsPerSecond)
    {
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        return this;
    }

    public double getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("openapi.max-splits-per-second")
    public OpenApiConfig setMaxSplitsPerSecond(double maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
    }

    @Min(1)
    public int getDomainExpansionLimit()
    {
        return domainExpansionLimit;
    }

    @Config("openapi.domain-expansion-limit")
    @ConfigDescription("Maximum number of discrete values in a predicate domain.")
    public OpenApiConfig setDomainExpansionLimit(int domainExpansionLimit)
    {
        this.domainExpansionLimit = domainExpansionLimit;
        return this;
    }
}
