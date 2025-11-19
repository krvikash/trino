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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.openapi.authentication.OpenApiAuthenticationScheme;
import io.trino.plugin.openapi.authentication.OpenApiAuthenticationType;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenApiConfig.class)
                .setSpecLocation(null)
                .setBaseUri(null)
                .setAuthenticationType(OpenApiAuthenticationType.NONE)
                .setAuthenticationScheme(OpenApiAuthenticationScheme.BASIC)
                .setClientId(null)
                .setClientSecret(null)
                .setUsername(null)
                .setPassword(null)
                .setBearerToken(null)
                .setApiKeyName(null)
                .setApiKeyValue(null)
                .setApiKeys("")
                .setMaxRequestsPerSecond(Double.MAX_VALUE)
                .setMaxSplitsPerSecond(Double.MAX_VALUE)
                .setDomainExpansionLimit(256));
    }

    @Test
    public void testExplicitPropertyMappingsApiKeys()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.spec-location", "/spec/on/server.json")
                .put("openapi.base-uri", "http://localhost:12012")
                .put("openapi.authentication.type", "api_key")
                .put("openapi.authentication.scheme", "bearer")
                // TODO should disallow setting oauth client values if using api_key auth type.
                .put("openapi.authentication.client-id", "clientid")
                .put("openapi.authentication.client-secret", "clientsecret")
                // TODO same as above, should disallow username/password combo for wrong auth types.
                .put("openapi.authentication.username", "username")
                .put("openapi.authentication.password", "password")
                .put("openapi.authentication.bearer-token", "mybearertoken")
                .put("openapi.authentication.api-keys", "my_key=my_value")
                .put("openapi.max-requests-per-second", "10")
                .put("openapi.max-splits-per-second", "10")
                .put("openapi.domain-expansion-limit", "10")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        OpenApiConfig config = configurationFactory.build(OpenApiConfig.class);

        assertThat(config.getSpecLocation()).isEqualTo("/spec/on/server.json");
        assertThat(config.getBaseUri()).isEqualTo(URI.create("http://localhost:12012"));
        assertThat(config.getAuthenticationType()).isEqualTo(OpenApiAuthenticationType.API_KEY);
        assertThat(config.getAuthenticationScheme()).isEqualTo(OpenApiAuthenticationScheme.BEARER);
        assertThat(config.getClientId()).isEqualTo("clientid");
        assertThat(config.getClientSecret()).isEqualTo("clientsecret");
        assertThat(config.getUsername()).isEqualTo("username");
        assertThat(config.getPassword()).isEqualTo("password");
        assertThat(config.getBearerToken()).isEqualTo("mybearertoken");
        assertThat(config.getApiKeys()).isEqualTo(ImmutableMap.of("my_key", "my_value"));
        assertThat(config.getMaxRequestsPerSecond()).isEqualTo(10.0);
        assertThat(config.getMaxSplitsPerSecond()).isEqualTo(10.0);
        assertThat(config.getDomainExpansionLimit()).isEqualTo(10);
    }

    @Test
    public void testExplicitPropertyMappingsApiKeyPair()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openapi.spec-location", "/spec/on/server.json")
                .put("openapi.base-uri", "http://localhost:12012")
                .put("openapi.authentication.api-key-name", "my_key")
                .put("openapi.authentication.api-key-value", "my_value")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        OpenApiConfig config = configurationFactory.build(OpenApiConfig.class);

        assertThat(config.getSpecLocation()).isEqualTo("/spec/on/server.json");
        assertThat(config.getBaseUri()).isEqualTo(URI.create("http://localhost:12012"));
        assertThat(config.getApiKeyName()).isEqualTo("my_key");
        assertThat(config.getApiKeyValue()).isEqualTo("my_value");
    }

    @Test
    public void testApiKeyValidation()
    {
        assertFailsValidation(
                new OpenApiConfig()
                        .setApiKeys("my_key=my_value")
                        .setApiKeyName("my_key")
                        .setApiKeyValue("my_value"),
                "apiKeyConfigurationValid",
                "At most one of the apiKeys property or both the apiKeyName and apiKeyValue properties must be set",
                AssertTrue.class);
    }

    @Test
    public void testSpecLocationValidation()
    {
        assertFailsValidation(
                new OpenApiConfig()
                        .setBaseUri(URI.create("http://localhost:12012")),
                "specLocation",
                "must not be null",
                NotNull.class);
    }

    @Test
    public void testBaseUriValidation()
    {
        assertFailsValidation(
                new OpenApiConfig()
                        .setSpecLocation("/file/on/server.json"),
                "baseUri",
                "must not be null",
                NotNull.class);
    }

    @Test
    public void testDomainExpansionLimitValidation()
    {
        assertFailsValidation(
                new OpenApiConfig()
                        .setSpecLocation("/file/on/server.json")
                        .setBaseUri(URI.create("http://localhost:12012"))
                        .setDomainExpansionLimit(0),
                "domainExpansionLimit",
                "must be greater than or equal to 1",
                Min.class);
    }
}
