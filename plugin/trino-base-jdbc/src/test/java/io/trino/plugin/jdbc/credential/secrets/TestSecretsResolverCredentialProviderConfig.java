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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestSecretsResolverCredentialProviderConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SecretsResolverCredentialProviderConfig.class)
                .setSecretsProviderName(null)
                .setUsernameKey(null)
                .setPasswordKey(null)
                .setRefreshInterval(Duration.valueOf("1h")));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of(
                "secrets-provider.name", "keystore",
                "secrets-provider.username-key", "username",
                "secrets-provider.password-key", "password",
                "secrets-provider.refresh-interval", "100h");

        SecretsResolverCredentialProviderConfig expected = new SecretsResolverCredentialProviderConfig()
                .setSecretsProviderName("keystore")
                .setUsernameKey("username")
                .setPasswordKey("password")
                .setRefreshInterval(Duration.valueOf("100h"));

        assertFullMapping(properties, expected);
    }
}
