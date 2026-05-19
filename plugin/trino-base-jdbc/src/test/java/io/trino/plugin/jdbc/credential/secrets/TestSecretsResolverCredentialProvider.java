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
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.spi.secrets.SecretProvider;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestSecretsResolverCredentialProvider
{
    @Test
    void testCrendentialProvider()
    {
        CredentialProvider credentialProvider = new SecretsResolverCredentialProvider(
                new SecretsResolverCredentialProviderConfig()
                        .setSecretsProviderName("testing")
                        .setUsernameKey("username")
                        .setPasswordKey("password")
                        .setRefreshInterval(Duration.valueOf("5s")),
                new SecretsResolver(ImmutableMap.of("testing", key -> "resolved_" + key)));

        assertThat(credentialProvider.getConnectionUser(Optional.empty()))
                .hasValue("resolved_username");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()))
                .hasValue("resolved_password");
    }

    @Test
    void testCredentialForInvalidKey()
    {
        CredentialProvider credentialProvider = new SecretsResolverCredentialProvider(
                new SecretsResolverCredentialProviderConfig()
                        .setSecretsProviderName("failing")
                        .setUsernameKey("username")
                        .setPasswordKey("password"),
                new SecretsResolver(
                        ImmutableMap.of(
                                "failing", _ -> {
                                    throw new UnsupportedOperationException();
                                })));

        assertThat(credentialProvider.getConnectionUser(Optional.empty()))
                .isEmpty();
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()))
                .isEmpty();
    }

    @Test
    void testCredentialCaching()
            throws Exception
    {
        CountingSecretsProvider countingSecretsProvider = new CountingSecretsProvider();
        CredentialProvider credentialProvider = new SecretsResolverCredentialProvider(
                new SecretsResolverCredentialProviderConfig()
                        .setSecretsProviderName("counting")
                        .setUsernameKey("username")
                        .setPasswordKey("password")
                        .setRefreshInterval(Duration.valueOf("5s")),
                new SecretsResolver(ImmutableMap.of("counting", countingSecretsProvider)));

        assertThat(credentialProvider.getConnectionUser(Optional.empty()))
                .hasValue("username");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()))
                .hasValue("password");

        assertThat(countingSecretsProvider.getCounterForKey("username")).isEqualTo(1);
        assertThat(countingSecretsProvider.getCounterForKey("password")).isEqualTo(1);

        assertThat(credentialProvider.getConnectionUser(Optional.empty()))
                .hasValue("username");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()))
                .hasValue("password");

        assertThat(countingSecretsProvider.getCounterForKey("username")).isEqualTo(1);
        assertThat(countingSecretsProvider.getCounterForKey("password")).isEqualTo(1);

        Thread.sleep(5000);

        assertThat(credentialProvider.getConnectionUser(Optional.empty()))
                .hasValue("username");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()))
                .hasValue("password");

        assertThat(countingSecretsProvider.getCounterForKey("username")).isEqualTo(2);
        assertThat(countingSecretsProvider.getCounterForKey("password")).isEqualTo(2);
    }

    private static class CountingSecretsProvider
            implements SecretProvider
    {
        private final Map<String, AtomicInteger> counters = new HashMap<>();

        @Override
        public String resolveSecretValue(String key)
        {
            counters.computeIfAbsent(key, _ -> new AtomicInteger()).incrementAndGet();
            return key;
        }

        public int getCounterForKey(String key)
        {
            return counters.get(key).get();
        }
    }
}
