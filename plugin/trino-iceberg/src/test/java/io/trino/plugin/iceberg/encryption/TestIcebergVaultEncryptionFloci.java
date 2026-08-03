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
package io.trino.plugin.iceberg.encryption;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.hashicorp.VaultKeyManagementClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.vault.VaultContainer;

import java.util.Map;

import static org.apache.iceberg.hashicorp.VaultKeyManagementClient.VAULT_ADDRESS;

@Testcontainers
final class TestIcebergVaultEncryptionFloci
        extends BaseIcebergEncryptionFlociTest
{
    private static final String ICEBERG_TEST_KEY_NAME = "iceberg-test-key";
    private static final String VAULT_TOKEN = "root-token";

    @Container
    @SuppressWarnings("resource")
    private static final VaultContainer<?> vault = new VaultContainer<>("hashicorp/vault:2.0")
            .withVaultToken(VAULT_TOKEN)
            .withInitCommand(
                    "secrets enable transit",
                    "write -f transit/keys/" + ICEBERG_TEST_KEY_NAME,
                    "auth enable approle",
                    "write sys/policy/transit-policy policy='path \"transit/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }'");

    private static final KeyManagementClient KMS_CLIENT = new VaultKeyManagementClient();

    @BeforeAll
    void beforeAll()
    {
        KMS_CLIENT.initialize(Map.of(VAULT_ADDRESS, vault.getHttpHostAddress(), VaultKeyManagementClient.VAULT_TOKEN, VAULT_TOKEN));
    }

    @AfterAll
    void afterAll()
    {
        KMS_CLIENT.close();
    }

    @Override
    protected String kmsKey()
    {
        return ICEBERG_TEST_KEY_NAME;
    }

    @Override
    protected KeyManagementClient kmsClient()
    {
        return KMS_CLIENT;
    }

    @Override
    protected Map<String, String> kmsProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.encryption.kms-type", "vault")
                .put("vault.address", vault.getHttpHostAddress())
                .put("vault.token", VAULT_TOKEN)
                .buildOrThrow();
    }
}
