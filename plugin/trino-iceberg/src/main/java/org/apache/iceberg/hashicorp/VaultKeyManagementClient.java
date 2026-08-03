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
package org.apache.iceberg.hashicorp;

import org.apache.iceberg.encryption.KeyManagementClient;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class VaultKeyManagementClient
        implements KeyManagementClient, Closeable
{
    public static final String VAULT_ADDRESS = "vault.address";
    public static final String VAULT_TOKEN = "vault.token";

    private VaultClient client;
    private String vaultToken;

    @Override
    public void initialize(Map<String, String> properties)
    {
        String vaultAddress = requireNonNull(properties.get(VAULT_ADDRESS), "%s is null".formatted(VAULT_ADDRESS));
        client = new VaultClient(vaultAddress);
        vaultToken = requireNonNull(properties.get(VAULT_TOKEN), "%s is null".formatted(VAULT_TOKEN));
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        byte[] keyBytes = new byte[key.remaining()];
        key.duplicate().get(keyBytes);
        String plaintext = Base64.getEncoder().encodeToString(keyBytes);

        String ciphertext = client.encrypt(vaultToken, wrappingKeyId, plaintext);
        return ByteBuffer.wrap(ciphertext.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        byte[] ciphertextBytes = new byte[wrappedKey.remaining()];
        wrappedKey.duplicate().get(ciphertextBytes);
        String ciphertext = new String(ciphertextBytes, StandardCharsets.UTF_8);

        String plaintext = client.decrypt(vaultToken, wrappingKeyId, ciphertext);
        byte[] keyBytes = Base64.getDecoder().decode(plaintext);
        return ByteBuffer.wrap(keyBytes);
    }

    @Override
    public boolean supportsKeyGeneration()
    {
        return true;
    }

    @Override
    public KeyGenerationResult generateKey(String wrappingKeyId)
    {
        VaultClient.DataKey dataKey = client.generateKey(vaultToken, wrappingKeyId);

        byte[] keyBytes = Base64.getDecoder().decode(dataKey.plaintext());
        ByteBuffer key = ByteBuffer.wrap(keyBytes);
        ByteBuffer wrappedKey = ByteBuffer.wrap(dataKey.ciphertext().getBytes(StandardCharsets.UTF_8));

        return new KeyGenerationResult(key, wrappedKey);
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
