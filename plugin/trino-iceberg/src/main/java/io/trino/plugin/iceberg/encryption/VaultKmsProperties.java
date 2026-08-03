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
import com.google.inject.Inject;
import io.trino.plugin.base.encryption.VaultConfig;
import org.apache.iceberg.hashicorp.VaultKeyManagementClient;

import java.util.Map;

import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_IMPL;
import static org.apache.iceberg.hashicorp.VaultKeyManagementClient.VAULT_ADDRESS;
import static org.apache.iceberg.hashicorp.VaultKeyManagementClient.VAULT_TOKEN;

public class VaultKmsProperties
        implements KmsProperties
{
    private final Map<String, String> properties;

    @Inject
    public VaultKmsProperties(VaultConfig config)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put(ENCRYPTION_KMS_IMPL, VaultKeyManagementClient.class.getName());
        builder.put(VAULT_ADDRESS, config.getAddress());
        builder.put(VAULT_TOKEN, config.getToken());
        properties = builder.buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
