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
package io.trino.plugin.hive.metastore.unity;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class UnityMetastoreConfig
{
    private String catalogName;
    private String host;
    private String token;
    private boolean catalogManagedTableEnabled;
    private boolean vendedCredentialsEnabled;
    private boolean proxyEnabled;

    @NotNull
    public String getCatalogName()
    {
        return catalogName;
    }

    @Config("hive.metastore.unity.catalog-name")
    public UnityMetastoreConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("hive.metastore.unity.host")
    public UnityMetastoreConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @NotNull
    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @ConfigSecuritySensitive
    @Config("hive.metastore.unity.token")
    @ConfigDescription("Unity metastore personal access token")
    public UnityMetastoreConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public boolean isCatalogManagedTableEnabled()
    {
        return catalogManagedTableEnabled;
    }

    @Config("hive.metastore.unity.catalog-managed-table-enabled")
    @LegacyConfig("hive.metastore.unity.catalog-owned-table-enabled")
    @ConfigDescription("Unity metastore catalog managed table writing enabled")
    public UnityMetastoreConfig setCatalogManagedTableEnabled(boolean catalogManagedTableEnabled)
    {
        this.catalogManagedTableEnabled = catalogManagedTableEnabled;
        return this;
    }

    public boolean isVendedCredentialsEnabled()
    {
        return vendedCredentialsEnabled;
    }

    @Config("hive.metastore.unity.vended-credentials-enabled")
    @ConfigDescription("Use credentials provided by Unity for file system access")
    public UnityMetastoreConfig setVendedCredentialsEnabled(boolean vendedCredentialsEnabled)
    {
        this.vendedCredentialsEnabled = vendedCredentialsEnabled;
        return this;
    }

    public boolean isProxyEnabled()
    {
        return proxyEnabled;
    }

    @ConfigDescription("Enable proxying of requests to Unity")
    @Config("hive.metastore.unity.proxy.enabled")
    public UnityMetastoreConfig setProxyEnabled(boolean proxyEnabled)
    {
        this.proxyEnabled = proxyEnabled;
        return this;
    }
}
