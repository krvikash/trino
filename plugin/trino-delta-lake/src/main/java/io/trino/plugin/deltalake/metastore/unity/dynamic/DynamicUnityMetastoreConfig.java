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
package io.trino.plugin.deltalake.metastore.unity.dynamic;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DynamicUnityMetastoreConfig
{
    private String unityHostCredentialName;
    private String unityTokenCredentialName;
    private String unityCatalogNameCredentialName;
    private boolean vendedCredentialsEnabled;
    private boolean catalogManagedTableEnabled;
    private Optional<String> vendedCredentialsCredentialName = Optional.empty();
    private Duration connectTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(60, TimeUnit.SECONDS);

    @NotNull
    public String getUnityHostCredentialName()
    {
        return unityHostCredentialName;
    }

    @Config("dynamic.hive-metastore-unity-host.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the Unity Catalog host")
    public DynamicUnityMetastoreConfig setUnityHostCredentialName(String unityHostCredentialName)
    {
        this.unityHostCredentialName = unityHostCredentialName;
        return this;
    }

    @NotNull
    public String getUnityTokenCredentialName()
    {
        return unityTokenCredentialName;
    }

    @Config("dynamic.hive-metastore-unity-token.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the Unity Catalog access token")
    public DynamicUnityMetastoreConfig setUnityTokenCredentialName(String unityTokenCredentialName)
    {
        this.unityTokenCredentialName = unityTokenCredentialName;
        return this;
    }

    @NotNull
    public String getUnityCatalogNameCredentialName()
    {
        return unityCatalogNameCredentialName;
    }

    @Config("dynamic.hive-metastore-unity-catalog-name.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the Unity Catalog catalog name")
    public DynamicUnityMetastoreConfig setUnityCatalogNameCredentialName(String unityCatalogNameCredentialName)
    {
        this.unityCatalogNameCredentialName = unityCatalogNameCredentialName;
        return this;
    }

    public boolean isVendedCredentialsEnabled()
    {
        return vendedCredentialsEnabled;
    }

    @Config("hive.metastore.unity.vended-credentials-enabled")
    @ConfigDescription("Use credentials provided by Unity for file system access. Per-request override via dynamic.hive-metastore-unity-vended-credentials-enabled.credential-name takes precedence when the key is present in extra credentials.")
    public DynamicUnityMetastoreConfig setVendedCredentialsEnabled(boolean vendedCredentialsEnabled)
    {
        this.vendedCredentialsEnabled = vendedCredentialsEnabled;
        return this;
    }

    public boolean isCatalogManagedTableEnabled()
    {
        return catalogManagedTableEnabled;
    }

    @Config("hive.metastore.unity.catalog-managed-table-enabled")
    @LegacyConfig("hive.metastore.unity.catalog-owned-table-enabled")
    @ConfigDescription("Unity metastore catalog managed table writing enabled")
    public DynamicUnityMetastoreConfig setCatalogManagedTableEnabled(boolean catalogManagedTableEnabled)
    {
        this.catalogManagedTableEnabled = catalogManagedTableEnabled;
        return this;
    }

    public Optional<String> getVendedCredentialsCredentialName()
    {
        return vendedCredentialsCredentialName;
    }

    @Config("dynamic.hive-metastore-unity-vended-credentials-enabled.credential-name")
    @ConfigDescription("Name of the extra credential key indicating whether to use Unity-vended credentials for file system access")
    public DynamicUnityMetastoreConfig setVendedCredentialsCredentialName(String vendedCredentialsCredentialName)
    {
        this.vendedCredentialsCredentialName = Optional.ofNullable(vendedCredentialsCredentialName);
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("hive.metastore.unity.connect-timeout")
    @ConfigDescription("Connect timeout for HTTP calls to the Unity Catalog server")
    public DynamicUnityMetastoreConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("hive.metastore.unity.read-timeout")
    @ConfigDescription("Read timeout for HTTP calls to the Unity Catalog server")
    public DynamicUnityMetastoreConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }
}
