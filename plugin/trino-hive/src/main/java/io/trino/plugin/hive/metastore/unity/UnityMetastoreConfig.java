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
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class UnityMetastoreConfig
{
    private String catalogName;
    private String host;
    private String token;

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
}
