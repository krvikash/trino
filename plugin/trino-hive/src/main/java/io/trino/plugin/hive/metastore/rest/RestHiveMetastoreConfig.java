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
package io.trino.plugin.hive.metastore.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.starburst.stargate.id.MetastoreId;
import io.trino.filesystem.Location;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.net.URI;

public class RestHiveMetastoreConfig
{
    private MetastoreId metastoreId;
    private String sharedSecret;
    private URI serverUri;
    private Location defaultDataDirectory;

    @NotNull
    public MetastoreId getMetastoreId()
    {
        return metastoreId;
    }

    @Config("hive.metastore.rest.metastore-id")
    public RestHiveMetastoreConfig setMetastoreId(MetastoreId metastoreId)
    {
        this.metastoreId = metastoreId;
        return this;
    }

    @NotNull
    @Size(min = 64, max = 64)
    @Pattern(regexp = "[0-9a-fA-F]+")
    public String getSharedSecret()
    {
        return sharedSecret;
    }

    @Config("hive.metastore.rest.shared-secret")
    @ConfigSecuritySensitive
    public RestHiveMetastoreConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = sharedSecret;
        return this;
    }

    @NotNull
    public URI getServerUri()
    {
        return serverUri;
    }

    @Config("hive.metastore.rest.server-uri")
    public RestHiveMetastoreConfig setServerUri(URI serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @NotNull
    public Location getDefaultDataDirectory()
    {
        return defaultDataDirectory;
    }

    @Config("hive.metastore.rest.default-data-dir")
    public RestHiveMetastoreConfig setDefaultDataDirectory(String defaultDataDirectory)
    {
        return setDefaultDataDirectory(defaultDataDirectory == null ? null : Location.of(defaultDataDirectory));
    }

    public RestHiveMetastoreConfig setDefaultDataDirectory(Location defaultDataDirectory)
    {
        this.defaultDataDirectory = defaultDataDirectory;
        return this;
    }
}
