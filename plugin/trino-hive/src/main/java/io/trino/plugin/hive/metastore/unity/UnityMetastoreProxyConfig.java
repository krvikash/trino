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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class UnityMetastoreProxyConfig
{
    private String proxyHost;
    private int proxyPort = -1;
    private Optional<String> username = Optional.empty();
    private Optional<String> password = Optional.empty();
    private List<String> nonProxyHosts = ImmutableList.of();

    @NotNull
    public String getProxyHost()
    {
        return proxyHost;
    }

    @ConfigDescription("Hostname of the proxy server to use")
    @Config("hive.metastore.unity.proxy.host")
    public UnityMetastoreProxyConfig setProxyHost(String proxyHost)
    {
        this.proxyHost = proxyHost;
        return this;
    }

    @Min(0)
    public int getProxyPort()
    {
        return proxyPort;
    }

    @ConfigDescription("Port number of the proxy server to use")
    @Config("hive.metastore.unity.proxy.port")
    public UnityMetastoreProxyConfig setProxyPort(int proxyPort)
    {
        this.proxyPort = proxyPort;
        return this;
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    @ConfigDescription("Username for authenticating to the proxy server")
    @Config("hive.metastore.unity.proxy.username")
    public UnityMetastoreProxyConfig setUsername(String username)
    {
        this.username = Optional.ofNullable(username);
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @ConfigDescription("Password for authenticating to the proxy server")
    @Config("hive.metastore.unity.proxy.password")
    @ConfigSecuritySensitive
    public UnityMetastoreProxyConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    @NotNull
    public List<String> getNonProxyHosts()
    {
        return nonProxyHosts;
    }

    @ConfigDescription("Lists of hosts to connect to directly, bypassing the proxy server")
    @Config("hive.metastore.unity.proxy.non-proxy-hosts")
    public UnityMetastoreProxyConfig setNonProxyHosts(List<String> nonProxyHosts)
    {
        this.nonProxyHosts = ImmutableList.copyOf(nonProxyHosts);
        return this;
    }
}
