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
package io.trino.sshtunnel;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class SshTunnelConfig
{
    private HostAndPort server;
    private String user;
    private String privateKey;
    private Duration reconnectCheckInterval = new Duration(2, TimeUnit.MINUTES);

    public HostAndPort getServer()
    {
        return server;
    }

    @Config("ssh-tunnel.server")
    public SshTunnelConfig setServer(HostAndPort server)
    {
        this.server = server;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("ssh-tunnel.user")
    public SshTunnelConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPrivateKey()
    {
        return privateKey;
    }

    @Config("ssh-tunnel.private-key")
    @ConfigSecuritySensitive
    public SshTunnelConfig setPrivateKey(String privateKey)
    {
        this.privateKey = privateKey;
        return this;
    }

    @NotNull
    public Duration getReconnectCheckInterval()
    {
        return reconnectCheckInterval;
    }

    @Config("ssh-tunnel.reconnect-check-interval")
    public SshTunnelConfig setReconnectCheckInterval(Duration reconnectCheckInterval)
    {
        this.reconnectCheckInterval = reconnectCheckInterval;
        return this;
    }
}
