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
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SshTunnelProperties
{
    private final HostAndPort server;
    private final String user;
    private final String privateKey;
    private final Duration reconnectCheckInterval;

    public SshTunnelProperties(HostAndPort server, String user, String privateKey, Duration reconnectCheckInterval)
    {
        this.server = requireNonNull(server, "server is null");
        this.user = requireNonNull(user, "user is null");
        this.privateKey = requireNonNull(privateKey, "privateKey is null");
        this.reconnectCheckInterval = requireNonNull(reconnectCheckInterval, "reconnectCheckInterval is null");
    }

    public static Optional<SshTunnelProperties> generateFrom(SshTunnelConfig config)
    {
        if (config.getServer() == null) {
            return Optional.empty();
        }

        return Optional.of(new SshTunnelProperties(
                config.getServer(),
                config.getUser(),
                config.getPrivateKey(),
                config.getReconnectCheckInterval()));
    }

    public HostAndPort getServer()
    {
        return server;
    }

    public String getUser()
    {
        return user;
    }

    public String getPrivateKey()
    {
        return privateKey;
    }

    public Duration getReconnectCheckInterval()
    {
        return reconnectCheckInterval;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SshTunnelProperties that = (SshTunnelProperties) o;
        return server.equals(that.server) && user.equals(that.user) &&
                privateKey.equals(that.privateKey) &&
                reconnectCheckInterval.equals(that.reconnectCheckInterval);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(server, user, privateKey, reconnectCheckInterval);
    }
}
