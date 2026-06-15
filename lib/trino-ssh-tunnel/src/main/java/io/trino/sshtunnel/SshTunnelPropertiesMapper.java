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

import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class SshTunnelPropertiesMapper
{
    public static final String SSH_TUNNEL_SERVER_PROPERTY_NAME = "sshTunnelServer";
    public static final String SSH_TUNNEL_USER_PROPERTY_NAME = "sshTunnelUser";
    public static final String SSH_TUNNEL_PRIVATE_KEY_PROPERTY_NAME = "sshTunnelPrivateKey";
    public static final String SSH_TUNNEL_RECONNECT_CHECK_INTERVAL_PROPERTY_NAME = "sshTunnelReconnectCheckInterval";

    private SshTunnelPropertiesMapper() {}

    public static void addSshTunnelProperties(BiConsumer<String, String> propertiesConsumer, SshTunnelProperties sshTunnelProperties)
    {
        propertiesConsumer.accept(SSH_TUNNEL_SERVER_PROPERTY_NAME, sshTunnelProperties.getServer().toString());
        propertiesConsumer.accept(SSH_TUNNEL_USER_PROPERTY_NAME, sshTunnelProperties.getUser());
        propertiesConsumer.accept(SSH_TUNNEL_PRIVATE_KEY_PROPERTY_NAME, sshTunnelProperties.getPrivateKey());
        propertiesConsumer.accept(SSH_TUNNEL_RECONNECT_CHECK_INTERVAL_PROPERTY_NAME, sshTunnelProperties.getReconnectCheckInterval().toString());
    }

    public static Optional<SshTunnelProperties> getSshTunnelProperties(Function<String, Optional<String>> propertyProvider)
    {
        return propertyProvider.apply(SSH_TUNNEL_SERVER_PROPERTY_NAME)
                .map(HostAndPort::fromString)
                .map(server -> {
                    String user = getRequiredProperty(propertyProvider, SSH_TUNNEL_USER_PROPERTY_NAME);
                    String privateKey = getRequiredProperty(propertyProvider, SSH_TUNNEL_PRIVATE_KEY_PROPERTY_NAME);
                    Duration reconnectCheckInterval = Duration.valueOf(getRequiredProperty(propertyProvider, SSH_TUNNEL_RECONNECT_CHECK_INTERVAL_PROPERTY_NAME));
                    return new SshTunnelProperties(server, user, privateKey, reconnectCheckInterval);
                });
    }

    public static String getRequiredProperty(Properties properties, String propertyName)
    {
        return getOptionalProperty(properties, propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }

    public static Optional<String> getOptionalProperty(Properties properties, String propertyName)
    {
        return Optional.ofNullable(properties.getProperty(propertyName));
    }

    public static String getRequiredProperty(Function<String, Optional<String>> propertyProvider, String propertyName)
    {
        return propertyProvider.apply(propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }
}
