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
package io.trino.plugin.warp.extension.config;

import io.airlift.configuration.Config;

public class WarpExtensionConfig
{
    public static final String CLUSTER_UUID = "warp-speed.cluster-uuid";
    public static final String HTTP_REST_PORT = "http-rest-port";
    // true in case we leverage the trino http port
    public static final String USE_HTTP_SERVER_PORT = "warp-speed.use-http-server-port";
    public static final String HTTP_REST_PORT_ENABLED = "warp-speed.config.http-rest-port-enabled";
    public static final String INTERNAL_COMMUNICATION_SHARED_SECRET = "warp-speed.config.internal-communication.shared-secret";
    public static final String ENABLED = "warp-speed.config.extensions.enabled";
    public static final int HTTP_REST_DEFAULT_PORT = 8088;

    private String clusterUUID;
    private int restHttpPort = HTTP_REST_DEFAULT_PORT;
    private boolean useHttpServerPort;
    private boolean restHttpDefaultPortEnabled;
    private String internalCommunicationSharedSecret;
    private boolean enabled;

    public String getClusterUUID()
    {
        return clusterUUID;
    }

    @Config(CLUSTER_UUID)
    public void setClusterUUID(String clusterUUID)
    {
        this.clusterUUID = clusterUUID;
    }

    @Config(USE_HTTP_SERVER_PORT)
    public void setUseHttpServerPort(boolean useHttpServerPort)
    {
        this.useHttpServerPort = useHttpServerPort;
    }

    public int getRestHttpPort()
    {
        return restHttpPort;
    }

    @Config(HTTP_REST_PORT)
    public void setRestHttpPort(int restHttpPort)
    {
        this.restHttpPort = restHttpPort;
    }

    @Config(HTTP_REST_PORT_ENABLED)
    public void setRestHttpDefaultPortEnabled(boolean restHttpDefaultPortEnabled)
    {
        this.restHttpDefaultPortEnabled = restHttpDefaultPortEnabled;
    }

    public String getInternalCommunicationSharedSecret()
    {
        return internalCommunicationSharedSecret;
    }

    @Config(INTERNAL_COMMUNICATION_SHARED_SECRET)
    public void setInternalCommunicationSharedSecret(String internalCommunicationSharedSecret)
    {
        this.internalCommunicationSharedSecret = internalCommunicationSharedSecret;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config(ENABLED)
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public boolean isUseHttpServerPort()
    {
        return useHttpServerPort;
    }

    public boolean isRestHttpDefaultPortEnabled()
    {
        return restHttpDefaultPortEnabled;
    }
}
