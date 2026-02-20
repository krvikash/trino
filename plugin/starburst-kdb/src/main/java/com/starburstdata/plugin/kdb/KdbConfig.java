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
package com.starburstdata.plugin.kdb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public class KdbConfig
{
    private String host = "localhost";
    private int port = 5000;

    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("kdb.host")
    @ConfigDescription("Hostname of the KDB+ server")
    public KdbConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Min(1)
    @Max(65535)
    public int getPort()
    {
        return port;
    }

    @Config("kdb.port")
    @ConfigDescription("Port of the KDB+ server")
    public KdbConfig setPort(int port)
    {
        this.port = port;
        return this;
    }
}
