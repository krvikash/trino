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
package com.starburstdata.plugin.openapi;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class OpenApiConfig
{
    private String specLocation;
    private URI baseUri;

    private double maxRequestsPerSecond = Double.MAX_VALUE;

    @NotNull
    public String getSpecLocation()
    {
        return specLocation;
    }

    @Config("openapi.spec-location")
    @ConfigDescription("Path to the OpenAPI spec file")
    public OpenApiConfig setSpecLocation(String value)
    {
        this.specLocation = value;
        return this;
    }

    @NotNull
    public URI getBaseUri()
    {
        return baseUri;
    }

    @Config("openapi.base-uri")
    @ConfigDescription("Base URI of the API")
    public OpenApiConfig setBaseUri(URI baseUri)
    {
        this.baseUri = baseUri;
        return this;
    }

    public double getMaxRequestsPerSecond()
    {
        return maxRequestsPerSecond;
    }

    @Config("openapi.max-requests-per-second")
    public OpenApiConfig setMaxRequestsPerSecond(double maxRequestsPerSecond)
    {
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        return this;
    }
}
