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
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

@DefunctConfig("openapi.max-requests-per-second")
public class OpenApiConfig
{
    private String descriptionLocation;
    private URI baseUri;

    @NotNull
    public String getDescriptionLocation()
    {
        return descriptionLocation;
    }

    @LegacyConfig("openapi.spec-location") // From original openapi plugin, distributed in some demo packages.
    @Config("openapi.description-location")
    @ConfigDescription("Path to the OpenAPI description file")
    public OpenApiConfig setDescriptionLocation(String value)
    {
        this.descriptionLocation = value;
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
}
