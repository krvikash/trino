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
package io.trino.server.starburst.accesscontrol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class GalaxyAccessControlUrlsConfig
{
    private URI accountUri;
    private URI accessControlOverrideUri;
    private Optional<URI> regionalAccessControlUri = Optional.empty();

    @NotNull
    public URI getAccountUri()
    {
        return accountUri;
    }

    @Config("galaxy.account-url")
    public GalaxyAccessControlUrlsConfig setAccountUri(URI accountUri)
    {
        this.accountUri = accountUri;
        return this;
    }

    @NotNull
    public URI getAccessControlOverrideUri()
    {
        return accessControlOverrideUri;
    }

    @Config("galaxy.access-control-url")
    public GalaxyAccessControlUrlsConfig setAccessControlOverrideUri(URI accessControlOverrideUri)
    {
        this.accessControlOverrideUri = accessControlOverrideUri;
        return this;
    }

    @NotNull
    public Optional<URI> getRegionalAccessControlUri()
    {
        return regionalAccessControlUri;
    }

    @Config("galaxy.regional-access-control-url")
    @ConfigDescription(
            """
            An optional URL to use for regional access control checks.
            This property may not be present. If not, the galaxy.access-control-url should be used instead.
            """)
    public GalaxyAccessControlUrlsConfig setRegionalAccessControlUri(URI regionalAccessControlUri)
    {
        this.regionalAccessControlUri = Optional.ofNullable(regionalAccessControlUri);
        return this;
    }
}
