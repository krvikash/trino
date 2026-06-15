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
package io.starburst.stargate.icehouse.catalog.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

/**
 * Airlift config bound to {@code iceberg.rest-catalog.*} property keys.
 */
public class RestCatalogConfig
{
    public enum Security
    {
        NONE,
        OAUTH2,
        SIGV4,
    }

    private URI uri;
    private String warehouse;
    private Security security = Security.NONE;
    private Optional<String> oauth2Credential = Optional.empty();
    private Optional<String> oauth2Scope = Optional.empty();
    private Optional<String> signingName = Optional.empty();
    private Optional<String> signingRegion = Optional.empty();

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("iceberg.rest-catalog.uri")
    @ConfigDescription("URI of the Iceberg REST catalog server")
    public RestCatalogConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @NotNull
    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("iceberg.rest-catalog.warehouse")
    @ConfigDescription("Warehouse location or catalog identifier for the REST catalog")
    public RestCatalogConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    @NotNull
    public Security getSecurity()
    {
        return security;
    }

    @Config("iceberg.rest-catalog.security")
    @ConfigDescription("Authentication protocol for the REST catalog (NONE, OAUTH2, SIGV4)")
    public RestCatalogConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    public Optional<String> getOauth2Credential()
    {
        return oauth2Credential;
    }

    @Config("iceberg.rest-catalog.oauth2.credential")
    @ConfigDescription("OAuth2 credential in clientId:clientSecret format")
    @ConfigSecuritySensitive
    public RestCatalogConfig setOauth2Credential(String oauth2Credential)
    {
        this.oauth2Credential = Optional.ofNullable(oauth2Credential);
        return this;
    }

    public Optional<String> getOauth2Scope()
    {
        return oauth2Scope;
    }

    @Config("iceberg.rest-catalog.oauth2.scope")
    @ConfigDescription("OAuth2 scope for token requests")
    public RestCatalogConfig setOauth2Scope(String oauth2Scope)
    {
        this.oauth2Scope = Optional.ofNullable(oauth2Scope);
        return this;
    }

    public Optional<String> getSigningName()
    {
        return signingName;
    }

    @Config("iceberg.rest-catalog.signing-name")
    @ConfigDescription("AWS SigV4 signing service name (e.g. s3tables)")
    public RestCatalogConfig setSigningName(String signingName)
    {
        this.signingName = Optional.ofNullable(signingName);
        return this;
    }

    public Optional<String> getSigningRegion()
    {
        return signingRegion;
    }

    @Config("iceberg.rest-catalog.signing-region")
    @ConfigDescription("AWS SigV4 signing region")
    public RestCatalogConfig setSigningRegion(String signingRegion)
    {
        this.signingRegion = Optional.ofNullable(signingRegion);
        return this;
    }
}
