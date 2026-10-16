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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.security.passthrough.IdPName;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.security.passthrough.TokenPassThrough.getToken;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE_OAUTH2;
import static org.apache.iceberg.rest.auth.OAuth2Properties.JWT_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.OAuth2Properties.OAUTH2_SERVER_URI;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN_REFRESH_ENABLED;

public class OAuth2PassThroughWithTokenExchangeSecurityProperties
        implements SecurityProperties
{
    private final Map<String, String> securityProperties;
    private final Optional<IdPName> idPName;
    private final URI oauth2ServerUri;
    private final String scope;
    private final HTTPClient httpClient;
    private final Map<String, String> exchangeTokenHeaders;
    private final Optional<String> extraCredentialsPrefix;

    @Inject
    public OAuth2PassThroughWithTokenExchangeSecurityProperties(
            OAuth2SecurityConfig securityConfig,
            TokenPassThroughConfig tokenPassThroughConfig,
            OAuth2PassthroughConfig oauth2PassthroughConfig)
    {
        this.idPName = tokenPassThroughConfig.getIdpName();
        this.oauth2ServerUri = securityConfig.getServerUri().orElseThrow(() -> new IllegalArgumentException("OAuth2 Server URI must be provided for OAuth2 PassThrough mode"));
        this.scope = securityConfig.getScope().orElse("");
        this.extraCredentialsPrefix = normalizePrefix(oauth2PassthroughConfig.getOauth2TokenExchangeExtraCredentialsPrefix());

        this.exchangeTokenHeaders = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, oauth2ServerUri.toString())
                .buildOrThrow();

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        propertiesBuilder.put(AUTH_TYPE, AUTH_TYPE_OAUTH2);
        propertiesBuilder.put(OAUTH2_SERVER_URI, this.oauth2ServerUri.toString());
        propertiesBuilder.put(TOKEN_REFRESH_ENABLED, String.valueOf(securityConfig.isTokenRefreshEnabled()));
        this.securityProperties = propertiesBuilder.buildOrThrow();

        this.httpClient = HTTPClient.builder(ImmutableMap.of())
                .uri(oauth2ServerUri.toString())
                .withAuthSession(AuthSession.EMPTY)
                .build();
    }

    @Override
    public Map<String, String> get(ConnectorIdentity identity)
    {
        String token = getToken(identity, idPName);

        try {
            token = exchangeToken(oauth2ServerUri, token, scope, provideAdditionalParameters(identity.getExtraCredentials()));
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "OAuth2 token exchange failed", e);
        }

        return ImmutableMap.<String, String>builder()
                .putAll(securityProperties)
                .put(TOKEN, token)
                .buildOrThrow();
    }

    @Override
    public boolean variesByUser()
    {
        return true;
    }

    private String exchangeToken(URI oauth2ServerUri, String subjectToken, String scope, Map<String, String> optionalParams)
    {
        OAuthTokenResponse response = OAuth2Util.exchangeToken(
                httpClient,
                exchangeTokenHeaders,
                subjectToken,
                JWT_TOKEN_TYPE,
                null,
                null,
                scope,
                oauth2ServerUri.toString(),
                optionalParams);

        response.validate();
        return response.token();
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    private Map<String, String> provideAdditionalParameters(Map<String, String> extraCredentials)
    {
        if (extraCredentialsPrefix.isEmpty()) {
            return ImmutableMap.of();
        }

        String prefix = extraCredentialsPrefix.get();
        return extraCredentials.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(toImmutableMap(
                        entry -> entry.getKey().substring(prefix.length()),
                        Map.Entry::getValue));
    }

    private static Optional<String> normalizePrefix(Optional<String> prefix)
    {
        return prefix.map(OAuth2PassThroughWithTokenExchangeSecurityProperties::ensureSuffixWithDot);
    }

    private static String ensureSuffixWithDot(String prefix)
    {
        return prefix.endsWith(".") ? prefix : prefix + ".";
    }
}
