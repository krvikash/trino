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
package io.trino.plugin.base.security.passthrough;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.base.security.passthrough.MultipleTokensPassthrough.toExtraCredentialKey;
import static io.trino.plugin.base.security.passthrough.OAuth2TokenPassThrough.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class TokenPassThrough
{
    private final Function<ConnectorIdentity, String> tokenExtractor;

    private TokenPassThrough(Function<ConnectorIdentity, String> tokenExtractor)
    {
        this.tokenExtractor = requireNonNull(tokenExtractor, "tokenSupplier is null");
    }

    public static String getToken(ConnectorSession connectorSession, Optional<IdPName> name)
    {
        return getToken(connectorSession.getIdentity(), name);
    }

    public static String getToken(ConnectorIdentity identity, Optional<IdPName> name)
    {
        return name
                .map(TokenPassThrough::token)
                .orElseGet(TokenPassThrough::token)
                .retrieveFrom(identity);
    }

    private static TokenPassThrough token()
    {
        return new TokenPassThrough(identity -> getExtraCredential(identity, OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL)
                .orElseThrow(() -> new TrinoException(
                        GENERIC_USER_ERROR,
                        "Token pass-through authentication requires a valid token, but none has been found")));
    }

    private static Optional<String> getExtraCredential(ConnectorIdentity identity, String key)
    {
        return Optional.ofNullable(identity.getExtraCredentials().get(key));
    }

    private static TokenPassThrough token(IdPName name)
    {
        return new TokenPassThrough(identity -> getExtraCredential(identity, toExtraCredentialKey(name))
                .orElseThrow(() -> new TrinoException(
                        GENERIC_USER_ERROR,
                        format("Token pass-through authentication requires a valid token, but none has been found for identity provider '%s'", name))));
    }

    private String retrieveFrom(ConnectorIdentity identity)
    {
        return Optional.ofNullable(tokenExtractor.apply(identity))
                .map(String::trim)
                .filter(not(String::isEmpty))
                .orElseThrow(() -> new TrinoException(GENERIC_USER_ERROR, "Token pass-through authentication requires a valid token"));
    }
}
