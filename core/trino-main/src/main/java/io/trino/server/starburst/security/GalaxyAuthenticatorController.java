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
package io.trino.server.starburst.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.jsonwebtoken.JwtException;
import io.trino.server.security.AuthenticationException;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.IdentityParams;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.RequestBodyHashing;
import io.trino.spi.security.Identity;

import java.util.Optional;

import static io.trino.server.starburst.security.GalaxyAuthenticationHelper.parseClaimsWithoutValidation;
import static io.trino.server.starburst.security.GalaxyIdentity.createIdentity;

public final class GalaxyAuthenticatorController
        extends AbstractGalaxyAuthenticatorController
{
    private static final Logger log = Logger.get(GalaxyAuthenticatorController.class);

    public GalaxyAuthenticatorController(String issuer, String audience, JwtClaimsParser jwtClaimsParser)
    {
        super(ImmutableMap.of(issuer, ImmutableSet.of(audience)), jwtClaimsParser);
    }

    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        try {
            IdentityParams identityParams = commonAuthenticate(token, requestBodyHashing);
            return createIdentity(
                    identityParams.username(),
                    identityParams.accountId(),
                    identityParams.userId(),
                    identityParams.roleId(),
                    identityParams.enabledRoles(),
                    token,
                    identityParams.identityType());
        }
        catch (JwtException e) {
            log.error(e, "Exception parsing JWT token: %s", parseClaimsWithoutValidation(token).orElse("not a JWT token"));
            throw new AuthenticationException(e.getMessage(), "Galaxy");
        }
        catch (RuntimeException e) {
            log.error(e, "Authentication error: %s", parseClaimsWithoutValidation(token).orElse("not a JWT token"));
            throw new RuntimeException("Authentication error", e);
        }
    }
}
