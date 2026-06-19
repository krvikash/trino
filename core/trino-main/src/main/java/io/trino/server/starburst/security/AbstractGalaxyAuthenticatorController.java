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

import io.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.server.security.AuthenticationException;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.IdentityParams;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.RequestBodyHashing;
import io.trino.server.starburst.security.GalaxyIdentity.EmbeddedActiveRoleSet;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.jsonwebtoken.ClaimJwtException.INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.jsonwebtoken.ClaimJwtException.MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.jsonwebtoken.Claims.ISSUER;
import static io.trino.server.starburst.security.GalaxyIdentity.parseEmbeddedEnabledRoles;
import static io.trino.server.starburst.security.GalaxyIdentity.toGalaxyIdentityType;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public abstract class AbstractGalaxyAuthenticatorController
{
    static final String REQUEST_EXPIRATION_CLAIM = "request_expiry";
    private static final Logger log = Logger.get(AbstractGalaxyAuthenticatorController.class);
    private final JwtClaimsParser jwtClaimsParser;
    private final Map<String, Set<String>> issuerAudienceMapping;

    protected AbstractGalaxyAuthenticatorController(Map<String, Set<String>> issuerAudienceMapping, JwtClaimsParser jwtClaimsParser)
    {
        this.issuerAudienceMapping = requireNonNull(issuerAudienceMapping, "issuerAudienceMapping is null");
        checkArgument(issuerAudienceMapping.size() > 0, "issuerAudienceMapping requires at least 1 issuer");
        this.jwtClaimsParser = requireNonNull(jwtClaimsParser, "jwtClaimsParser is null");
    }

    protected IdentityParams commonAuthenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        Claims claims = jwtClaimsParser.claims(token);
        String username = claims.get("username", String.class);
        if (username == null) {
            throw new AuthenticationException("Invalid username", "Galaxy");
        }
        AccountId accountId;
        try {
            accountId = new AccountId(getOnlyElement(claims.getAudience()));
        }
        catch (IllegalArgumentException e) {
            throw new AuthenticationException("Invalid audience", "Galaxy");
        }
        UserId userId = new UserId(requireNonNull(claims.get("user_id", String.class), "userId is null"));
        RoleId roleId = new RoleId(requireNonNull(claims.get("role_id", String.class), "roleId is null"));
        if (claims.getIssuer() == null) {
            String msg = String.format(
                    MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                    ISSUER,
                    issuerAudienceMapping.keySet());
            throw new AuthenticationException(msg);
        }
        Set<String> audiences = issuerAudienceMapping.get(claims.getIssuer());
        if (audiences == null) {
            String msg = String.format(
                    INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                    ISSUER,
                    issuerAudienceMapping.keySet(),
                    claims.getIssuer());
            throw new AuthenticationException(msg, "Galaxy");
        }
        else {
            if (!audiences.isEmpty()) {
                if (claims.getAudience() == null || claims.getAudience().isEmpty()) {
                    String msg = String.format(
                            MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                            AUDIENCE,
                            audiences);
                    throw new AuthenticationException(msg, "Galaxy");
                }
                if (!audiences.contains(getOnlyElement(claims.getAudience()))) {
                    String msg = String.format(
                            INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                            AUDIENCE,
                            audiences,
                            claims.getAudience());
                    throw new AuthenticationException(msg, "Galaxy");
                }
            }
        }
        if (requestBodyHashing.isPresent()) {
            if (!claims.containsKey(REQUEST_EXPIRATION_CLAIM) || now().isAfter(claims.get(REQUEST_EXPIRATION_CLAIM, Date.class).toInstant())) {
                String expiration = Optional.ofNullable(claims.get(REQUEST_EXPIRATION_CLAIM, Date.class)).map(Date::toInstant).map(Instant::toString).orElse("null");
                log.error("Attempt to use expired (as of %s) token for user: %s %s %s", expiration, accountId, userId, roleId);
                throw new AuthenticationException("Token expired", "Galaxy");
            }

            String requestHash = requestBodyHashing.get().hash();
            if (!requestHash.equals(claims.get(requestBodyHashing.get().claimName()))) {
                log.error("Request body hash does not match for user: %s %s %s", accountId, userId, roleId);
                throw new AuthenticationException("Request body hash does not match", "Galaxy");
            }
        }

        GalaxyIdentity.GalaxyIdentityType identityType = toGalaxyIdentityType(claims.get("identity_type", String.class));
        Optional<EmbeddedActiveRoleSet> enabledRoles = Optional.empty();
        String enabledRolesString = claims.get("enabled_roles", String.class);
        if (enabledRolesString != null) {
            enabledRoles = Optional.of(parseEmbeddedEnabledRoles(enabledRolesString));
        }
        return new IdentityParams(username, accountId, userId, roleId, enabledRoles, identityType, claims);
    }
}
