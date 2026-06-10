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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class GalaxyIdentity
{
    public static final Logger log = Logger.get(GalaxyIdentity.class);

    private static final String GALAXY_TOKEN_CREDENTIAL = "GalaxyTokenCredential";
    private static final GalaxyIdentityCrypto crypto = new GalaxyIdentityCrypto();

    private static final String GALAXY_VIEW_OWNER_USER_NAME_TEMPLATE = "<galaxy role %s>";
    private static final Pattern GALAXY_USER_STRING_IDENTITY_MATCHER = Pattern.compile("<galaxy(:[^:]+){4}>");
    private static final Splitter PRINCIPAL_SPLITTER = Splitter.on(':').limit(5);
    public static final String PORTAL_IDENTITY_TYPE = "galaxy_portal";
    public static final String INDEXER_IDENTITY_TYPE = "galaxy_indexer";
    public static final String DISPATCH_IDENTITY_TYPE = "galaxy_dispatch";

    private GalaxyIdentity() {}

    public enum GalaxyIdentityType
    {
        PORTAL(PORTAL_IDENTITY_TYPE),
        INDEXER(INDEXER_IDENTITY_TYPE),
        DISPATCH(DISPATCH_IDENTITY_TYPE);

        private final String type;

        public String type()
        {
            return type;
        }

        GalaxyIdentityType(String type)
        {
            this.type = requireNonNull(type, "type is null");
        }
    }

    public record EmbeddedActiveRoleSet(
            RoleId forRoleId,
            List<RoleName> activeRoleSet)
    {
        public EmbeddedActiveRoleSet
        {
            requireNonNull(forRoleId, "forRoleId is null");
            requireNonNull(activeRoleSet, "enabledRoles is null");
            checkArgument(!activeRoleSet.isEmpty(), "enabledRoles is empty");
        }

        public String toExtraCredentials()
        {
            String enabledRolesString = activeRoleSet.stream()
                    .map(RoleName::getName)
                    .collect(joining(","));
            return "%s:%s".formatted(forRoleId, enabledRolesString);
        }
    }

    public static GalaxyIdentityType toGalaxyIdentityType(String value)
    {
        if (value == null) {
            return GalaxyIdentityType.PORTAL;
        }
        return switch (value) {
            case INDEXER_IDENTITY_TYPE -> GalaxyIdentityType.INDEXER;
            case DISPATCH_IDENTITY_TYPE -> GalaxyIdentityType.DISPATCH;
            default -> GalaxyIdentityType.PORTAL;
        };
    }

    public static GalaxyIdentityType getGalaxyIdentityType(Identity identity)
    {
        String identityType = identity.getExtraCredentials().get("identityType");
        try {
            return GalaxyIdentityType.valueOf(identityType);
        }
        catch (IllegalArgumentException e) {
            // ignore
        }
        return GalaxyIdentityType.PORTAL;
    }

    public static Identity createIdentity(
            String username,
            AccountId accountId,
            UserId userId,
            RoleId roleId,
            Optional<EmbeddedActiveRoleSet> enabledRoles,
            String token,
            GalaxyIdentityType identityType)
    {
        ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.<String, String>builder()
                .put("accountId", accountId.toString())
                .put("userId", userId.toString())
                .put("roleId", roleId.toString())
                .put("identityType", identityType.name())
                .put(GALAXY_TOKEN_CREDENTIAL, token);
        enabledRoles.ifPresent(roles -> extraCredentialsBuilder.put("enabledRoles", roles.toExtraCredentials()));
        return Identity.forUser(username)
                .withPrincipal(createPrincipal(accountId, userId, roleId))
                .withExtraCredentials(extraCredentialsBuilder.buildOrThrow())
                .build();
    }

    public static Identity createIdentity(String username, AccountId accountId, UserId userId, RoleId roleId, Set<String> enabledRoles, String token, GalaxyIdentityType identityType)
    {
        return Identity.forUser(username)
                .withPrincipal(createPrincipal(accountId, userId, roleId))
                .withExtraCredentials(Map.of(
                        "accountId", accountId.toString(),
                        "userId", userId.toString(),
                        "roleId", roleId.toString(),
                        "identityType", identityType.name(),
                        GALAXY_TOKEN_CREDENTIAL, token))
                .withEnabledRoles(enabledRoles)
                .build();
    }

    private static BasicPrincipal createPrincipal(AccountId accountId, UserId userId, RoleId roleId)
    {
        return new BasicPrincipal(createPrincipalString(accountId, userId, roleId));
    }

    @VisibleForTesting
    public static String createPrincipalString(AccountId accountId, UserId userId, RoleId roleId)
    {
        return format("galaxy:%s:%s:%s", accountId, userId, roleId);
    }

    public static RoleId getRoleId(Identity identity)
    {
        return new RoleId(splitPrincipal(identity).get(3));
    }

    public static DispatchSession toDispatchSession(Session session)
    {
        return toDispatchSession(session.getIdentity());
    }

    public static RoleId getContextRoleId(Identity identity)
    {
        String roleIdString;
        if (isIdentityEncodedInTheUserString(identity)) {
            roleIdString = PRINCIPAL_SPLITTER.splitToList(identity.getUser()).get(3);
        }
        else {
            roleIdString = identity.getExtraCredentials().get("roleId");
            if (roleIdString == null) {
                roleIdString = splitPrincipal(identity).get(3);
            }
        }
        return new RoleId(roleIdString);
    }

    public static Optional<Set<String>> maybeGetEmbeddedEnabledRoles(Identity identity)
    {
        String enabledRolesString = identity.getExtraCredentials().get("enabledRoles");
        if (enabledRolesString == null) {
            return Optional.empty();
        }
        RoleId roleId = getContextRoleId(identity);
        EmbeddedActiveRoleSet embeddedEnabledRoles = parseEmbeddedEnabledRoles(enabledRolesString);

        if (!roleId.equals(embeddedEnabledRoles.forRoleId())) {
            return Optional.empty();
        }

        Set<String> enabledRoles = embeddedEnabledRoles.activeRoleSet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());
        return Optional.of(enabledRoles);
    }

    public static EmbeddedActiveRoleSet parseEmbeddedEnabledRoles(String enabledRolesString)
    {
        String[] rolesArray = enabledRolesString.split(":");
        if (rolesArray.length != 2) {
            log.error("Poorly formatted enabled roles string. Expected format <roleId>:<role1,role2,...> but got %s".formatted(enabledRolesString));
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "There was an internal error processing roles. Please contact support.");
        }
        RoleId enabledRolesRoleId = new RoleId(rolesArray[0]);
        List<RoleName> enabledRoles = Splitter.on(',')
                .splitToStream(rolesArray[1])
                .map(String::trim)
                .map(RoleName::new)
                .collect(toImmutableList());
        if (enabledRoles.isEmpty()) {
            log.warn("For enabled roles extra credential %s, only found roles %s. public, at a minimum, should always be enabled.".formatted(enabledRolesString, enabledRoles));
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "There was an internal error processing roles. Please contact support.");
        }
        return new EmbeddedActiveRoleSet(enabledRolesRoleId, enabledRoles);
    }

    public static DispatchSession toDispatchSession(Identity identity)
    {
        if (isIdentityEncodedInTheUserString(identity)) {
            return toDispatchSessionFromUserString(identity.getUser());
        }
        return toDispatchSessionFromPrincipal(identity);
    }

    public static DispatchSession toDispatchSessionFromPrincipal(Identity identity)
    {
        List<String> parts = splitPrincipal(identity);
        return new DispatchSession(
                new AccountId(parts.get(1)),
                new UserId(parts.get(2)),
                new RoleId(parts.get(3)),
                getGalaxyToken(identity));
    }

    private static DispatchSession toDispatchSessionFromUserString(String user)
    {
        List<String> parts = PRINCIPAL_SPLITTER.splitToList(user.substring(1, user.length() - 1));
        return new DispatchSession(
                new AccountId(parts.get(1)),
                new UserId(parts.get(2)),
                new RoleId(parts.get(3)),
                crypto.decryptToUtf8(parts.get(4)));
    }

    public static AccountId toAccountId(Identity identity)
    {
        return toDispatchSession(identity).getAccountId();
    }

    public static Identity createViewOrFunctionOwnerIdentity(Identity identity, RoleName viewOwnerRoleName, RoleId viewOwnerId)
    {
        return Identity.from(identity)
                .withUser(GALAXY_VIEW_OWNER_USER_NAME_TEMPLATE.formatted(viewOwnerId))
                .withEnabledRoles(ImmutableSet.of(viewOwnerRoleName.getName()))
                .withAdditionalExtraCredentials(ImmutableMap.<String, String>builder()
                        .put("roleId", viewOwnerId.toString())
                        .buildOrThrow())
                .build();
    }

    public static Optional<String> getRowFilterAndColumnMaskUserString(Identity identity, RoleId owningRoleId)
    {
        if (identity.getPrincipal().isPresent()) {
            String name = identity.getPrincipal().get().getName();
            if (name.startsWith("galaxy:")) {
                List<String> parts = PRINCIPAL_SPLITTER.splitToList(name);
                if (parts.size() == 4) {
                    return Optional.of("<galaxy:%s:%s:%s:%s>".formatted(parts.get(1), parts.get(2), owningRoleId, crypto.encryptFromUtf8(getGalaxyToken(identity))));
                }
            }
        }
        return Optional.empty();
    }

    private static String getGalaxyToken(Identity identity)
    {
        return requireNonNull(identity.getExtraCredentials().get(GALAXY_TOKEN_CREDENTIAL), "token is null");
    }

    public static boolean isDelegateIdentityEncoded(Identity identity)
    {
        return isIdentityEncodedInTheUserString(identity) || isViewOwnerIdentity(identity);
    }

    private static boolean isIdentityEncodedInTheUserString(Identity identity)
    {
        return GALAXY_USER_STRING_IDENTITY_MATCHER.matcher(identity.getUser()).matches();
    }

    public static boolean isViewOwnerIdentity(Identity identity)
    {
        return identity.getUser().startsWith("<galaxy role ");
    }

    private static List<String> splitPrincipal(Identity identity)
    {
        String principal = identity.getPrincipal().orElseThrow(() -> new IllegalArgumentException("Identity does not contain a principal: " + identity)).toString();
        List<String> parts = PRINCIPAL_SPLITTER.splitToList(principal);
        if (parts.size() != 4 || !parts.get(0).equals("galaxy")) {
            throw new IllegalArgumentException("Invalid Galaxy principal: " + principal);
        }
        return parts;
    }
}
