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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.OwnerEntityAndPrivilegeInfo;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.server.starburst.accesscontrol.GalaxySystemAccessControlConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.sql.tree.Node;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.spi.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.trino.spi.StandardErrorCode.MISSING_SCHEMA_NAME;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class EntityPropertyManager
        implements EntityPropertyManagerApi
{
    private final TrinoSecurityApi accessControlClient;
    private final GalaxySystemAccessControlConfig accessControlConfig;

    @Inject
    public EntityPropertyManager(TrinoSecurityApi accessControlClient, GalaxySystemAccessControlConfig accessControlConfig)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.accessControlConfig = requireNonNull(accessControlConfig, "accessControlConfig is null");
    }

    @Override
    public List<DisplayedGrant> getPrivilegesForShowGrants(SystemSecurityContext context, EntityKind entityKind, List<String> qualifiedEntityNameParts)
    {
        if (!isEnabled()) {
            return ImmutableList.of();
        }
        DispatchSession session = toDispatchSession(context.getIdentity());
        OwnerEntityAndPrivilegeInfo privileges = accessControlClient.getPrivilegesForShowGrants(session, session.roleId(), entityKind, qualifiedEntityNameParts);
        return privileges.privilegeInfo().stream()
                .map(grant -> new DisplayedGrant(
                        entityKind,
                        grant.grantNameParts(),
                        grant.grantKind(),
                        privileges.owner(),
                        privileges.isExplicitOwner(),
                        grant.grantee(),
                        grant.privilege(),
                        grant.grantOption()))
                .collect(toImmutableList());
    }

    @Override
    public boolean isEnabled()
    {
        return accessControlConfig.isGalaxyEntityPrivilegesEnabled();
    }

    public static List<String> fillInMissingNameElements(Session session, Node sqlNode, EntityKind entityKind, List<String> parts)
    {
        return fillInMissingNameElements(session, Optional.of(sqlNode), entityKind, parts);
    }

    public static List<String> fillInMissingNameElements(Session session, EntityKind entityKind, List<String> parts)
    {
        return fillInMissingNameElements(session, Optional.empty(), entityKind, parts);
    }

    private static List<String> fillInMissingNameElements(Session session, Optional<Node> node, EntityKind entityKind, List<String> parts)
    {
        return switch (entityKind) {
            case CATALOG, CLUSTER, COLUMN_MASK, DATA_PRODUCT, LOCATION, POLICY, ROLE, ROW_FILTER, TAG, USER -> parts;
            case SCHEMA -> switch (parts.size()) {
                case 1 -> ImmutableList.of(getCatalog(session, node), parts.get(0));
                case 2 -> parts;
                default -> throw new TrinoException(GENERIC_USER_ERROR, "Illegal schema name %s".formatted(dottedName(parts)));
            };
            case TABLE -> switch (parts.size()) {
                case 1 -> ImmutableList.of(getCatalog(session, node), getSchema(session, node), parts.get(0));
                case 2 -> ImmutableList.of(getCatalog(session, node), parts.get(0), parts.get(1));
                case 3 -> parts;
                default -> throw new TrinoException(GENERIC_USER_ERROR, "Illegal table name %s".formatted(dottedName(parts)));
            };
            case FUNCTION -> switch (parts.size()) {
                case 1 -> ImmutableList.of("galaxy", "functions", parts.get(0));
                case 2 -> ImmutableList.of("galaxy", parts.get(0), parts.get(1));
                case 3 -> parts;
                default -> throw new TrinoException(GENERIC_USER_ERROR, "Illegal function name %s".formatted(dottedName(parts)));
            };
            case COLUMN -> switch (parts.size()) {
                case 2 -> ImmutableList.of(getCatalog(session, node), getSchema(session, node), parts.get(0), parts.get(1));
                case 3 -> ImmutableList.of(getCatalog(session, node), parts.get(0), parts.get(1), parts.get(2));
                case 4 -> ImmutableList.of(parts.get(0), parts.get(1), parts.get(2), parts.get(3));
                default -> throw new TrinoException(GENERIC_USER_ERROR, "Illegal column name %s".formatted(dottedName(parts)));
            };
            case ACCOUNT -> {
                if (parts.size() != 1 || !"ACCOUNT".equalsIgnoreCase(parts.get(0))) {
                    throw new TrinoException(INVALID_PRIVILEGE, "Account privileges must have target name of 'ACCOUNT'");
                }
                yield ImmutableList.of(toDispatchSession(session).getAccountId().getBaseEntityIdString());
            }
            default -> throw new UnsupportedOperationException("Granting privileges on entity kind %s is not supported".formatted(entityKind));
        };
    }

    private static String getSchema(Session session, Optional<Node> node)
    {
        return session.getSchema().orElseThrow(() -> {
            if (node.isPresent()) {
                return semanticException(MISSING_SCHEMA_NAME, node.orElseThrow(), "Schema must be specified when session schema is not set");
            }
            return new TrinoException(MISSING_SCHEMA_NAME, "Schema must be specified when session schema is not set");
        });
    }

    private static String getCatalog(Session session, Optional<Node> node)
    {
        return session.getCatalog().orElseThrow(() -> {
            if (node.isPresent()) {
                return semanticException(MISSING_CATALOG_NAME, node.orElseThrow(), "Catalog must be specified when session catalog is not set");
            }
            return new TrinoException(MISSING_CATALOG_NAME, "Catalog must be specified when session catalog is not set");
        });
    }

    private static String dottedName(List<String> parts)
    {
        return String.join(".", parts);
    }
}
