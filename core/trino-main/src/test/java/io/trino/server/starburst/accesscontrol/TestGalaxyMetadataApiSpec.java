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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.TableGrant;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestUser;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GalaxyPrivilegeInfo;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.server.starburst.security.GalaxyIdentity;
import io.trino.server.starburst.security.GalaxyTestHelper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.DENY;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_ROLE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_SCHEMA;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.DELETE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.INSERT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.MANAGE_SECURITY;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.UPDATE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.getEntityKindPrivileges;
import static io.starburst.stargate.id.EntityKind.COLUMN;
import static io.starburst.stargate.id.EntityKind.SCHEMA;
import static io.starburst.stargate.id.EntityKind.TABLE;
import static io.trino.server.starburst.accesscontrol.GalaxySecurityMetadata.PRIVILEGE_TRANSLATIONS;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.security.Privilege.CREATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public abstract class TestGalaxyMetadataApiSpec
{
    private static final Logger log = Logger.get(TestGalaxyMetadataApiSpec.class);

    private static final String ACCOUNT_ADMIN = "accountadmin";
    private static final RoleName ACCOUNT_ADMIN_ROLE = new RoleName(ACCOUNT_ADMIN);
    private static final String PUBLIC = "public";
    private static final String FEARLESS_LEADER = "fearless_leader";
    private static final String LACKEY_FOLLOWER = "lackey_follower";

    private static final AtomicInteger ROLE_COUNTER = new AtomicInteger(1);
    private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(1);
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(1);
    private static final AtomicInteger COLUMN_COUNTER = new AtomicInteger(1);
    private static final List<Boolean> TRUE_AND_FALSE = ImmutableList.of(true, false);
    private static final List<Boolean> JUST_FALSE = ImmutableList.of(false);

    private GalaxyTestHelper helper;
    private GalaxyAccessControl accessControl;
    private GalaxySecurityMetadata securityApi;
    private TrinoSecurityApi client;
    private TestingAccountClient accountClient;
    private RoleId fearlessRoleId;
    private RoleId lackeyRoleId;
    private Set<String> publicCatalogNames;
    private List<String> catalogNames;
    private String adminEmail;

    private AccountId accountId;

    private RoleId adminRoleId;
    private RoleId publicRoleId;
    protected boolean enableSharedCoordinatorCache;

    @BeforeAll
    public void initialize()
            throws Exception
    {
        helper = new GalaxyTestHelper();
        // Enable BRANCH_PRIVILEGES so that entity creation includes the CREATE_BRANCH privilege. This must be set as part
        // of initialize(), before any access-control-server request caches the account's feature flags (see GalaxyTestHelper).
        helper.initialize(enableSharedCoordinatorCache, ImmutableSet.of("BRANCH_PRIVILEGES"));
        accountClient = helper.getAccountClient();
        accountId = accountClient.getAccountId();
        adminRoleId = accountClient.getAdminRoleId();
        publicRoleId = accountClient.getPublicRoleId();
        adminEmail = accountClient.getAdminEmail();
        accessControl = helper.getAccessControl();
        securityApi = helper.getMetadataApi();
        client = helper.getClient();
        Map<RoleName, RoleId> roles = helper.getAccessController(adminIdentity()).listEnabledRoles(adminIdentity());
        fearlessRoleId = requireNonNull(roles.get(new RoleName(FEARLESS_LEADER)), "Didn't find fearless_leader");
        lackeyRoleId = requireNonNull(roles.get(new RoleName(LACKEY_FOLLOWER)), "Didn't find lackey_follower");
        publicCatalogNames = accessControl.filterCatalogs(publicContext(), helper.getAccountClient().getAllCatalogNames());
        catalogNames = helper.getCatalogResolver().getCatalogNames().stream().sorted().collect(toImmutableList());
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (helper != null) {
            helper.close();
        }
        helper = null;
        accountClient = null;
        accountId = null;
        adminRoleId = null;
        publicRoleId = null;
        adminEmail = null;
        accessControl = null;
        securityApi = null;
        client = null;
        fearlessRoleId = null;
        lackeyRoleId = null;
        publicCatalogNames = null;
        catalogNames = null;
    }

    @AfterEach
    public void verifyCleanup()
    {
        assertThat(securityApi.listRoles(adminSession()))
                .isEqualTo(ImmutableSet.of("accountadmin", "fearless_leader", "lackey_follower", "public"));
    }

    /**
     * Map&lt;RoleName, RoleId&gt; listEnabledRoles(DispatchSession session)
     * <p>
     * Gets all roles enabled in the current session. This set includes the primary
     * role of the session and all roles granted to that role transitively. Since PUBLIC
     * is granted to all roles the PUBLIC role will always be contained in this set.
     * <p>
     * Security: none - all role names are public in Galaxy
     */
    @Test
    public void testListEnabledRoles()
    {
        // All roles return public
        assertThat(securityApi.listEnabledRoles(publicIdentity())).isEqualTo(ImmutableSet.of(PUBLIC));

        // lackey_follower returns itself and public
        assertThat(securityApi.listEnabledRoles(lackeyIdentity())).isEqualTo(ImmutableSet.of(LACKEY_FOLLOWER, PUBLIC));

        // fearless_leader is granted lackey_follower
        assertThat(securityApi.listEnabledRoles(fearlessIdentity())).isEqualTo(ImmutableSet.of(FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC));

        // Finally, admin is granted fearless_leader
        assertThat(securityApi.listEnabledRoles(adminIdentity())).isEqualTo(ImmutableSet.of(ACCOUNT_ADMIN, FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC));
    }

    /**
     * Map&lt;RoleName, RoleId&gt; listRoles(DispatchSession session)
     *
     * Gets all roles in the current account.  This set will always contain the PUBLIC
     * role, but will not contain the _SYSTEM role
     * <p>
     * Security: none - all role names are public in Galaxy
     */
    @Test
    public void testListAllRoles()
    {
        ImmutableSet.of(adminRoleId, fearlessRoleId, lackeyRoleId, publicRoleId).forEach(roleId ->
                assertThat(securityApi.listRoles(fromRole(roleId)))
                        .isEqualTo(ImmutableSet.of(ACCOUNT_ADMIN, FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC)));
    }

    /**
     * boolean roleExists(DispatchSession session, RoleName role)
     *
     * Does the specified role exist?
     * <p>
     * Security: none - all role names are public in Galaxy
     */
    @Test
    public void testRoleExists()
    {
        for (RoleId roleId : ImmutableSet.of(adminRoleId, fearlessRoleId, lackeyRoleId, publicRoleId)) {
            for (String roleName : ImmutableSet.of(ACCOUNT_ADMIN, FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC)) {
                assertThat(securityApi.roleExists(fromRole(roleId), roleName)).isTrue();
            }
        }
    }

    /**
     * void createRole(DispatchSession session, RoleName role)
     * <p>
     * Creates the specified role.
     * <p>
     * Security: CREATE_ROLE or MANAGE_SECURITY
     */
    @Test
    public void testCreateRole()
    {
        // Illegal role names
        for (String name : ImmutableList.of("my-role", "my role", "my#role", "my$role", "interminably_long_excessively_long_disgustingly_comically_long_role")) {
            assertThatThrownBy(() -> securityApi.createRole(admin(), name, Optional.empty()))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("Invalid role name " + name + ". The role name must be between 1 and 64 characters, may contain only lowercase latin characters (a-z), numbers (0-9) and underscores (_).");
        }

        // Test role with grantor
        assertThatThrownBy(() -> securityApi.createRole(admin(), "impossible_role", Optional.of(new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Galaxy does not support creating a role with an explicit grantor");

        // Test role already exists
        assertThatThrownBy(() -> securityApi.createRole(admin(), FEARLESS_LEADER, Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Role already exists: %s".formatted(FEARLESS_LEADER));

        withNewRoleAndPrincipal(adminRoleId, (roleName, roleId, rolePrincipal) -> {
            String newRoleName = makeRoleName();

            // A role with no privileges can't create a role
            assertThatThrownBy(() -> securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty()))
                    .cause()
                    .isInstanceOf(OperationNotAllowedException.class)
                    .hasMessage("Operation not allowed: CREATE_ROLE");

            // If that role is granted MANAGE_SECURITY, it can create a new role
            withAccountPrivilege(MANAGE_SECURITY, ALLOW, roleName, true, () -> {
                securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty());

                // Check that the newRoleName was granted to roleName
                assertThat(securityApi.listRoleGrants(admin(), rolePrincipal))
                        .containsOnly(new RoleGrant(rolePrincipal, newRoleName, true), publicGrant(roleName));

                // That role case also drop the new role
                securityApi.dropRole(fromRole(roleId), newRoleName);
            });

            // If that role is granted CREATE_ROLE, it can create a new role
            withAccountPrivilege(CREATE_ROLE, ALLOW, roleName, true, () -> {
                securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty());
                // That role case also drop the new role
                securityApi.dropRole(fromRole(roleId), newRoleName);
            });

            // If that role is granted CREATE_ROLE, and the grant is removed, the granted
            // role can still drop newRoleName because it owns that role
            withAccountPrivilege(CREATE_ROLE, ALLOW, roleName, true, () ->
                    securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty()));
            securityApi.dropRole(fromRole(roleId), newRoleName);
        });
    }

    /**
     * void dropRole(DispatchSession session, RoleName role)
     * <p>
     * Drops the specified role. A role that owns an entity can not be removed.
     * Any grants for the role, or privileges granted to the role will be removed.
     * <p>
     * Security: owner of role or MANAGE_SECURITY
     */
    @Test
    public void testDropRole()
    {
        // Dropping an unknown role results in an exception
        assertThatThrownBy(() -> securityApi.dropRole(admin(), "unrecognized_role"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Role not found: unrecognized_role");

        // A role without ownership of MANAGE_SECURITY can't drop
        withNewRole(adminRoleId, (_, roleId) -> {
            String newRoleName = makeRoleName();
            securityApi.createRole(admin(), newRoleName, Optional.empty());

            // The outer role can't drop the new one, because it has no relationship with the new one.
            assertThatThrownBy(() -> securityApi.dropRole(fromRole(roleId), newRoleName))
                    .cause()
                    .isInstanceOf(OperationNotAllowedException.class)
                    .hasMessage("Operation not allowed: DELETE_ROLE");
            securityApi.dropRole(admin(), newRoleName);
        });

        // Test owner can drop
        withNewRole(adminRoleId, (roleName, roleId) -> {
            String newRoleName = makeRoleName();
            withAccountPrivilege(CREATE_ROLE, ALLOW, roleName, true, () ->
                    securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty()));

            // The outer role can drop the new one because it owns the new one
            securityApi.dropRole(fromRole(roleId), newRoleName);
        });

        // Test MANAGE_SECURITY privilege can drop
        withNewRole(adminRoleId, (roleName, roleId) -> {
            String newRoleName = makeRoleName();
            withAccountPrivilege(MANAGE_SECURITY, ALLOW, roleName, true, () -> {
                securityApi.createRole(fromRole(roleId), newRoleName, Optional.empty());
                RoleId newRoleId = roleIdFromName(newRoleName);
                // Remove ownership
                setEntityOwner(adminRoleId, adminRoleId, newRoleId);

                // The outer role can drop the new one because it has MANAGE_SECURITY even though it no
                // longer owns the new one
                securityApi.dropRole(fromRole(roleId), newRoleName);
            });
        });

        // Test that dropping a role removes role-to-role grants
        withNewRoleAndPrincipal(adminRoleId, (roleName, _, rolePrincipal) -> {
            String newRoleName = makeRoleName();
            securityApi.createRole(admin(), newRoleName, Optional.empty());
            securityApi.grantRoles(admin(), ImmutableSet.of(newRoleName), ImmutableSet.of(rolePrincipal), false, Optional.empty());
            assertThat(securityApi.listRoleGrants(admin(), rolePrincipal)).containsOnly(
                    new RoleGrant(rolePrincipal, newRoleName, false),
                    publicGrant(roleName));

            // Drop the granted role and show that the role grant is gone.
            securityApi.dropRole(admin(), newRoleName);
            assertThat(securityApi.listRoleGrants(admin(), rolePrincipal)).containsOnly(publicGrant(roleName));
        });

        // Test that dropping a role removes user role grants
        withNewRole(adminRoleId, (_, _) -> {
            String newRoleName = makeRoleName();
            securityApi.createRole(admin(), newRoleName, Optional.empty());
            withNewUserGrantee(adminRoleId, grantee -> {
                securityApi.grantRoles(admin(), ImmutableSet.of(newRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                assertThat(securityApi.listRoleGrants(admin(), grantee))
                        .contains(new RoleGrant(grantee, newRoleName, false));

                // Drop the granted role and show that the role grant is gone.
                securityApi.dropRole(admin(), newRoleName);
                assertThat(securityApi.listRoleGrants(admin(), grantee))
                        .doesNotContain(new RoleGrant(grantee, newRoleName, false));
            });
        });
    }

    /**
     * Set&lt;RoleGrant&gt; listRoleGrants(DispatchSession session, GalaxyPrincipal principal, boolean transitive)
     * <p>
     * Gets roles grants for the specified principal. If transitive is set, the grants on
     * granted roles are also returned. If the principal is unknown, or the user does not
     * have permissions to view the grants, an empty set is returned.  This should always
     * include a grant for the PUBLIC role.  This will never include grants to _SYSTEM role
     * because that role can not be a grantee.
     * <p>
     * Security: owner of principal, current user is granted principal, or MANAGE_SECURITY <br>
     * This includes any role the current user has and not just the current role, because
     * this list is used by the user to choose a role to switch to.
     */
    @Test
    public void testListRoleGrants()
    {
        // Show that unrecognized role or user principals have no role grants, transitively or otherwise
        assertThat(securityApi.listRoleGrants(admin(), rolePrincipal("unrecognized_role"))).isEmpty();
        assertThat(securityApi.listRoleGrants(admin(), userPrincipal("unrecognized@silly.test"))).isEmpty();
        assertThat(securityApi.listApplicableRoles(admin(), rolePrincipal("unrecognized_role"))).isEmpty();
        assertThat(securityApi.listApplicableRoles(admin(), userPrincipal("unrecognized@silly.test"))).isEmpty();

        for (boolean grantOption : TRUE_AND_FALSE) {
            withNewRoleAndPrincipal(adminRoleId, (parentRoleName, parentRoleId, parentPrincipal) -> {
                withNewRoleAndPrincipal(adminRoleId, (childRoleName, _, childPrincipal) -> {
                    // Show that a role grant with an grantor raises an exception
                    assertThatThrownBy(() -> securityApi.grantRoles(admin(), ImmutableSet.of(childRoleName), ImmutableSet.of(parentPrincipal), grantOption, Optional.of(new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER))))
                            .isInstanceOf(TrinoException.class)
                            .hasMessage("Galaxy does not support GRANT with the GRANTED BY clause");

                    // Show that both of the new roles have only the public grant, transitively or otherwise
                    for (String roleName : ImmutableList.of(parentRoleName, childRoleName)) {
                        assertThat(securityApi.listRoleGrants(fromRole(parentRoleId), rolePrincipal(roleName)))
                                .containsOnly(publicGrant(roleName));
                        assertThat(securityApi.listApplicableRoles(fromRole(parentRoleId), rolePrincipal(roleName)))
                                .containsOnly(publicGrant(roleName));
                    }

                    // Show that after a role grant, listRoleGrants for a role principal returns the granted role and the public grant
                    securityApi.grantRoles(admin(), ImmutableSet.of(childRoleName), ImmutableSet.of(parentPrincipal), grantOption, Optional.empty());
                    assertThat(securityApi.listRoleGrants(fromRole(parentRoleId), parentPrincipal))
                            .containsOnly(new RoleGrant(parentPrincipal, childRoleName, grantOption), publicGrant(parentRoleName));

                    withNewRole(adminRoleId, (grandchildRoleName, _) -> {
                        // Grant grandchild to child
                        securityApi.grantRoles(admin(), ImmutableSet.of(grandchildRoleName), ImmutableSet.of(childPrincipal), grantOption, Optional.empty());

                        // Show that parent's direct grants haven't changed
                        assertThat(securityApi.listRoleGrants(fromRole(parentRoleId), parentPrincipal))
                                .containsOnly(new RoleGrant(parentPrincipal, childRoleName, grantOption), publicGrant(parentRoleName));

                        // But parent's transitive grants include child
                        assertThat(securityApi.listApplicableRoles(fromRole(parentRoleId), parentPrincipal)).containsOnly(
                                new RoleGrant(parentPrincipal, childRoleName, grantOption),
                                new RoleGrant(childPrincipal, grandchildRoleName, grantOption),
                                publicGrant(parentRoleName));
                    });

                    withNewUserGrantee(adminRoleId, userGrantee -> {
                        if (grantOption) {
                            assertThatThrownBy(() -> securityApi.grantRoles(admin(), ImmutableSet.of(parentRoleName), ImmutableSet.of(userGrantee), grantOption, Optional.empty()))
                                    .isInstanceOf(TrinoException.class)
                                    .hasMessage("Galaxy only supports a ROLE for GRANT with ADMIN OPTION");
                        }
                        else {
                            securityApi.grantRoles(admin(), ImmutableSet.of(parentRoleName), ImmutableSet.of(userGrantee), grantOption, Optional.empty());

                            // Non-transitive has only the direct grant and public
                            assertThat(securityApi.listRoleGrants(fromRole(parentRoleId), userGrantee))
                                    .containsOnly(
                                            new RoleGrant(userGrantee, parentRoleName, grantOption),
                                            new RoleGrant(userGrantee, PUBLIC, false));

                            // Transitive also include the childRole
                            assertThat(securityApi.listApplicableRoles(fromRole(parentRoleId), userGrantee))
                                    .containsOnly(
                                            new RoleGrant(userGrantee, parentRoleName, grantOption),
                                            new RoleGrant(parentPrincipal, childRoleName, grantOption),
                                            new RoleGrant(userGrantee, PUBLIC, false));
                        }
                    });
                });
            });
        }
    }

    /**
     * void grantRoles(DispatchSession session, Set&lt;CreateRoleGrant&gt; roleGrants)
     * <p>
     * Creates the specified role grants. If a grant already exists, it is ignored.  If
     * grant option is set, and a grant without grant option already exists, the grant is
     * upgraded to a grant with grant option. <br>
     * If the set contains multiple grants for the same pair (role, grantee) the grant
     * will fail.
     * <p>
     * Security: owner of target role, grant with admin option on role, or MANAGE_SECURITY
     */
    @Test
    public void testGrantRoles()
    {
        withNewRoleAndPrincipal(adminRoleId, (_, granteeRoleId, granteePrincipal) -> {
            withNewRole(adminRoleId, (grantedRoleName, grantedRoleId) -> {
                withNewUserGrantee(adminRoleId, userGrantee -> {
                    for (TrinoPrincipal grantee : ImmutableList.of(granteePrincipal, userGrantee)) {
                        // Create the grant
                        securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                        assertThat(securityApi.listRoleGrants(admin(), grantee)).containsOnly(
                                new RoleGrant(grantee, grantedRoleName, false),
                                new RoleGrant(grantee, PUBLIC, false));

                        // Verify that if a grant already exists, it is ignored
                        securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                        assertThat(securityApi.listRoleGrants(admin(), grantee)).containsOnly(
                                new RoleGrant(grantee, grantedRoleName, false),
                                new RoleGrant(grantee, PUBLIC, false));

                        // If the grantee is a role, verify that if the grantOption is false, a grant
                        // with grantOption of true changes the grant option
                        if (grantee.getType() == PrincipalType.ROLE) {
                            securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), true, Optional.empty());
                            assertThat(securityApi.listRoleGrants(admin(), grantee)).containsOnly(
                                    new RoleGrant(grantee, grantedRoleName, true),
                                    new RoleGrant(grantee, PUBLIC, false));
                        }
                        securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());

                        // Check privileges
                        withIsolatedUserAndRole(adminRoleId, (isolatedUserId, isolatedRoleName, isolatedRoleId, isolatedPrincipal) -> {
                            Session isolatedSession = fromUserAndRole(isolatedUserId, isolatedRoleId);
                            // With no privileges or ownership, granting is not allowed
                            assertThatThrownBy(() -> securityApi.grantRoles(isolatedSession, ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty()))
                                    .cause()
                                    .isInstanceOf(OperationNotAllowedException.class)
                                    .hasMessageMatching("Operation not allowed: User does not have privileges to grant role.*");

                            // With MANAGE_SECURITY, the grant succeeds
                            withAccountPrivilege(MANAGE_SECURITY, ALLOW, isolatedRoleName, false, () -> {
                                securityApi.grantRoles(fromRole(isolatedRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                                securityApi.revokeRoles(fromRole(isolatedRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                            });

                            if (grantee.getType() == PrincipalType.ROLE) {
                                securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), true, Optional.empty());

                                // With adminOption on the granted role, the grant succeeds
                                securityApi.grantRoles(fromRole(granteeRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(isolatedPrincipal), true, Optional.empty());

                                securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(isolatedPrincipal), false, Optional.empty());
                                securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(granteePrincipal), false, Optional.empty());
                            }

                            // With ownership on the granted role, the grant succeeds
                            withEntityOwnership(isolatedRoleId, grantedRoleId, () -> {
                                securityApi.grantRoles(fromRole(isolatedRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());
                                securityApi.revokeRoles(fromRole(isolatedRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(isolatedPrincipal), false, Optional.empty());
                            });
                        });
                    }
                });
            });
        });
    }

    /**
     * void revokeRoles(DispatchSession session, Set&lt;RoleGrant&gt; roleGrants)
     * <p>
     * Revokes the specified role grants. If a specified role grant has the grantable option set, only
     * the grant option will be removed; otherwise the entire grant is revoked.<br>
     * If the set contains multiple grants for the same pair (role, grantee) the grant
     * will fail. If any role or principal does not exist, all grants fail.
     * <p>
     * Security: owner of target role, grant with admin option on role, or MANAGE_SECURITY
     */
    @Test
    public void testRevokeRoles()
    {
        withNewRoleAndPrincipal(adminRoleId, (_, granteeRoleId, granteePrincipal) -> {
            withNewRole(adminRoleId, (grantedRoleName, _) -> {
                withIsolatedUserAndRole(adminRoleId, (otherUserId, otherRoleName, otherRoleId, otherPrincipal) -> {
                    Session isolcatedSession = fromUserAndRole(otherUserId, otherRoleId);
                    withNewUserGrantee(adminRoleId, userGrantee -> {
                        for (TrinoPrincipal grantee : ImmutableList.of(granteePrincipal, userGrantee)) {
                            securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());

                            // Show that the grant exists
                            assertThat(securityApi.listRoleGrants(fromRole(granteeRoleId), grantee))
                                    .containsOnly(new RoleGrant(grantee, grantedRoleName, false), new RoleGrant(grantee, PUBLIC, false));

                            // otherRole has no relation to grantedRole, so revoking the grant fails
                            assertThatThrownBy(() -> securityApi.revokeRoles(isolcatedSession, ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty()))
                                    .cause()
                                    .isInstanceOf(OperationNotAllowedException.class)
                                    .hasMessageMatching("Operation not allowed: User does not have privileges to revoke.*");

                            // If the role has MANAGE_SECURITY, it can revoke the role grant
                            withAccountPrivilege(MANAGE_SECURITY, ALLOW, otherRoleName, false, () ->
                                    securityApi.revokeRoles(fromRole(otherRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty()));

                            // Reinstate the original grant
                            securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty());

                            // The role has a grant without admin option, so it cannot revoke the original grant
                            assertThatThrownBy(() -> securityApi.revokeRoles(isolcatedSession, ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.empty()))
                                    .cause()
                                    .isInstanceOf(OperationNotAllowedException.class)
                                    .hasMessageMatching("Operation not allowed: User does not have privileges to revoke.*");

                            if (grantee.getType() == PrincipalType.ROLE) {
                                // If the role has a grant with admin option, it can revoke any other grant
                                securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(otherPrincipal), true, Optional.empty());
                                securityApi.revokeRoles(fromRole(otherRoleId), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), true, Optional.empty());

                                // Show that revoking with a grantor isn't allowed
                                assertThatThrownBy(() -> securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(grantee), false, Optional.of(rolePrincipal(FEARLESS_LEADER))))
                                        .isInstanceOf(TrinoException.class)
                                        .hasMessage("Galaxy does not support REVOKE with the GRANTED BY clause");

                                // If the role has a grant with admin option, revoking that grant leaves the record but changes adminOption to false
                                securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(otherPrincipal), true, Optional.empty());
                                assertThat(securityApi.listRoleGrants(fromRole(otherRoleId), otherPrincipal))
                                        .containsOnly(new RoleGrant(otherPrincipal, grantedRoleName, false), publicGrant(otherRoleName));
                            }

                            // Remove the grant
                            securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(otherPrincipal), false, Optional.empty());
                        }
                    });
                });
            });
        });
    }

    /**
     * EntityPrivileges getEntityPrivileges(DispatchSession session, EntityId entityId);
     * <p>
     * Gets the privileges granted to the current role on the specified entity.
     * <p>
     * Security: none - this only shows the permissions the current user has,
     * and the owner which is not private.
     */
    @Test
    public void testGetEntityPrivileges()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            // Test catalog privileges
            withNewCatalog(adminRoleId, (_, catalogId) -> {
                for (boolean grantOption : TRUE_AND_FALSE) {
                    // No privileges to start with
                    assertThat(client.getEntityPrivileges(dispatchSession(roleId), catalogId).getPrivileges()).isEmpty();

                    withGrantedPrivilege(adminRoleId, catalogId, CREATE_SCHEMA, ALLOW, roleName, grantOption, () ->
                            assertThat(client.getEntityPrivileges(dispatchSession(roleId), catalogId))
                                    .isEqualTo(new EntityPrivileges(ACCOUNT_ADMIN_ROLE, adminRoleId, true, false, ImmutableSet.of(new GalaxyPrivilegeInfo(CREATE_SCHEMA, grantOption)), ImmutableMap.of(), ImmutableList.of(), ImmutableMap.of(), true, ImmutableMap.of())));

                    // Test schema privileges
                    SchemaId schemaId = makeSchemaId(catalogId);
                    withGrantedPrivilege(adminRoleId, schemaId, CREATE_TABLE, ALLOW, roleName, grantOption, () ->
                            assertThat(client.getEntityPrivileges(dispatchSession(roleId), schemaId))
                                    .isEqualTo(new EntityPrivileges(ACCOUNT_ADMIN_ROLE, adminRoleId, false, false, ImmutableSet.of(new GalaxyPrivilegeInfo(CREATE_TABLE, grantOption)), ImmutableMap.of(), ImmutableList.of(), ImmutableMap.of(), true, ImmutableMap.of())));

                    // Test table privileges
                    TableId tableId = makeTableId(schemaId);
                    for (Privilege privilege : ImmutableSet.of(SELECT, INSERT, DELETE, UPDATE)) {
                        withGrantedPrivilege(adminRoleId, tableId, privilege, ALLOW, roleName, grantOption, () ->
                                assertThat(client.getEntityPrivileges(dispatchSession(roleId), tableId))
                                        .isEqualTo(new EntityPrivileges(
                                                ACCOUNT_ADMIN_ROLE,
                                                adminRoleId,
                                                false,
                                                false,
                                                ImmutableSet.of(
                                                        new GalaxyPrivilegeInfo(privilege, grantOption)),
                                                ImmutableMap.of(privilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of())),
                                                ImmutableList.of(),
                                                ImmutableMap.of(),
                                                true,
                                                ImmutableMap.of())));
                    }
                }
            });
        });
    }

    /**
     * void addEntityPrivileges(DispatchSession session, EntityId entityId, Set&lt;CreateEntityPrivilege&gt; privileges)
     * <p>
     * Add the specified privileges to the specified entity. If a privilege grant already
     * exists, the new grant updates the existing grant as follows:<br>
     * 1. The grant kind replaces the existing grant kind.<br>
     * 2. If the new grant is a DENY, the grant option is also removed.<br>.
     * 3. If the new grant is an ALLOW, the grant option of false does not remove an existing
     *    grant option, and a grant option of true replaces the existing grant option.
     * <p>
     * If the set contains multiple grants for the same pair (privilege, grantee) the add
     * will fail. If any role or principal does not exist, all grants fail.
     * <p>
     * Security: owner of entity, privilege grant with grant option on entity, or MANAGE_SECURITY <br>
     */
    @Test
    public void testAddEntityPrivileges()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            // Test catalog privileges
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                String schemaName = makeSchemaName();
                CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
                CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schemaName, makeTableName());
                QualifiedObjectName qualifiedTable = new QualifiedObjectName(catalogName, schemaName, table.getSchemaTableName().getTableName());
                TableId tableId = new TableId(catalogId, schemaName, table.getSchemaTableName().getTableName());
                ColumnId wildcardColumnId = new ColumnId(catalogId, tableId.getSchemaName(), tableId.getTableName(), "*");
                // No privileges to start with
                assertThat(client.getEntityPrivileges(dispatchSession(roleId), catalogId).getPrivileges()).isEmpty();

                // Make sure that admin is the owner in all cases
                securityApi.setSchemaOwner(admin(), schema, rolePrincipal(ACCOUNT_ADMIN));
                securityApi.setTableOwner(admin(), table, rolePrincipal(ACCOUNT_ADMIN));
                for (io.trino.spi.security.Privilege privilege : PRIVILEGE_TRANSLATIONS.keySet()) {
                    Privilege translatedPrivilege = PRIVILEGE_TRANSLATIONS.get(privilege);
                    if (translatedPrivilege == CREATE_TABLE) {
                        continue;
                    }
                    for (GrantKind grantKind : GrantKind.values()) {
                        for (boolean grantOption : grantKind == DENY ? JUST_FALSE : TRUE_AND_FALSE) {
                            Runnable checkNotAllowed = () -> {
                                // The new role has no rights to add entity privileges
                                assertThatThrownBy(() -> securityApi.grantTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), grantOption))
                                        .cause()
                                        .isInstanceOf(OperationNotAllowedException.class)
                                        .hasMessageMatching("Operation not allowed: User does not have the right to grant privileges.*");
                            };

                            // Not allowed to start with
                            checkNotAllowed.run();

                            // If the role is granted MANAGE_SECURITY, it can add the privilege
                            withAccountPrivilege(MANAGE_SECURITY, ALLOW, roleName, false, () -> {
                                // Add the privilege
                                grantOrDenyTablePrivileges(grantKind, roleId, roleName, qualifiedTable, ImmutableSet.of(privilege), grantOption);

                                // Show that it took - - can't use the TrinoSecurityApi because it doesn't return DENY privilege grants
                                assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, grantKind, grantOption, wildcardColumnId));
                                // Revoke the privilege
                                securityApi.revokeTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                            });

                            // Again not allowed
                            checkNotAllowed.run();

                            // If the role owns the entityId, it can add the privilege
                            withEntityOwnership(roleId, tableId, () -> {
                                // Add the privilege
                                grantOrDenyTablePrivileges(grantKind, roleId, roleName, qualifiedTable, ImmutableSet.of(privilege), grantOption);

                                // Revoke the privilege
                                securityApi.revokeTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                            });

                            // Again not allowed
                            checkNotAllowed.run();
                        }
                    }
                    // Explicitly check the special cases for existing privilege grants

                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);

                    // The grant kind replaces the existing grant kind, and if the new grant is a DENY, the grant option is also removed
                    securityApi.denyTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName));

                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(PRIVILEGE_TRANSLATIONS.get(privilege), roleId, DENY, false, wildcardColumnId));

                    //  If the new grant is an ALLOW, a grant option of true replaces the existing grant option.
                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, wildcardColumnId));

                    //  If the new grant is an ALLOW, the grant option of false does not remove an existing
                    //  grant option
                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, wildcardColumnId));

                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                }
            });
        });
    }

    @Test
    public void testSchemaPrivileges()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                String schemaName = makeSchemaName();
                CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
                SchemaId schemaId = new SchemaId(catalogId, schemaName);

                // A grant with a user principal fails
                assertThatThrownBy(() -> securityApi.grantSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Galaxy only supports a ROLE as a grantee");

                // Grant a schema privilege
                securityApi.grantSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName), true);
                assertThat(getEntityPrivileges(roleId, schemaId)).containsOnly(new GrantDetails(CREATE_TABLE, roleId, ALLOW, true, schemaId));

                // Turn it into a DENY
                securityApi.denySchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName));
                assertThat(getEntityPrivileges(roleId, schemaId)).containsOnly(new GrantDetails(CREATE_TABLE, roleId, DENY, false, schemaId));

                // A revoke with a user principal fails
                assertThatThrownBy(() -> securityApi.revokeSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Galaxy only supports a ROLE as a grantee");

                // Remove it
                securityApi.revokeSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName), false);
                assertThat(getEntityPrivileges(roleId, schemaId)).isEmpty();

                // Turn it back to ALLOW
                securityApi.grantSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName), true);
                assertThat(getEntityPrivileges(roleId, schemaId)).containsOnly(new GrantDetails(CREATE_TABLE, roleId, ALLOW, true, schemaId));

                // Revoke it with grantOption of true, meaning change the grantOtion of the privilege to false
                securityApi.revokeSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName), true);
                assertThat(getEntityPrivileges(roleId, schemaId)).containsOnly(new GrantDetails(CREATE_TABLE, roleId, ALLOW, false, schemaId));

                // Finally revoke with grantOption of false, meaning remove it
                securityApi.revokeSchemaPrivileges(adminSession(), schema, ImmutableSet.of(CREATE), rolePrincipal(roleName), false);
                assertThat(getEntityPrivileges(roleId, schemaId)).isEmpty();
            });
        });
    }

    /**
     * void revokeEntityPrivileges(DispatchSession session, EntityId entityId, Set&lt;RevokeEntityPrivilege&gt; privileges)
     * <p>
     * Removes the specified privileges from the specified entity. This removes an ALLOW
     * or DENY, but if grant option is set on the revoke only the grant option is revoked.
     * If grant option is set, only the grant option on an ALLOW is revoked (DENY can not
     * have a grant option).
     * <p>
     * If the set contains multiple grants for the same pair (privilege, grantee) the add
     * will fail. If any role or principal does not exist, all grants fail.
     * <p>
     * Security: owner of entity, privilege grant with grant option on entity, or MANAGE_SECURITY <br>
     */
    @Test
    public void testRevokeEntityPrivileges()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            // Test catalog privileges
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                String schemaName = makeSchemaName();
                String tableName = makeTableName();
                CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
                CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schemaName, tableName);
                QualifiedObjectName qualifiedTable = new QualifiedObjectName(catalogName, schemaName, tableName);
                TableId tableId = new TableId(catalogId, schemaName, tableName);
                ColumnId columnId = new ColumnId(catalogId, schemaName, tableName, "*");
                // No privileges to start with
                assertThat(client.getEntityPrivileges(dispatchSession(roleId), catalogId).getPrivileges()).isEmpty();

                // Show that a USER principal is not allowed for grant, deny or revoke
                assertThatThrownBy(() -> securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(io.trino.spi.security.Privilege.INSERT), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Galaxy only supports a ROLE as a grantee");

                assertThatThrownBy(() -> securityApi.denyTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(io.trino.spi.security.Privilege.INSERT), adminUserPrincipal()))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Galaxy only supports a ROLE as a grantee");

                assertThatThrownBy(() -> securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(io.trino.spi.security.Privilege.INSERT), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Galaxy only supports a ROLE as a grantee");

                // Show tha an unknown catalog name in a schema gives an exception for grant, deny or revoke
                CatalogSchemaName unknownSchema = new CatalogSchemaName("nonexistent_catalog", "any_schema");
                assertThatThrownBy(() -> securityApi.grantSchemaPrivileges(admin(), unknownSchema, ImmutableSet.of(CREATE), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Catalog 'nonexistent_catalog' does not exist");

                assertThatThrownBy(() -> securityApi.denySchemaPrivileges(admin(), unknownSchema, ImmutableSet.of(CREATE), adminUserPrincipal()))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Catalog 'nonexistent_catalog' does not exist");

                assertThatThrownBy(() -> securityApi.revokeSchemaPrivileges(admin(), unknownSchema, ImmutableSet.of(CREATE), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Catalog 'nonexistent_catalog' does not exist");

                // Show tha an unknown catalog name in a table gives an exception for grant, deny or revoke
                QualifiedObjectName unknownTable = new QualifiedObjectName("nonexistent_catalog", "any_schema", "any_table");
                assertThatThrownBy(() -> securityApi.grantTablePrivileges(admin(), unknownTable, ImmutableSet.of(io.trino.spi.security.Privilege.INSERT), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching("Catalog 'nonexistent_catalog' does not exist");

                assertThatThrownBy(() -> securityApi.denyTablePrivileges(admin(), unknownTable, ImmutableSet.of(CREATE), adminUserPrincipal()))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching("Catalog 'nonexistent_catalog' does not exist");

                assertThatThrownBy(() -> securityApi.revokeTablePrivileges(admin(), unknownTable, ImmutableSet.of(CREATE), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching("Catalog 'nonexistent_catalog' does not exist");

                // Show that you can't grant privileges to a schema or table in catalog "system"
                assertThatThrownBy(() -> securityApi.grantSchemaPrivileges(admin(), new CatalogSchemaName("system", "any_schema"), ImmutableSet.of(CREATE), rolePrincipal(FEARLESS_LEADER), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("System catalog is read-only");

                assertThatThrownBy(() -> securityApi.grantTablePrivileges(admin(), new QualifiedObjectName("system", "any_schema", "any_table"), ImmutableSet.of(io.trino.spi.security.Privilege.INSERT), adminUserPrincipal(), false))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching("System catalog is read-only");

                // Make sure that admin is the owner in all cases
                securityApi.setSchemaOwner(admin(), schema, rolePrincipal(ACCOUNT_ADMIN));
                securityApi.setTableOwner(admin(), table, rolePrincipal(ACCOUNT_ADMIN));
                for (io.trino.spi.security.Privilege privilege : PRIVILEGE_TRANSLATIONS.keySet()) {
                    Privilege translatedPrivilege = PRIVILEGE_TRANSLATIONS.get(privilege);
                    if (translatedPrivilege == CREATE_TABLE) {
                        continue;
                    }
                    Runnable checkNotAllowed = () -> {
                        // The new role has no rights to add entity privileges
                        assertThatThrownBy(() -> securityApi.revokeTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false))
                                .cause()
                                .isInstanceOf(OperationNotAllowedException.class)
                                .hasMessageMatching("Operation not allowed: User does not have the right to revoke privileges.*");
                    };

                    assertThat(getEntityPrivileges(roleId, tableId)).isEmpty();

                    // Add a grant and show that it took
                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                    assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, columnId));

                    // Revoking with grantOption true removes the grantOption but leaves the grant
                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                    assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, false, columnId));
                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);

                    // Whether grantOption is true or false in the original ALLOW grant, revoke with grantOption of false removes the grant
                    for (boolean grantOption : TRUE_AND_FALSE) {
                        securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), grantOption);
                        assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, grantOption, columnId));
                        securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                        assertThat(getEntityPrivileges(roleId, tableId)).isEmpty();
                    }

                    securityApi.denyTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName));
                    assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, DENY, false, columnId));

                    // Revoking a DENY privilege with revoke grantOption of true is a no-op
                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                    assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, DENY, false, columnId));

                    // Revoking a DENY privilege with revoke grantOption of false removes it
                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                    assertThat(getEntityPrivileges(roleId, tableId)).isEmpty();

                    // Privilege checks - - not allowed to start with
                    checkNotAllowed.run();

                    // If the role is granted MANAGE_SECURITY, it can add and revoke the privilege
                    withAccountPrivilege(MANAGE_SECURITY, ALLOW, roleName, false, () -> {
                        // Add the privilege
                        securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                        // Show that it took
                        assertThat(getEntityPrivileges(roleId, tableId)).containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, columnId));
                        // Revoke the privilege
                        securityApi.revokeTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                        // Show that it took
                        assertThat(getEntityPrivileges(roleId, tableId)).isEmpty();
                    });

                    // Again not allowed
                    checkNotAllowed.run();

                    // If the role owns the entityId, it can revoke the privilege
                    withEntityOwnership(roleId, tableId, () -> {
                        for (GrantKind grantKind : GrantKind.values()) {
                            // Add the privilege
                            switch (grantKind) {
                                case ALLOW -> securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                                case DENY -> securityApi.denyTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName));
                                default -> throw new IllegalArgumentException("Unrecognized grantKind " + grantKind);
                            }
                            // Revoke the privilege
                            securityApi.revokeTablePrivileges(fromRole(roleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                        }
                    });

                    // Again not allowed
                    checkNotAllowed.run();

                    // Explicitly check the special cases for existing privilege grants

                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);

                    // The grant kind replaces the existing grant kind, and if the new grant is a DENY, the grant option is also removed
                    securityApi.denyTablePrivileges(fromRole(adminRoleId), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName));
                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(translatedPrivilege, roleId, DENY, false, columnId));

                    //  If the new grant is an ALLOW, a grant option of true replaces the existing grant option.
                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), true);
                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, columnId));

                    //  If the new grant is an ALLOW, the grant option of false does not remove an existing
                    //  grant option
                    securityApi.grantTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                    assertThat(getEntityPrivileges(roleId, tableId))
                            .containsOnly(new GrantDetails(translatedPrivilege, roleId, ALLOW, true, columnId));

                    securityApi.revokeTablePrivileges(admin(), qualifiedTable, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                }
            });
        });
    }

    /**
     * List&lt;TableGrant&gt; listTableGrants(DispatchSession session, EntityId entity);
     * <p>
     * Gets the grants for all tables in the catalog, schema, or table, for the current
     * role.
     * <p>
     * Security: none - this only shows the permissions the current session has.
     */
    @Test
    public void testListTableGrants()
    {
        withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
            String schemaName1 = makeSchemaName();
            String schemaName2 = makeSchemaName();
            CatalogSchemaName schema1 = new CatalogSchemaName(catalogName, schemaName1);
            CatalogSchemaName schema2 = new CatalogSchemaName(catalogName, schemaName2);
            List<CatalogSchemaName> schemas = ImmutableList.of(schema1, schema2);
            String tableName1 = makeTableName();
            String tableName2 = makeTableName();
            QualifiedObjectName table1 = new QualifiedObjectName(catalogName, schemaName1, tableName1);
            QualifiedObjectName table2 = new QualifiedObjectName(catalogName, schemaName1, tableName2);
            String tableName3 = makeTableName();
            QualifiedObjectName table3 = new QualifiedObjectName(catalogName, schemaName2, tableName3);
            List<QualifiedObjectName> specificTables = ImmutableList.of(table1, table2, table3);
            List<QualifiedObjectName> tables = ImmutableList.of(
                    new QualifiedObjectName(catalogName, "*", "*"),
                    new QualifiedObjectName(catalogName, schemaName1, "*"),
                    new QualifiedObjectName(catalogName, schemaName2, "*"),
                    table1,
                    table2,
                    table3);

            assertThat(securityApi.listTablePrivileges(admin(), new QualifiedTablePrefix("system")))
                    .containsOnly(new GrantInfo(
                            new PrivilegeInfo(io.trino.spi.security.Privilege.SELECT, false),
                            rolePrincipal(PUBLIC),
                            schemaTableName("*", "*"),
                            Optional.empty(),
                            Optional.empty()));

            for (io.trino.spi.security.Privilege privilege : PRIVILEGE_TRANSLATIONS.keySet()) {
                Privilege translatedPrivilege = PRIVILEGE_TRANSLATIONS.get(privilege);
                if (translatedPrivilege == CREATE_TABLE) {
                    continue;
                }
                withNewRole(adminRoleId, (roleName, _) -> {
                    Set<TableGrant> catalogGrants = new HashSet<>();
                    SetMultimap<CatalogSchemaName, TableGrant> schemaGrants = HashMultimap.create();
                    SetMultimap<QualifiedObjectName, TableGrant> tableGrants = HashMultimap.create();
                    for (QualifiedObjectName table : tables) {
                        securityApi.grantTablePrivileges(admin(), table, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                        TableGrant grant = new TableGrant(translatedPrivilege, ALLOW, false, new RoleName(roleName), new TableId(catalogId, table.schemaName(), table.objectName()));
                        catalogGrants.add(grant);
                        String schemaName = table.schemaName();
                        if ("*".equals(schemaName)) {
                            for (CatalogSchemaName schema : schemas) {
                                schemaGrants.put(schema, grant);
                            }
                            for (QualifiedObjectName name : specificTables) {
                                tableGrants.put(name, grant);
                            }
                        }
                        else {
                            schemaGrants.put(new CatalogSchemaName(catalogName, schemaName), grant);
                            String tableName = table.objectName();
                            if ("*".equals(tableName)) {
                                specificTables.stream().filter(id -> id.schemaName().equals(schemaName)).forEach(id -> tableGrants.put(id, grant));
                            }
                            else {
                                tableGrants.put(table, grant);
                            }
                        }

                        // Check cumulative grants
                        assertThat(toTableGrantList(catalogId, securityApi.listTablePrivileges(admin(), new QualifiedTablePrefix(catalogName)))).containsExactlyInAnyOrderElementsOf(catalogGrants);

                        for (CatalogSchemaName schema : schemas) {
                            SchemaId schemaId = new SchemaId(catalogId, schema.getSchemaName());

                            // Use the clietn api to get the grants
                            assertThat(client.listTableGrants(dispatchSession(adminRoleId), schemaId).stream())
                                    .containsExactlyInAnyOrderElementsOf(schemaGrants.get(schema));

                            // Use the securityApi to get the grants
                            assertThat(toTableGrantList(catalogId, securityApi.listTablePrivileges(admin(), new QualifiedTablePrefix(catalogName, schemaId.getSchemaName()))))
                                    .containsExactlyInAnyOrderElementsOf(schemaGrants.get(schema));
                        }

                        for (QualifiedObjectName specificTable : specificTables) {
                            assertThat(toTableGrantList(catalogId, securityApi.listTablePrivileges(admin(), toPrefix(specificTable)))).containsExactlyInAnyOrderElementsOf(tableGrants.get(specificTable));
                        }
                    }
                });
            }
        });
    }

    /**
     * Set&lt;CatalogId&gt; getVisibleCatalogIds(DispatchSession session);
     * <p>
     * Return the CatalogIds of catalogs visible to the session.  A catalog
     * is visible if the role has any privilege or ownership on the catalog,
     * or on any schema or table contained in the catalog.
     * <p>
     * Security: none - this only shows the permissions the current session has.
     */
    @Test
    public void testGetVisibleCatalogIds()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                CatalogSchemaName schema = new CatalogSchemaName(catalogName, makeSchemaName());
                SchemaId schemaId = new SchemaId(catalogId, schema.getSchemaName());
                TableId tableId = makeTableId(schemaId);
                CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schema.getSchemaName(), tableId.getTableName());
                List<EntityId> entityIds = ImmutableList.of(catalogId, schemaId, tableId);
                securityApi.setSchemaOwner(admin(), schema, rolePrincipal(ACCOUNT_ADMIN));
                securityApi.setTableOwner(admin(), table, rolePrincipal(ACCOUNT_ADMIN));

                // With no privileges or ownership, the catalog isn't visible to roleName
                assertThat(getFilteredCatalogsForRole(roleId)).isEmpty();

                // Show that any ALLOW or DENY privilege on the catalog or any contained object makes the catalog visible
                for (GrantKind grantKind : ImmutableList.of(ALLOW, DENY)) {
                    for (EntityId entityId : entityIds) {
                        for (Privilege privilege : getEntityKindPrivileges(entityId.getEntityKind())) {
                            assertThat(getFilteredCatalogsForRole(roleId)).isEmpty();
                            withGrantedPrivilege(adminRoleId, entityId, privilege, grantKind, roleName, false, () -> {
                                Set<String> catalogs = getFilteredCatalogsForRole(roleId);
                                switch (grantKind) {
                                    case ALLOW -> assertThat(catalogs).containsOnly(catalogName);
                                    case DENY -> assertThat(catalogs).isEmpty();
                                    default -> throw new IllegalArgumentException("Unrecognized grantKind " + grantKind);
                                }
                            });
                        }
                    }
                }

                // Show that ownership on the catalog or any contained object makes the catalog visible
                for (EntityId entityId : entityIds) {
                    assertThat(getFilteredCatalogsForRole(roleId)).isEmpty();
                    withEntityOwnership(roleId, entityId, () ->
                            assertThat(getFilteredCatalogsForRole(roleId)).containsOnly(catalogName));
                }
            });
        });
    }

    /**
     * ContentsVisibility getSchemaVisibility(DispatchSession session, CatalogId catalogId);
     * <p>
     * Get the visibility rules for schemas contained within the specified catalogs
     * <p>
     * Security: none - this only shows the permissions the current session has.
     */
    @Test
    public void testGetSchemaVisibility()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                SchemaId schemaId = makeSchemaId(catalogId);
                String schemaName = schemaId.getSchemaName();
                CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
                TableId tableId = makeTableId(schemaId);
                CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schemaName, tableId.getTableName());
                List<EntityId> entityIds = ImmutableList.of(schemaId, tableId);
                securityApi.setSchemaOwner(admin(), schema, rolePrincipal(ACCOUNT_ADMIN));
                securityApi.setTableOwner(admin(), table, rolePrincipal(ACCOUNT_ADMIN));
                ContentsVisibility noVisibility = new ContentsVisibility(DENY, ImmutableSet.of());
                Runnable checkNoVisibility = () -> assertThat(client.getSchemaVisibility(dispatchSession(roleId), catalogId)).isEqualTo(noVisibility);

                // With no privileges or ownership, the catalog isn't visible to roleName
                checkNoVisibility.run();

                // Show that any ALLOW or DENY privilege on the schema or any contained object makes the schema visible
                for (GrantKind grantKind : ImmutableList.of(ALLOW, DENY)) {
                    for (EntityId entityId : entityIds) {
                        for (Privilege privilege : getEntityKindPrivileges(entityId.getEntityKind())) {
                            checkNoVisibility.run();
                            withGrantedPrivilege(adminRoleId, entityId, privilege, grantKind, roleName, false, () -> {
                                Set<String> schemas = grantKind == DENY ? ImmutableSet.of() : ImmutableSet.of(schemaName);
                                assertThat(client.getSchemaVisibility(dispatchSession(roleId), catalogId)).isEqualTo(new ContentsVisibility(DENY, schemas));
                            });
                        }
                    }
                }

                // Show that ownership of the schema or any contained object makes the schema visible
                for (EntityId entityId : entityIds) {
                    checkNoVisibility.run();
                    withEntityOwnership(roleId, entityId, () ->
                            assertThat(client.getSchemaVisibility(dispatchSession(roleId), catalogId)).isEqualTo(new ContentsVisibility(DENY, ImmutableSet.of(schemaName))));
                }

                checkNoVisibility.run();

                withGrantedPrivilege(adminRoleId, schemaId, CREATE_TABLE, ALLOW, roleName, false, () -> {
                    // With the schema ALLOW privilege, the schema is visible
                    assertThat(client.getSchemaVisibility(dispatchSession(roleId), catalogId)).isEqualTo(new ContentsVisibility(DENY, ImmutableSet.of(schemaName)));
                    // Show that a DENY privilege on the schema masks the ALLOW privilege on a schema
                    withGrantedPrivilege(adminRoleId, schemaId, CREATE_TABLE, DENY, roleName, false, checkNoVisibility);
                });

                checkNoVisibility.run();

                // Exercise wildcard privileges

                withGrantedPrivilege(adminRoleId, new SchemaId(catalogId, "*"), CREATE_TABLE, ALLOW, roleName, false, () -> {
                    // Wildcard ALLOW and wildcard DENY mean no access
                    withGrantedPrivilege(adminRoleId, new SchemaId(catalogId, "*"), CREATE_TABLE, DENY, roleName, false, checkNoVisibility);

                    // Wildcard ALLOW with DENY for a specific schema yields (ALLOW, [schemaName])
                    withGrantedPrivilege(adminRoleId, new SchemaId(catalogId, schemaName), CREATE_TABLE, DENY, roleName, false, () -> {
                        assertThat(client.getSchemaVisibility(dispatchSession(roleId), catalogId)).isEqualTo(new ContentsVisibility(ALLOW, ImmutableSet.of(schemaName)));
                    });
                });
            });
        });
    }

    /**
     * Map&lt;String, ContentsVisibility&gt; getTableVisibility(DispatchSession session, CatalogId catalogId, Set&lt;String&gt; schemaNames);
     * <p>
     * Get the visibility rules for tables contained within the specified schemas in the specified catalog.
     * <p>
     * Security: none<br>
     * This only shows the permissions the current session has.
     */
    @Test
    public void testGetTableVisibility()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                SchemaId schemaId1 = makeSchemaId(catalogId);
                CatalogSchemaName schema1 = new CatalogSchemaName(catalogName, schemaId1.getSchemaName());
                String schemaName1 = schemaId1.getSchemaName();
                SchemaId schemaId2 = makeSchemaId(catalogId);
                String schemaName2 = schemaId2.getSchemaName();
                Set<String> oneSchema = ImmutableSet.of(schemaName1);
                Set<String> bothSchemas = ImmutableSet.of(schemaName1, schemaName2);
                TableId schema1TableId1 = makeTableId(schemaId1);
                CatalogSchemaTableName schema1Table1 = new CatalogSchemaTableName(catalogName, schemaName1, schema1TableId1.getTableName());
                Set<String> tables = ImmutableSet.of(schema1TableId1.getTableName());
                TableId schema1TableId2 = makeTableId(schemaId1);
                TableId schema2TableId = makeTableId(schemaId2);
                securityApi.setSchemaOwner(admin(), schema1, rolePrincipal(ACCOUNT_ADMIN));
                securityApi.setTableOwner(admin(), schema1Table1, rolePrincipal(ACCOUNT_ADMIN));
                ContentsVisibility noVisibility = new ContentsVisibility(DENY, ImmutableSet.of());
                Runnable checkNoVisibility = () -> assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, oneSchema).get(schemaName1)).isEqualTo(noVisibility);

                // With no privileges or ownership, the catalog isn't visible to roleName
                checkNoVisibility.run();

                // Show that any ALLOW or DENY privilege on the table makes the schema visible
                for (GrantKind grantKind : ImmutableList.of(ALLOW, DENY)) {
                    for (Privilege privilege : getEntityKindPrivileges(EntityKind.TABLE)) {
                        checkNoVisibility.run();
                        withGrantedPrivilege(adminRoleId, schema1TableId1, privilege, grantKind, roleName, false, () -> {
                            Set<String> tablesToUse = grantKind == DENY ? ImmutableSet.of() : tables;
                            assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, oneSchema).get(schemaName1))
                                    .isEqualTo(new ContentsVisibility(DENY, tablesToUse));
                        });
                    }
                }

                checkNoVisibility.run();

                // Show that ownership of the table makes the table visible
                withEntityOwnership(roleId, schema1TableId1, () ->
                        assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, oneSchema).get(schemaName1)).isEqualTo(new ContentsVisibility(DENY, tables)));

                checkNoVisibility.run();

                for (Privilege privilege : getEntityKindPrivileges(EntityKind.TABLE)) {
                    withGrantedPrivilege(adminRoleId, schema1TableId1, privilege, ALLOW, roleName, false, () -> {
                        // With the table ALLOW privilege, the table is visible
                        assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, oneSchema).get(schemaName1))
                                .isEqualTo(new ContentsVisibility(DENY, ImmutableSet.of(schema1TableId1.getTableName())));

                        // Similarly, an ALLOW grant on another table in the same schema, visibility has both tableNames
                        withGrantedPrivilege(adminRoleId, schema1TableId2, privilege, ALLOW, roleName, false, () ->
                                assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, oneSchema).get(schemaName1))
                                        .isEqualTo(new ContentsVisibility(DENY, ImmutableSet.of(schema1TableId1.getTableName(), schema1TableId2.getTableName()))));

                        // Show that a DENY privilege on the table masks the ALLOW privilege on a table
                        withGrantedPrivilege(adminRoleId, schema1TableId1, privilege, DENY, roleName, false, checkNoVisibility);
                    });

                    // With a *.* wildcard ALLOW, any table is allowed
                    ContentsVisibility allVisibility = new ContentsVisibility(ALLOW, ImmutableSet.of());
                    withGrantedPrivilege(adminRoleId, new TableId(catalogId, "*", "*"), privilege, ALLOW, roleName, false, () -> {
                        // With wildcard *.* ALLOW, all tables are visible to all schemas
                        for (String schema : bothSchemas) {
                            assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schema)).isEqualTo(allVisibility);
                        }

                        // With a wildcard *.* DENY, all tables if not visible to all schemas, overriding the ALLOW privilege
                        withGrantedPrivilege(adminRoleId, new TableId(catalogId, "*", "*"), privilege, DENY, roleName, false, () -> {
                            for (String schema : bothSchemas) {
                                assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schema)).isEqualTo(noVisibility);
                            }

                            // Granting a wildcard * ALLOW privilege cannot reverse the effect of the wildcard DENY
                            withGrantedPrivilege(adminRoleId, new TableId(catalogId, schemaName1, "*"), privilege, ALLOW, roleName, false, () ->
                                    assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName1)).isEqualTo(noVisibility));

                            // Granting a non-wildcard ALLOW privilege cannot reverse the effect of the wildcard DENY
                            withGrantedPrivilege(adminRoleId, new TableId(catalogId, schemaName1, schema1TableId2.getTableName()), privilege, ALLOW, roleName, false, () ->
                                    assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName1)).isEqualTo(noVisibility));
                        });

                        // Show that visibility tracks with the schema
                        withGrantedPrivilege(adminRoleId, new TableId(catalogId, schemaName1, "*"), privilege, DENY, roleName, false, () -> {
                            withGrantedPrivilege(adminRoleId, new TableId(catalogId, schemaName2, "*"), privilege, ALLOW, roleName, false, () -> {
                                assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName1)).isEqualTo(noVisibility);
                                assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName2)).isEqualTo(allVisibility);
                            });
                        });

                        withGrantedPrivilege(adminRoleId, new TableId(catalogId, schemaName1, "*"), privilege, DENY, roleName, false, () -> {
                            // Show that a table wildcard DENY denies any table in the schema
                            assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName1)).isEqualTo(noVisibility);

                            // But still have visibility to schema2
                            assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName2)).isEqualTo(allVisibility);

                            // Show that a specific table DENY creates an exception
                            withGrantedPrivilege(adminRoleId, schema2TableId, privilege, DENY, roleName, false, () ->
                                    assertThat(client.getTableVisibility(dispatchSession(roleId), catalogId, bothSchemas).get(schemaName2)).isEqualTo(new ContentsVisibility(ALLOW, ImmutableSet.of(schema2TableId.getTableName()))));
                        });
                    });
                }

                checkNoVisibility.run();
            });
        });
    }

    /**
     * void setEntityOwner(DispatchSession session, EntityId entityId, RoleName owner);
     * <p>
     * Set the owner of the specified entity.
     * <p>
     * Security: (owner of entity AND user has grant for new owner) OR MANAGE_SECURITY
     */
    @Test
    public void testSetEntityOwner()
    {
        // Show that the owner of catalog "system" is role "_system"
        assertThat(securityApi.getSchemaOwner(admin(), new CatalogSchemaName("system", "any_schema"))).contains(rolePrincipal("_system"));

        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewRole(adminRoleId, (newOwnerRoleName, newOwnerRoleId) -> {
                withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                    SchemaId schemaId = makeSchemaId(catalogId);
                    String schemaName = schemaId.getSchemaName();
                    CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
                    TableId tableId = makeTableId(schemaId);
                    CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schemaName, tableId.getTableName());

                    // Setting schema or table owner to a USER TrinoPrincipal raises an exception
                    assertThatThrownBy(() -> securityApi.setSchemaOwner(admin(), schema, adminUserPrincipal()))
                            .isInstanceOf(TrinoException.class)
                            .hasMessage("Galaxy only supports a ROLE as an owner");
                    assertThatThrownBy(() -> securityApi.setTableOwner(admin(), table, adminUserPrincipal()))
                            .isInstanceOf(TrinoException.class)
                            .hasMessage("Galaxy only supports a ROLE as an owner");

                    // Show that setting the owner of a schema in catalog "system" results in an exception
                    assertThatThrownBy(() -> securityApi.setSchemaOwner(admin(), new CatalogSchemaName("system", "any_schema"), rolePrincipal(roleName)))
                            .isInstanceOf(TrinoException.class)
                            .hasMessage("System catalog is read-only");

                    // Show that setting the owner of a table in catalog "system" results in an exception
                    assertThatThrownBy(() -> securityApi.setTableOwner(admin(), new CatalogSchemaTableName("system", "any_schema", "any_table"), rolePrincipal(roleName)))
                            .isInstanceOf(TrinoException.class)
                            .hasMessage("System catalog is read-only");

                    securityApi.setSchemaOwner(admin(), schema, rolePrincipal(ACCOUNT_ADMIN));
                    securityApi.setTableOwner(fromRole(adminRoleId), table, rolePrincipal(ACCOUNT_ADMIN));

                    List<EntityId> entityIds = ImmutableList.of(schemaId, tableId);
                    for (EntityId entityId : entityIds) {
                        Consumer<RoleId> checkOwnership = someRoleId -> assertThat(accountClient.getEntityOwnership(entityId)).isEqualTo(someRoleId);
                        Runnable checkNoAccess = () -> assertThatThrownBy(() -> setEntityOwner(roleId, newOwnerRoleId, entityId))
                                .isInstanceOf(OperationNotAllowedException.class)
                                .hasMessageMatching("Operation not allowed.*");
                        Runnable checkAdminOwnershipAndNoAccess = () -> {
                            checkOwnership.accept(adminRoleId);
                            checkNoAccess.run();
                        };

                        checkAdminOwnershipAndNoAccess.run();

                        // With MANAGE_SECURITY, roleName can change the owner to newOwnerRoleName
                        withAccountPrivilege(MANAGE_SECURITY, ALLOW, roleName, false, () -> {
                            setEntityOwner(roleId, newOwnerRoleId, entityId);
                            // Verify the ownership change
                            checkOwnership.accept(newOwnerRoleId);

                            if (entityId.getEntityKind() == SCHEMA) {
                                // Show that securityApi returns the right schema owner
                                assertThat(securityApi.getSchemaOwner(admin(), schema)).contains(rolePrincipal(newOwnerRoleName));
                            }

                            // Set it back to admin
                            setEntityOwner(roleId, adminRoleId, entityId);
                        });

                        checkAdminOwnershipAndNoAccess.run();

                        // Being owner of the entityId is not sufficient to allow changing the owner
                        // if the current role doesn't have a grant on the new role
                        withEntityOwnership(newOwnerRoleId, entityId, checkNoAccess);

                        // Having a grant of the new role is not sufficient to allow changing the owner
                        // if the current role is not the owner of the entityId
                        withGrantedRole(newOwnerRoleName, roleName, checkNoAccess);

                        // But both ownership of the entityId and a grant on newOwnerRoleName allows changing the owner
                        withEntityOwnership(roleId, entityId, () ->
                                withGrantedRole(newOwnerRoleName, roleName, () -> {
                                    setEntityOwner(roleId, newOwnerRoleId, entityId);
                                    checkOwnership.accept(newOwnerRoleId);
                                    // Set it back to admin
                                    setEntityOwner(roleId, adminRoleId, entityId);
                                }));

                        checkNoAccess.run();

                        // Clean up
                        accountClient.deleteEntityReferences(entityId);
                    }
                    // Put back admin ownership of the catalog, so the role can be deleted
                    setEntityOwner(adminRoleId, adminRoleId, catalogId);
                });
            });
        });
    }

    /**
     * void entityCreated(DispatchSession session, EntityId entityId);
     * <p>
     * Removes all existing permissions and creates the default permission set for the entity.
     * <p>
     * Security: none - assume permissions were checked before the entity was created
     */
    @Test
    public void testEntityCreated()
    {
        withNewRole(adminRoleId, (_, _) -> {
            withNewRole(adminRoleId, (originalRoleName, originalRoleId) -> {
                withNewRole(adminRoleId, (_, newRoleId) -> {
                    withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                        SchemaId schemaId = makeSchemaId(catalogId);
                        CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaId.getSchemaName());
                        TableId tableId = makeTableId(schemaId);
                        ColumnId columnId = makeColumnId(tableId);
                        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, tableId.getSchemaName(), tableId.getTableName());
                        List<EntityId> entityIds = ImmutableList.of(schemaId, tableId);

                        for (EntityId entityId : entityIds) {
                            EntityKind privilegeEntityKind = entityId.getEntityKind() == TABLE ? COLUMN : entityId.getEntityKind();
                            Set<Privilege> privileges = getEntityKindPrivileges(privilegeEntityKind);
                            Privilege privilege = privileges.iterator().next();

                            // Grant the privilege to the originalRoleName and show that it took
                            client.addEntityPrivileges(dispatchSession(adminRoleId), entityId, ImmutableSet.of(new CreateEntityPrivilege(privilege, ALLOW, new RoleName(originalRoleName), false)));
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(originalRoleId), entityId))).containsOnly(privilege);

                            // Show that the new role name has no privileges
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(newRoleId), entityId))).isEmpty();

                            // Run the operation
                            switch (entityId.getEntityKind()) {
                                case SCHEMA -> securityApi.schemaCreated(fromRole(newRoleId), schema);
                                case TABLE -> securityApi.tableCreated(fromRole(newRoleId), table);
                                default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                            }

                            // show that column creation works and defaults to the appropriate owner
                            securityApi.columnCreated(fromRole(newRoleId), table, columnId.getColumnName());
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(newRoleId), tableId))).isEqualTo(entityId.getEntityKind() == TABLE ? privileges : ImmutableSet.of());

                            // Show that the newRoleName has all privileges
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(newRoleId), entityId))).isEqualTo(privileges);
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(originalRoleId), entityId))).isEqualTo(ImmutableSet.of(privilege));

                            client.revokeEntityPrivileges(dispatchSession(adminRoleId), entityId, ImmutableSet.of(new RevokeEntityPrivilege(privilege, new RoleName(originalRoleName), false)));
                            assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(originalRoleId), entityId))).isEmpty();

                            // Set ownership of the entity back to admin
                            setEntityOwner(adminRoleId, adminRoleId, entityId);
                        }
                    });
                });
            });
        });
    }

    /**
     * &lt;E extends EntityId&gt; void entityRenamed(DispatchSession session, E entityId, E newEntityId);
     * <p>
     * Update ownership and privileges to the new entity id.
     * <p>
     * Security: none - assume permissions were checked before the entity was renamed
     */
    @Test
    public void testEntityRenamed()
    {
        withNewRole(adminRoleId, (roleName, roleId) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                SchemaId schemaId = makeSchemaId(catalogId);
                SchemaId newSchemaId = makeSchemaId(catalogId);
                TableId tableId = makeTableId(schemaId);
                TableId newTableId = makeTableId(newSchemaId);
                SchemaId lastSchemaId = makeSchemaId(catalogId);
                TableId lastTableId = makeTableId(lastSchemaId);
                ColumnId columnId = makeColumnId(tableId);
                Map<EntityId, EntityId> entityIds = ImmutableMap.of(schemaId, newSchemaId, tableId, newTableId);

                entityIds.forEach((entityId, newEntityId) -> {
                    EntityKind entityKind = entityId.getEntityKind();
                    EntityId lastEntityId = entityKind == SCHEMA ? lastSchemaId : lastTableId;
                    for (io.trino.spi.security.Privilege privilege : PRIVILEGE_TRANSLATIONS.keySet()) {
                        Privilege translatedPrivilege = PRIVILEGE_TRANSLATIONS.get(privilege);
                        if (translatedPrivilege == CREATE_TABLE ? entityKind != SCHEMA : entityKind != TABLE) {
                            continue;
                        }
                        Set<Privilege> privileges = ImmutableSet.of(translatedPrivilege);

                        // Grant the privilege to the originalRoleName and show that it took
                        switch (entityId.getEntityKind()) {
                            case SCHEMA -> {
                                CatalogSchemaName schema = new CatalogSchemaName(catalogName, entityId.getSchemaName());
                                securityApi.grantSchemaPrivileges(admin(), schema, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                            }
                            case TABLE -> {
                                QualifiedObjectName table = new QualifiedObjectName(catalogName, entityId.getSchemaName(), entityId.getTableName());
                                securityApi.grantTablePrivileges(admin(), table, ImmutableSet.of(privilege), rolePrincipal(roleName), false);
                            }
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        }
                        assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(adminRoleId), entityId))).isEqualTo(privileges);

                        // create some column privileges under the entity, and make sure those work as well
                        io.trino.spi.security.Privilege columnPrivilege = translatedPrivilege == SELECT ? io.trino.spi.security.Privilege.UPDATE : io.trino.spi.security.Privilege.SELECT;
                        Map<String, ContentsVisibility> expectedColumnPrivileges = switch (entityId.getEntityKind()) {
                            case SCHEMA -> ImmutableMap.of(columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId.getColumnName())));
                            case TABLE -> ImmutableMap.of(translatedPrivilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of()), columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId.getColumnName())));
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        };
                        securityApi.grantColumnPrivileges(
                                admin(),
                                new QualifiedObjectName(catalogName, entityId.getSchemaName(), columnId.getTableName()),
                                columnId.getColumnName(),
                                ImmutableSet.of(columnPrivilege),
                                rolePrincipal(roleName),
                                false);
                        assertThat(client.getEntityPrivileges(dispatchSession(roleId), tableId).getColumnPrivileges())
                                .isEqualTo(expectedColumnPrivileges);

                        // Set ownership to roleName and show that it took
                        setEntityOwner(adminRoleId, roleId, entityId);
                        assertThat(accountClient.getEntityOwnership(entityId)).isEqualTo(roleId);

                        // Run the operation using the client
                        client.entityRenamed(dispatchSession(adminRoleId), entityId, newEntityId);

                        // Show that the roleName has the right privileges set on newEntityId
                        assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(adminRoleId), newEntityId))).isEqualTo(privileges);

                        // And that there are no privileges on the original entityId
                        assertThat(client.getEntityPrivileges(dispatchSession(adminRoleId), entityId).getPrivileges()).isEmpty();

                        // And that the column privileges were renamed properly as well
                        // because the role is the owner of the new entity, it has the synthetic VISIBLE privilege as well
                        expectedColumnPrivileges = switch (entityId.getEntityKind()) {
                            case SCHEMA -> ImmutableMap.of(
                                    columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId.getColumnName())),
                                    "VISIBLE", new ContentsVisibility(ALLOW, ImmutableSet.of()));
                            case TABLE -> ImmutableMap.of(
                                    privilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of()),
                                    columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId.getColumnName())),
                                    "VISIBLE", new ContentsVisibility(ALLOW, ImmutableSet.of()));
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        };
                        TableId expectedTableIdForColumn = entityId.getEntityKind() == TABLE ? (TableId) newEntityId : new TableId(columnId.getCatalogId(), newEntityId.getSchemaName(), columnId.getTableName());
                        assertThat(client.getEntityPrivileges(dispatchSession(roleId), expectedTableIdForColumn).getColumnPrivileges())
                                .isEqualTo(expectedColumnPrivileges);

                        // renaming column works as well
                        expectedColumnPrivileges = switch (entityId.getEntityKind()) {
                            case SCHEMA -> ImmutableMap.of(
                                    columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of("newName")),
                                    "VISIBLE", new ContentsVisibility(ALLOW, ImmutableSet.of()));
                            case TABLE -> ImmutableMap.of(
                                    privilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of()),
                                    columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of("newName")),
                                    "VISIBLE", new ContentsVisibility(ALLOW, ImmutableSet.of()));
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        };
                        client.entityRenamed(dispatchSession(adminRoleId),
                                new ColumnId(expectedTableIdForColumn.getCatalogId(), expectedTableIdForColumn.getSchemaName(), expectedTableIdForColumn.getTableName(), columnId.getColumnName()),
                                new ColumnId(expectedTableIdForColumn.getCatalogId(), expectedTableIdForColumn.getSchemaName(), expectedTableIdForColumn.getTableName(), "newName"));
                        assertThat(client.getEntityPrivileges(dispatchSession(roleId), expectedTableIdForColumn).getColumnPrivileges()).isEqualTo(expectedColumnPrivileges);

                        // Show that the owned entityId has been updated
                        assertThat(accountClient.getEntityOwnership(newEntityId)).isEqualTo(roleId);

                        // Show that the roleName has the right privileges set on newEntityId
                        assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(adminRoleId), newEntityId))).isEqualTo(privileges);

                        // And that there are no privileges on the original entityId
                        assertThat(client.getEntityPrivileges(dispatchSession(adminRoleId), entityId).getPrivileges()).isEmpty();

                        // Show that the owned entityId has been updated
                        assertThat(accountClient.getEntityOwnership(newEntityId)).isEqualTo(roleId);

                        // Run the operation using securityApi
                        switch (entityId.getEntityKind()) {
                            case SCHEMA -> {
                                CatalogSchemaName schema = new CatalogSchemaName(catalogName, newEntityId.getSchemaName());
                                CatalogSchemaName newSchema = new CatalogSchemaName(catalogName, lastEntityId.getSchemaName());
                                securityApi.schemaRenamed(admin(), schema, newSchema);
                            }
                            case TABLE -> {
                                CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, newEntityId.getSchemaName(), newEntityId.getTableName());
                                CatalogSchemaTableName newTable = new CatalogSchemaTableName(catalogName, lastEntityId.getSchemaName(), lastEntityId.getTableName());
                                securityApi.tableRenamed(admin(), table, newTable);
                            }
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        }

                        // Show that the roleName has the right privileges set on lastEntityId
                        assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(adminRoleId), lastEntityId))).isEqualTo(privileges);

                        // And that there are no privileges on the original entityId or newEntityId
                        assertThat(client.getEntityPrivileges(dispatchSession(adminRoleId), entityId).getPrivileges()).isEmpty();
                        assertThat(client.getEntityPrivileges(dispatchSession(adminRoleId), newEntityId).getPrivileges()).isEmpty();

                        // Show that the owned entityId has been updated
                        assertThat(accountClient.getEntityOwnership(lastEntityId)).isEqualTo(roleId);

                        // Clean up
                        accountClient.deleteEntityReferences(newEntityId);
                        accountClient.deleteEntityReferences(lastEntityId);
                    }
                });
            });
        });
    }

    /**
     * void entityDropped(DispatchSession session, EntityId entityId);
     * <p>
     * Removes all existing permissions and creates the default permission set for the entity.
     * <p>
     * Security: none - assume permissions were checked before the entity was renamed
     */
    @Test
    public void testEntityDropped()
    {
        withNewRoleAndPrincipal(adminRoleId, (roleName, _, rolePrincipal) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                SchemaId schemaId = makeSchemaId(catalogId);
                SchemaId newSchemaId = makeSchemaId(catalogId);
                TableId tableId = makeTableId(schemaId);
                TableId newTableId = makeTableId(newSchemaId);
                ColumnId columnId1 = makeColumnId(tableId);
                ColumnId columnId2 = makeColumnId(tableId);
                Map<EntityId, EntityId> entityIds = ImmutableMap.of(schemaId, newSchemaId, tableId, newTableId);
                entityIds.forEach((entityId, _) -> {
                    for (io.trino.spi.security.Privilege privilege : PRIVILEGE_TRANSLATIONS.keySet()) {
                        Privilege translatedPrivilege = PRIVILEGE_TRANSLATIONS.get(privilege);
                        EntityKind entityKind = entityId.getEntityKind();
                        if (translatedPrivilege == CREATE_TABLE ? entityKind != SCHEMA : entityKind != TABLE) {
                            continue;
                        }
                        Set<Privilege> privileges = ImmutableSet.of(translatedPrivilege);
                        String otherRoleName = makeRoleName();
                        securityApi.createRole(admin(), otherRoleName, Optional.empty());
                        RoleId otherRoleId = roleIdFromName(otherRoleName);

                        // Grant the privilege to the otherRoleName and show that it took
                        switch (entityId.getEntityKind()) {
                            case SCHEMA -> {
                                CatalogSchemaName schema = new CatalogSchemaName(catalogName, entityId.getSchemaName());
                                securityApi.grantSchemaPrivileges(admin(), schema, ImmutableSet.of(privilege), rolePrincipal(otherRoleName), false);
                                assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(otherRoleId), entityId))).isEqualTo(privileges);
                            }
                            case TABLE -> {
                                QualifiedObjectName table = new QualifiedObjectName(catalogName, entityId.getSchemaName(), entityId.getTableName());
                                securityApi.grantTablePrivileges(admin(), table, ImmutableSet.of(privilege), rolePrincipal(otherRoleName), false);
                                assertThat(extractPrivileges(client.getEntityPrivileges(dispatchSession(otherRoleId), entityId))).isEqualTo(privileges);
                            }
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        }

                        // grant column privileges as well to make sure they get dropped appropriately
                        QualifiedObjectName table = new QualifiedObjectName(catalogName, tableId.getSchemaName(), tableId.getTableName());
                        io.trino.spi.security.Privilege columnPrivilege = translatedPrivilege == SELECT ? io.trino.spi.security.Privilege.UPDATE : io.trino.spi.security.Privilege.SELECT;
                        securityApi.grantColumnPrivileges(admin(), table, columnId1.getColumnName(), ImmutableSet.of(columnPrivilege), rolePrincipal(otherRoleName), false);
                        securityApi.grantColumnPrivileges(admin(), table, columnId2.getColumnName(), ImmutableSet.of(columnPrivilege), rolePrincipal(otherRoleName), false);

                        Map<String, ContentsVisibility> expectedColumnPrivileges = switch (entityId.getEntityKind()) {
                            case SCHEMA -> ImmutableMap.of(columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId1.getColumnName(), columnId2.getColumnName())));
                            case TABLE -> ImmutableMap.of(translatedPrivilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of()), columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId1.getColumnName(), columnId2.getColumnName())));
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        };
                        assertThat(client.getEntityPrivileges(dispatchSession(otherRoleId), tableId).getColumnPrivileges())
                                .isEqualTo(expectedColumnPrivileges);

                        // drop a column and make sure the privilege is dropped accordingly
                        securityApi.columnDropped(admin(), new CatalogSchemaTableName(catalogName, columnId1.getSchemaName(), columnId1.getTableName()), columnId1.getColumnName());
                        expectedColumnPrivileges = switch (entityId.getEntityKind()) {
                            case SCHEMA -> ImmutableMap.of(columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId2.getColumnName())));
                            case TABLE -> ImmutableMap.of(
                                    translatedPrivilege.name(), new ContentsVisibility(ALLOW, ImmutableSet.of()),
                                    columnPrivilege.name(), new ContentsVisibility(DENY, ImmutableSet.of(columnId2.getColumnName())));
                            default -> throw new IllegalArgumentException("Unrecognized entityKind " + entityId.getEntityKind());
                        };
                        assertThat(client.getEntityPrivileges(dispatchSession(otherRoleId), tableId).getColumnPrivileges())
                                .isEqualTo(expectedColumnPrivileges);

                        // Set ownership to otherRoleName and show that it took
                        setEntityOwner(adminRoleId, otherRoleId, entityId);
                        assertThat(accountClient.getEntityOwnership(entityId)).isEqualTo(otherRoleId);

                        // Run the operation
                        switch (entityId.getEntityKind()) {
                            case SCHEMA -> {
                                SchemaId entitySchemaId = (SchemaId) entityId;
                                securityApi.schemaDropped(admin(), new CatalogSchemaName(catalogName, entitySchemaId.getSchemaName()));
                            }
                            case TABLE -> {
                                TableId entityTableId = (TableId) entityId;
                                securityApi.tableDropped(admin(), new CatalogSchemaTableName(catalogName, entityTableId.getSchemaName(), entityTableId.getTableName()));
                            }
                            default -> throw new IllegalArgumentException("Don't handle entityKind " + entityId.getEntityKind());
                        }

                        // Show that the privilege granted is gone and the column privilege was dropped when the container entity was dropped
                        EntityPrivileges entityPrivileges = client.getEntityPrivileges(dispatchSession(otherRoleId), entityId);
                        assertThat(extractPrivileges(entityPrivileges)).isEmpty();
                        assertThat(entityPrivileges.getColumnPrivileges()).isEmpty();

                        // Show that grant is gone
                        assertThat(securityApi.listRoleGrants(admin(), rolePrincipal)).containsOnly(publicGrant(roleName));

                        securityApi.dropRole(admin(), otherRoleName);
                    }
                });
            });
        });
    }

    @Test
    public void testStatisticsCollectionFinished()
    {
        withNewRoleAndPrincipal(adminRoleId, (_, _, _) -> {
            withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
                SchemaId schemaId = makeSchemaId(catalogId);
                TableId tableId = makeTableId(schemaId);
                CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName(catalogName, schemaId.getSchemaName(), tableId.getTableName());

                // This should just complete without error, notifying galaxy that new stats are available on table
                securityApi.finishStatisticsCollection(admin(), catalogSchemaTableName);
            });
        });
    }

    @Test
    public void testViewApis()
    {
        withNewCatalog(adminRoleId, (catalogName, catalogId) -> {
            CatalogSchemaTableName name = new CatalogSchemaTableName(catalogName, makeSchemaName(), makeTableName());

            // Show that we get the right error message from getViewRunAsIdentity()
            assertThatThrownBy(() -> securityApi.getViewRunAsIdentity(admin(), name))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageMatching(format("View '%s' does not have an explicit owner role, which is not allowed. Please define an explicit owner with an 'ALTER VIEW %s SET AUTHORIZATION ROLE' command. For more information, visit docs.starburst.io/starburst-galaxy/security/privileges.html*", name, name));

            // Show that we cqn set the view owner
            withNewRoleAndPrincipal(adminRoleId, (_, _, principal) -> {
                securityApi.setViewOwner(admin(), name, principal);

                accountClient.deleteEntityReferences(new TableId(catalogId, name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName()));
            });
        });
    }

    private Session fromRole(RoleId roleId)
    {
        return helper.session(roleId);
    }

    private Session fromUserAndRole(UserId userId, RoleId roleId)
    {
        return helper.session(userId, roleId);
    }

    private DispatchSession dispatchSession(RoleId roleId)
    {
        return GalaxyIdentity.toDispatchSession(helper.session(roleId));
    }

    private DispatchSession dispatchSession(UserId userId, RoleId roleId)
    {
        return GalaxyIdentity.toDispatchSession(helper.session(userId, roleId));
    }

    private static TrinoPrincipal rolePrincipal(String roleName)
    {
        return new TrinoPrincipal(PrincipalType.ROLE, roleName);
    }

    private TrinoPrincipal adminUserPrincipal()
    {
        return userPrincipal(adminEmail);
    }

    private static TrinoPrincipal userPrincipal(String email)
    {
        return new TrinoPrincipal(PrincipalType.USER, email);
    }

    private static String makeRoleName()
    {
        return "role_" + ROLE_COUNTER.incrementAndGet();
    }

    private RoleGrant publicGrant(String grantee)
    {
        return new RoleGrant(new TrinoPrincipal(PrincipalType.ROLE, grantee), "public", false);
    }

    private Session adminSession()
    {
        return helper.adminSession();
    }

    private Identity adminIdentity()
    {
        return identity(adminRoleId);
    }

    private Identity publicIdentity()
    {
        return identity(publicRoleId);
    }

    private Identity fearlessIdentity()
    {
        return identity(fearlessRoleId);
    }

    private Identity lackeyIdentity()
    {
        return identity(lackeyRoleId);
    }

    private Identity identity(RoleId roleId)
    {
        return helper.context(roleId).getIdentity();
    }

    private SystemSecurityContext publicContext()
    {
        return helper.publicContext();
    }

    private SystemSecurityContext context(RoleId roleId)
    {
        return helper.context(roleId);
    }

    private Session admin()
    {
        return fromRole(adminRoleId);
    }

    private interface GranteeConsumer
    {
        void accept(String roleName, RoleId roleId, TrinoPrincipal grantee);
    }

    private void withNewRole(RoleId sessionRoleId, BiConsumer<String, RoleId> consumer)
    {
        String roleName = "role_" + ROLE_COUNTER.incrementAndGet();
        boolean created = false;
        try {
            securityApi.createRole(fromRole(sessionRoleId), roleName, Optional.empty());
            created = true;
            RoleId roleId = helper.getAccessController(adminIdentity()).listEnabledRoles(adminIdentity()).get(new RoleName(roleName));
            consumer.accept(roleName, roleId);
        }
        catch (Throwable t) {
            log.error(t, format("withNewRole unexpected exception sessionRoleId %s, roleName %s", sessionRoleId, roleName));
            throw t;
        }
        finally {
            if (created) {
                securityApi.dropRole(fromRole(sessionRoleId), roleName);
            }
        }
    }

    private void withNewRoleAndPrincipal(RoleId sessionRoleId, GranteeConsumer consumer)
    {
        String roleName = "role_" + ROLE_COUNTER.incrementAndGet();
        TrinoPrincipal principal = rolePrincipal(roleName);
        boolean created = false;
        try {
            securityApi.createRole(fromRole(sessionRoleId), roleName, Optional.empty());
            created = true;
            RoleId roleId = helper.getAccessController(adminIdentity()).listEnabledRoles(adminIdentity()).get(new RoleName(roleName));
            consumer.accept(roleName, roleId, principal);
        }
        catch (Throwable t) {
            log.error(t, format("withNewRole unexpected exception sessionRoleId %s, roleName %s", sessionRoleId, roleName));
            throw t;
        }
        finally {
            if (created) {
                securityApi.dropRole(fromRole(sessionRoleId), roleName);
            }
        }
    }

    private interface IsolatedUserAndRoleConsumer
    {
        void accept(UserId userId, String roleName, RoleId roleId, TrinoPrincipal grantee);
    }

    private void withIsolatedUserAndRole(RoleId sessionRoleId, IsolatedUserAndRoleConsumer consumer)
    {
        withNewUser(sessionRoleId, (_, userId) -> {
            String roleName = "role_" + ROLE_COUNTER.incrementAndGet();
            TrinoPrincipal principal = rolePrincipal(roleName);
            boolean created = false;
            try {
                securityApi.createRole(fromRole(sessionRoleId), roleName, Optional.empty());
                created = true;
                RoleId roleId = helper.getAccessController(adminIdentity()).listEnabledRoles(adminIdentity()).get(new RoleName(roleName));
                consumer.accept(userId, roleName, roleId, principal);
            }
            catch (Throwable t) {
                log.error(t, format("withNewRole unexpected exception sessionRoleId %s, roleName %s", sessionRoleId, roleName));
                throw t;
            }
            finally {
                if (created) {
                    securityApi.dropRole(fromRole(sessionRoleId), roleName);
                }
            }
        });
    }

    private void withNewUserGrantee(RoleId sessionRoleId, Consumer<TrinoPrincipal> consumer)
    {
        withNewUser(sessionRoleId, (email, _) -> {
            TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, email);
            consumer.accept(principal);
        });
    }

    private void withNewUser(RoleId sessionRoleId, BiConsumer<String, UserId> consumer)
    {
        boolean created = false;
        TestUser testUser = null;
        try {
            testUser = helper.getAccountClient().createUser();
            created = true;
            consumer.accept(testUser.getEmail(), testUser.getUserId());
        }
        catch (Throwable t) {
            log.error(t, format("withNewUser unexpected exception sessionRoleId %s, testUser %s", sessionRoleId, testUser));
            throw t;
        }
        finally {
            if (created) {
                helper.getAccountClient().deleteUser(testUser.getUserId());
            }
        }
    }

    private void withAccountPrivilege(Privilege privilege, GrantKind grantKind, String grantee, boolean grantOption, Runnable runnable)
    {
        boolean created = false;
        RoleId granteeRoleId = null;
        Optional<GrantDetails> previousGrant = Optional.empty();
        try {
            granteeRoleId = roleIdFromName(grantee);
            previousGrant = accountClient.getAccountPrivileges(granteeRoleId).stream()
                    .filter(grant -> grant.getPrivilege() == privilege)
                    .findFirst();
            accountClient.grantAccountPrivilege(new GrantDetails(privilege, granteeRoleId, grantKind, grantOption, accountId));
            created = true;
            runnable.run();
        }
        catch (Throwable t) {
            log.error(t, "withAccountPrivilege unexpected exception");
            throw t;
        }
        finally {
            Optional<GrantDetails> previousGrantAgain = previousGrant;
            if (created) {
                accountClient.revokeAccountPrivilege(new GrantDetails(privilege, granteeRoleId, grantKind, grantOption, accountId));
                // Revoke removes the grant regardless of current grantKind, so if it previously existed, reinstate the grant
                previousGrantAgain.ifPresent(grant ->
                        accountClient.grantAccountPrivilege(grant));
            }
        }
    }

    private void withGrantedPrivilege(RoleId sessionRoleId, EntityId entityId, Privilege privilege, GrantKind grantKind, String grantee, boolean grantOption, Runnable runnable)
    {
        boolean created = false;
        RoleId granteeRoleId;
        Optional<GrantDetails> previousGrant = Optional.empty();
        try {
            granteeRoleId = roleIdFromName(grantee);
            previousGrant = getEntityPrivileges(granteeRoleId, entityId).stream()
                    .filter(grantedPrivilege -> grantedPrivilege.getEntityId().equals(entityId) && grantedPrivilege.getPrivilege() == privilege)
                    .findFirst();
            client.addEntityPrivileges(dispatchSession(sessionRoleId), entityId, ImmutableSet.of(new CreateEntityPrivilege(privilege, grantKind, new RoleName(grantee), grantOption)));
            created = true;
            runnable.run();
        }
        catch (Throwable t) {
            log.info("withGrantedPrivilege unexpected exception sessionRoleId %s, entityId %s", sessionRoleId, entityId);
            throw t;
        }
        finally {
            Optional<GrantDetails> previousGrantAgain = previousGrant;
            if (created) {
                client.revokeEntityPrivileges(dispatchSession(adminRoleId), entityId, ImmutableSet.of(new RevokeEntityPrivilege(privilege, new RoleName(grantee), false)));
                // Revoke removes the grant regardless of current grantKind, so if it previously existed, reinstate the grant
                previousGrantAgain.ifPresent(grant ->
                        client.addEntityPrivileges(
                                dispatchSession(sessionRoleId),
                                entityId,
                                ImmutableSet.of(new CreateEntityPrivilege(privilege, grant.getGrantKind(), new RoleName(roleNameFromId(grant.getGranteeRoleId())), grant.isGrantOption()))));
            }
        }
    }

    private void withNewCatalog(RoleId sessionRoleId, BiConsumer<String, CatalogId> consumer)
    {
        String catalogName = catalogNames.get(CATALOG_COUNTER.getAndIncrement());
        CatalogId catalogId = helper.getCatalogId(catalogName);
        try {
            consumer.accept(catalogName, catalogId);
        }
        catch (Throwable t) {
            log.error(t, format("withNewCatalog unexpected exception sessionRoleId %s", sessionRoleId));
            throw t;
        }
        finally {
            CATALOG_COUNTER.decrementAndGet();
        }
    }

    private void withEntityOwnership(RoleId owningRoleId, EntityId entityId, Runnable consumer)
    {
        boolean created = false;
        try {
            setEntityOwner(adminRoleId, owningRoleId, entityId);
            created = true;
            consumer.run();
        }
        catch (Throwable t) {
            log.error(t, format("withEntityOwnership unexpected exception owningRoleId %s, entityId %s", owningRoleId, entityId));
            throw t;
        }
        finally {
            if (created) {
                setEntityOwner(adminRoleId, adminRoleId, entityId);
            }
        }
    }

    private void setEntityOwner(RoleId roleId, RoleId owningRoleId, EntityId entityId)
    {
        accountClient.setEntityOwnership(roleId, owningRoleId, entityId);
    }

    private void withGrantedRole(String grantedRoleName, String granteeRoleName, Runnable consumer)
    {
        boolean created = false;
        try {
            securityApi.grantRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(rolePrincipal(granteeRoleName)), false, Optional.empty());
            created = true;
            consumer.run();
        }
        catch (Throwable t) {
            log.error(t, format("withGrantedRole unexpected exception granteeRoleName %s, grantedRoleName %s", granteeRoleName, grantedRoleName));
            throw t;
        }
        finally {
            if (created) {
                securityApi.revokeRoles(admin(), ImmutableSet.of(grantedRoleName), ImmutableSet.of(rolePrincipal(granteeRoleName)), false, Optional.empty());
            }
        }
    }

    private void grantOrDenyTablePrivileges(GrantKind grantKind, RoleId roleId, String roleName, QualifiedObjectName qualifiedTable, Set<io.trino.spi.security.Privilege> privileges, boolean grantOption)
    {
        switch (grantKind) {
            case ALLOW -> securityApi.grantTablePrivileges(fromRole(roleId), qualifiedTable, privileges, rolePrincipal(roleName), grantOption);
            case DENY -> securityApi.denyTablePrivileges(fromRole(roleId), qualifiedTable, privileges, rolePrincipal(roleName));
            default -> throw new IllegalArgumentException("Unrecognized grantKind " + grantKind);
        }
    }

    private Set<String> getFilteredCatalogsForRole(RoleId roleId)
    {
        return Sets.difference(accessControl.filterCatalogs(context(roleId), helper.getAccountClient().getAllCatalogNames()), publicCatalogNames);
    }

    private RoleId roleIdFromName(String roleName)
    {
        Map<RoleName, RoleId> roles = client.listEnabledRoles(dispatchSession(adminRoleId));
        return roles.get(new RoleName(roleName));
    }

    private String roleNameFromId(RoleId roleId)
    {
        BiMap<RoleName, RoleId> roles = ImmutableBiMap.copyOf(client.listEnabledRoles(dispatchSession(adminRoleId)));
        return roles.inverse().get(roleId).getName();
    }

    private List<GrantDetails> getEntityPrivileges(RoleId roleId, EntityId entityId)
    {
        return accountClient.getPrivilegesOnEntityForRole(roleId, entityId);
    }

    private TableGrant toTableGrant(CatalogId catalogId, GrantInfo grant)
    {
        return new TableGrant(
                PRIVILEGE_TRANSLATIONS.get(grant.getPrivilegeInfo().getPrivilege()),
                ALLOW,
                grant.getPrivilegeInfo().isGrantOption(),
                new RoleName(grant.getGrantee().getName()),
                new TableId(catalogId, grant.getSchemaTableName().getSchemaName(), grant.getSchemaTableName().getTableName()));
    }

    private List<TableGrant> toTableGrantList(CatalogId catalogId, Collection<GrantInfo> info)
    {
        return info.stream().map(grant -> toTableGrant(catalogId, grant)).collect(toImmutableList());
    }

    private static String makeSchemaName()
    {
        return "schema" + SCHEMA_COUNTER.getAndIncrement();
    }

    private static SchemaId makeSchemaId(CatalogId catalogId)
    {
        return new SchemaId(catalogId, makeSchemaName());
    }

    private static String makeTableName()
    {
        return "table" + TABLE_COUNTER.getAndIncrement();
    }

    private static String makeColumnName()
    {
        return "column" + COLUMN_COUNTER.getAndIncrement();
    }

    private static TableId makeTableId(SchemaId schemaId)
    {
        return new TableId(schemaId.getCatalogId(), schemaId.getSchemaName(), makeTableName());
    }

    private static ColumnId makeColumnId(TableId tableId)
    {
        return new ColumnId(tableId.getCatalogId(), tableId.getSchemaName(), tableId.getTableName(), makeColumnName());
    }

    private static Set<Privilege> extractPrivileges(EntityPrivileges grants)
    {
        return grants.getPrivileges().stream().map(GalaxyPrivilegeInfo::getPrivilege).collect(toImmutableSet());
    }

    private static QualifiedTablePrefix toPrefix(QualifiedObjectName name)
    {
        return new QualifiedTablePrefix(name.catalogName(), name.schemaName(), name.objectName());
    }
}
