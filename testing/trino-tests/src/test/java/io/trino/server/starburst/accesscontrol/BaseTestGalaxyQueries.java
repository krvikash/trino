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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Key;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.ColumnMaskType;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestUser;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GalaxyPrivilegeInfo;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ClusterId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.ColumnMaskId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.PolicyId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.RowFilterId;
import io.starburst.stargate.id.StorageLocation;
import io.starburst.stargate.id.TableBranchId;
import io.starburst.stargate.id.TagId;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.server.starburst.GalaxyCockroachContainer;
import io.trino.server.starburst.security.TestingAccountFactory;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.ENABLE_DISABLE_CLUSTER;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.MONITOR_CLUSTER;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SSO_USER_PASSWORD_LOGIN;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.TROUBLESHOOT_QUERY;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.USE_CLUSTER;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.VIEW_ALL_QUERY_HISTORY;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.VIEW_PUBLIC_OAUTH_CLIENT;
import static io.trino.server.starburst.security.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static io.trino.server.starburst.security.GalaxyIdentity.createIdentity;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.server.starburst.security.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.DELETE;
import static io.trino.spi.security.Privilege.INSERT;
import static io.trino.spi.security.Privilege.MANAGE_DATA_OBSERVABILITY;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * You can debug these queries in both Trino and Stargate portal-server!
 * <p>
 * To run a subclass of this test, allowing breakpoints in both Trino and portal-server:
 * <ul>
 *     <li>Debug main class LocalTestingTrinoPortalServer in the stargate project.  No args are required.</li>
 *     <li>Debug the subclass with VM argument -DdebugPortal=true</li>
 * </ul>
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public abstract class BaseTestGalaxyQueries
        extends AbstractTestQueryFramework
{
    private static final String MY_SCHEMA = "my_schema";
    private static final String MY_TABLE = "my_table";
    private static final SchemaTableName MY_SCHEMA_TABLE = new SchemaTableName(MY_SCHEMA, MY_TABLE);
    @Language("SQL")
    private static final String MY_TABLE_COLUMNS_QUERY = "SELECT column_name FROM memory.information_schema.columns WHERE table_schema = 'my_schema' AND table_name = 'my_table'";
    private static final ImmutableSet<PrivilegeInfo> ALL_TABLE_PRIVILEGES = ImmutableSet.of(SELECT, INSERT, UPDATE, DELETE, MANAGE_DATA_OBSERVABILITY).stream()
            .map(privilege -> new PrivilegeInfo(privilege, true))
            .collect(toImmutableSet());

    private TestingAccountFactory testingAccountFactory;
    protected TestingAccountClient testingAccountClient;
    protected boolean useSharedPermissionsCache;
    protected GalaxyAccountPermissionsCache galaxyAccountPermissionsCache;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> closeAfterClass(new GalaxyCockroachContainer())));
        testingAccountClient = testingAccountFactory.createAccountClient();
        DistributedQueryRunner queryRunner = createGalaxyQueryRunner(
                testingAccountFactory,
                testingAccountClient,
                ImmutableMap.of("galaxy.use-shared-permissions-cache", Boolean.toString(useSharedPermissionsCache)),
                ImmutableMap.of("galaxy.entity-privileges.enabled", "true"));
        initWithQueryRunner(queryRunner);
        galaxyAccountPermissionsCache = queryRunner.getCoordinator().getInstance(Key.get(GalaxyAccountPermissionsCache.class));
        return queryRunner;
    }

    /**
     * Creates a query runner with Galaxy access control and no workers. The default session must use {@code memory.tiny},
     * and the runner must have a read-only {@code tpch} catalog and a writable {@code memory} catalog.
     */
    protected abstract DistributedQueryRunner createGalaxyQueryRunner(
            TestingAccountFactory testingAccountFactory,
            TestingAccountClient testingAccountClient,
            Map<String, String> coordinatorProperties,
            Map<String, String> extraProperties)
            throws Exception;

    /**
     * Catalogs the query runner has in addition to {@code system}, {@code tpch} and {@code memory}.
     */
    protected Set<String> extraCatalogs()
    {
        return ImmutableSet.of();
    }

    protected void initWithQueryRunner(QueryRunner queryRunner)
    {
        queryRunner.execute(format("CREATE SCHEMA %s.%s", "memory", "tiny"));
    }

    @AfterEach
    public void verifyCleanup()
    {
        assertThat(query("SHOW ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));
        assertThat(query("SHOW SCHEMAS FROM memory"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("information_schema", "tiny", "default"));
    }

    @Test
    public void validateEverythingIsWorking()
    {
        assertThat(query("SELECT 1234"))
                .matches("VALUES 1234");
    }

    @Test
    public void testGalaxyNotSupported()
    {
        String explicitGrantorNotSupported = "Galaxy does not support creating a role with an explicit grantor";
        assertQueryFails("CREATE ROLE my_role WITH ADMIN ROLE accountadmin", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN ROLE public", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN USER \"admin@example.com\"", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN CURRENT_USER", explicitGrantorNotSupported);

        String grantWithGrantedByNotSupported = "Galaxy does not support GRANT with the GRANTED BY clause";
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY ROLE accountadmin", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY ROLE public", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY USER \"admin@example.com\"", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY CURRENT_USER", grantWithGrantedByNotSupported);

        String revokeWithGrantedByNotSupported = "Galaxy does not support REVOKE with the GRANTED BY clause";
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY ROLE accountadmin", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY ROLE public", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY USER \"admin@example.com\"", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY CURRENT_USER", revokeWithGrantedByNotSupported);

        assertUpdate("CREATE SCHEMA memory.unsupported_test");
        assertQueryFails("GRANT CREATE ON SCHEMA memory.unsupported_test TO USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("REVOKE CREATE ON SCHEMA memory.unsupported_test FROM USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("ALTER SCHEMA memory.unsupported_test SET AUTHORIZATION USER \"admin@example.com\"", "Galaxy only supports a ROLE as an owner");

        assertUpdate("CREATE TABLE memory.unsupported_test.my_table (my_column varchar)");
        assertQueryFails("GRANT SELECT ON TABLE memory.unsupported_test.my_table TO USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("REVOKE SELECT ON TABLE memory.unsupported_test.my_table FROM USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("ALTER TABLE memory.unsupported_test.my_table SET AUTHORIZATION USER \"admin@example.com\"", "Galaxy only supports a ROLE as an owner");

        assertUpdate("DROP TABLE memory.unsupported_test.my_table");
        assertUpdate("DROP SCHEMA memory.unsupported_test");
    }

    @Test
    public void testCatalogManagementNotSupported()
    {
        // Galaxy manages catalogs itself, so catalog DDL is denied even for accountadmin
        assertQueryFails("CREATE CATALOG catalog_management_test USING memory", "Access Denied: Cannot create catalog catalog_management_test");
        // The access check runs before the existence check, so this never drops anything
        assertQueryFails("DROP CATALOG IF EXISTS catalog_management_test", "Access Denied: Cannot drop catalog catalog_management_test");
        assertQueryFails(publicSession(), "CREATE CATALOG catalog_management_test USING memory", "Access Denied: Cannot create catalog catalog_management_test");
    }

    @Test
    public void testPrivilegesNotValidForEntityKind()
    {
        try {
            assertUpdate("CREATE SCHEMA memory.entity_kind_privileges");
            assertUpdate("CREATE TABLE memory.entity_kind_privileges.my_table (my_column varchar)");

            // Galaxy restricts schemas to CREATE and tables to the table privileges, unlike upstream Trino which accepts any privilege
            assertQueryFails("GRANT INSERT ON SCHEMA memory.entity_kind_privileges TO ROLE public", ".*Unknown privilege: 'INSERT'");
            assertQueryFails("DENY SELECT ON SCHEMA memory.entity_kind_privileges TO ROLE public", ".*Unknown privilege: 'SELECT'");
            assertQueryFails("REVOKE DELETE ON SCHEMA memory.entity_kind_privileges FROM ROLE public", ".*Unknown privilege: 'DELETE'");
            assertQueryFails("GRANT CREATE ON TABLE memory.entity_kind_privileges.my_table TO ROLE public", ".*Unknown privilege: 'CREATE'");
            assertQueryFails("DENY CREATE ON TABLE memory.entity_kind_privileges.my_table TO ROLE public", ".*Unknown privilege: 'CREATE'");
            assertQueryFails("REVOKE CREATE ON TABLE memory.entity_kind_privileges.my_table FROM ROLE public", ".*Unknown privilege: 'CREATE'");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS memory.entity_kind_privileges CASCADE");
        }
    }

    @Test
    public void testRoleManagement()
    {
        Session userA = newUserSession(getTestingAccountClient().getPublicRoleId());
        Session userB = newUserSession(getTestingAccountClient().getPublicRoleId());

        verifyCurrentRoles(ImmutableSet.of(), ImmutableSet.of());

        assertUpdate("CREATE ROLE role_x");
        assertUpdate("CREATE ROLE role_y");
        assertUpdate("CREATE ROLE role_z");
        verifyCurrentRoles(ImmutableSet.of("role_x", "role_y", "role_z"), ImmutableSet.of());

        assertQueryFails(
                "GRANT role_x, role_y, role_z TO ROLE public",
                "Cannot grant roles to PUBLIC as that would create a cycle. Attempted to grant the following roles to public:.*");
        verifyCurrentRoles(ImmutableSet.of("role_x", "role_y", "role_z"), ImmutableSet.of());

        // revoke is "if exists"
        assertUpdate("REVOKE role_x, role_y, role_z FROM ROLE public");

        // multiple grants to users and roles
        assertUpdate("GRANT role_x, role_y TO ROLE role_z, USER \"" + userA.getUser() + "\", USER \"" + userB.getUser() + "\"");
        verifyRoleGrants(userA, ImmutableMultimap.<TrinoPrincipal, String>builder()
                .put(new TrinoPrincipal(USER, userA.getUser()), "role_x")
                .put(new TrinoPrincipal(USER, userA.getUser()), "role_y")
                .build());
        verifyUserRoles(userA, ImmutableSet.of("role_x", "role_y"));
        verifyUserRoles(userB, ImmutableSet.of("role_x", "role_y"));

        assertQueryFails("GRANT role_y, role_z TO USER \"" + userA.getUser() + "\", USER \"" + userB.getUser() + "\" WITH ADMIN OPTION",
                "Galaxy only supports a ROLE for GRANT with ADMIN OPTION");

        assertUpdate("DROP ROLE role_x");
        assertUpdate("DROP ROLE role_y");
        assertUpdate("DROP ROLE role_z");
        verifyCurrentRoles(ImmutableSet.of(), ImmutableSet.of());
    }

    private void verifyCurrentRoles(Set<String> extraAdminRoles, Set<String> extraPublicRoles)
    {
        MaterializedResult allRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("accountadmin")
                .add("public")
                .addAll(extraAdminRoles)
                .addAll(extraPublicRoles)
                .build());

        MaterializedResult adminRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("accountadmin")
                .add("public")
                .addAll(extraAdminRoles)
                .build());

        MaterializedResult publicRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("public")
                .addAll(extraPublicRoles)
                .build());

        assertThat(query("SHOW ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SHOW ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);

        assertThat(query("SELECT * FROM system.information_schema.roles"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.roles"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query("SELECT * FROM tpch.information_schema.roles"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.roles"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);

        assertThat(query("SHOW CURRENT ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(adminRoles);
        assertThat(query(publicSession(), "SHOW CURRENT ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(publicRoles);

        assertThat(query("SELECT * FROM system.information_schema.ENABLED_ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(adminRoles);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.ENABLED_ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(publicRoles);
        assertThat(query("SELECT * FROM tpch.information_schema.ENABLED_ROLES"))
                .result()
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.enabled_roles"))
                .result()
                .skippingTypesCheck()
                .matches(publicRoles);

        // role grants only shows the direct grants of the user, and is not transitive
        assertThat(query("SHOW ROLE GRANTS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));
        assertThat(query(publicSession(), "SHOW ROLE GRANTS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(getSession().getUser(), "USER", "accountadmin", "NO")
                .row(getSession().getUser(), "USER", "public", "NO");
        extraAdminRoles.forEach(roleName -> rowGrantsBuilder.row("accountadmin", "ROLE", roleName, "YES"));
        extraPublicRoles.forEach(roleName -> rowGrantsBuilder.row("public", "ROLE", roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query("SELECT * FROM system.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query("SELECT * FROM tpch.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    private void verifyUserRoles(Session session, Set<String> roles)
    {
        Set<String> allRoles = ImmutableSet.<String>builder()
                .add("public")
                .addAll(roles)
                .build();

        assertThat(query(session, "SHOW ROLE GRANTS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(allRoles));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(session.getUser(), "USER", "public", "NO");
        roles.forEach(roleName -> rowGrantsBuilder.row(session.getUser(), "USER", roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query(session, "SELECT * FROM system.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    private void verifyRoleGrants(Session session, ImmutableMultimap<TrinoPrincipal, String> grants)
    {
        Set<String> allRoles = ImmutableSet.<String>builder()
                .add("public")
                .addAll(grants.values())
                .build();

        assertThat(query(session, "SHOW ROLE GRANTS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(allRoles));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(session.getUser(), "USER", "public", "NO");
        grants.forEach((grantee, roleName) -> rowGrantsBuilder.row(grantee.getName(), grantee.getType().toString(), roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query(session, "SELECT * FROM system.information_schema.applicable_roles"))
                .result()
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    @Test
    public void testSchema()
    {
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());

        // admin: create schema and is visible
        // public: not visible
        assertUpdate("CREATE SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        assertThat(query("show create schema memory.my_schema"))
                .result().matches(result -> result.getOnlyValue().toString().contains("AUTHORIZATION ROLE accountadmin"));

        // public: granted on schema makes the schema visible
        assertUpdate("GRANT CREATE ON SCHEMA memory.my_schema TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));

        // public: revoked grant and verify schema is no longer visible
        assertUpdate("REVOKE create ON SCHEMA memory.my_schema FROM ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());

        // admin: create table in schema
        // public: schema is not visible
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());
        assertUpdate("CREATE TABLE memory.my_schema.my_table (my_column bigint)");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);

        // public: granted select on table, so schema is visible
        assertUpdate("GRANT select ON TABLE memory.my_schema.my_table TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(SELECT, false)));

        // public: table dropped with all grants, so schema is not visible
        assertUpdate("DROP TABLE memory.my_schema.my_table");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        // public: granted create on schema, so schema is visible
        assertUpdate("GRANT CREATE ON SCHEMA memory.my_schema TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));

        // admin and public: can not create table because of explicit deny
        assertUpdate("DENY CREATE ON SCHEMA memory.my_schema TO ROLE public");
        // TODO catalog visibility of DENY is broken
        // verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        assertQueryFails("CREATE TABLE memory.my_schema.anything (my_column bigint)", "Access Denied.*");
        assertQueryFails(publicSession(), "CREATE TABLE memory.my_schema.anything (my_column bigint)", "Access Denied.*");

        // We can't rename the schema since memory connector does not support it

        // public: schema dropped with all grants, so schema is not visible
        assertUpdate("DROP SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());

        // admin: create schema and is visible
        // public: not visible
        assertUpdate("CREATE SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());

        // admin: admin change owner to public role
        // public: visible
        assertUpdate("ALTER SCHEMA memory.my_schema SET AUTHORIZATION role public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));
        assertThat(query("show create schema memory.my_schema"))
                .result().matches(result -> result.getOnlyValue().toString().contains("AUTHORIZATION ROLE public"));

        // public: schema dropped with all grants, so schema is not visible
        assertUpdate(publicSession(), "DROP SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());
    }

    @Test
    public void testBranchPrivilegeSql()
    {
        assertUpdate("CREATE SCHEMA memory.branch_grants");
        assertUpdate("CREATE TABLE memory.branch_grants.branched (my_column bigint)");
        try {
            // With BRANCH_PRIVILEGES disabled (the account default), branch-scoped grants are
            // rejected, while GRANT/REVOKE ALL PRIVILEGES still work and silently skip
            // CREATE_BRANCH through the disabled-privileges filter
            assertQueryFails("GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE public", ".*Cannot grant privileges to branches at this time.*");
            assertUpdate("GRANT ALL PRIVILEGES ON TABLE memory.branch_grants.branched TO ROLE public");
            assertThat(grantedTablePrivileges("public", "branch_grants", "branched"))
                    .containsExactlyInAnyOrder("SELECT", "INSERT", "UPDATE", "DELETE", "MANAGE_DATA_OBSERVABILITY");
            assertUpdate("REVOKE ALL PRIVILEGES ON TABLE memory.branch_grants.branched FROM ROLE public");

            setBranchPrivilegesFlag(true);

            // With the flag enabled, ALL PRIVILEGES includes CREATE_BRANCH
            assertUpdate("GRANT ALL PRIVILEGES ON TABLE memory.branch_grants.branched TO ROLE public");
            assertThat(grantedTablePrivileges("public", "branch_grants", "branched"))
                    .containsExactlyInAnyOrder("SELECT", "INSERT", "UPDATE", "DELETE", "MANAGE_DATA_OBSERVABILITY", "CREATE_BRANCH");
            assertUpdate("REVOKE ALL PRIVILEGES ON TABLE memory.branch_grants.branched FROM ROLE public");

            // Branch grant management is connector independent, so it works even though the
            // memory connector cannot create branches
            assertUpdate("GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE public");
            assertUpdate("DENY INSERT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE public");
            assertUpdate("GRANT CREATE BRANCH ON TABLE memory.branch_grants.branched TO ROLE public");

            // an unprivileged role may not manage branch grants
            assertQueryFails(publicSession(), "GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE public", "(Access Denied|Operation not allowed).*");
            assertQueryFails(publicSession(), "REVOKE SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched FROM ROLE public", "(Access Denied|Operation not allowed).*");

            assertUpdate("REVOKE SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched FROM ROLE public");
            assertUpdate("REVOKE INSERT ON BRANCH dev IN TABLE memory.branch_grants.branched FROM ROLE public");
            assertUpdate("REVOKE CREATE BRANCH ON TABLE memory.branch_grants.branched FROM ROLE public");

            // a role with GRANT OPTION on a branch privilege can grant it onward; one without cannot.
            // Neither information_schema.table_privileges (keyed on the base table name) nor SHOW
            // GRANTS ON BRANCH (the rewrite ignores the branch qualifier entirely and shows the
            // table's grants -- a separate, pre-existing gap) surface branch-scoped grants, so this
            // checks the actual privilege store through the security API instead
            Session grantOptionSession = createUserAndGrantedRole("branch_grant_option_role");
            Session granteeSession = createUserAndGrantedRole("branch_grantee_role");
            assertUpdate("CREATE ROLE branch_grantee_role_2");
            assertUpdate("GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE branch_grant_option_role WITH GRANT OPTION");
            assertUpdate(grantOptionSession, "GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE branch_grantee_role");
            RoleId granteeRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession())).get(new RoleName("branch_grantee_role"));
            TableBranchId devBranchId = new TableBranchId(testingAccountClient.getOrCreateCatalog("memory"), "branch_grants", "branched", "dev");
            Set<String> granteePrivileges = getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(getSession()), granteeRoleId, devBranchId).getPrivileges().stream()
                    .map(privilege -> privilege.getPrivilege().name())
                    .collect(toImmutableSet());
            assertThat(granteePrivileges).contains("SELECT");
            // branch_grantee_role holds the privilege but not the grant option, so it cannot grant it onward
            assertQueryFails(granteeSession, "GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE branch_grantee_role_2", "(Access Denied|Operation not allowed).*");
            assertQueryFails(publicSession(), "GRANT SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched TO ROLE branch_grantee_role", "(Access Denied|Operation not allowed).*");
            assertUpdate("DROP ROLE branch_grantee_role_2");
            assertUpdate("REVOKE SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched FROM ROLE branch_grantee_role");
            assertUpdate("REVOKE GRANT OPTION FOR SELECT ON BRANCH dev IN TABLE memory.branch_grants.branched FROM ROLE branch_grant_option_role");
            assertUpdate("DROP ROLE branch_grantee_role");
            assertUpdate("DROP ROLE branch_grant_option_role");
        }
        finally {
            setBranchPrivilegesFlag(false);
            assertUpdate("DROP TABLE memory.branch_grants.branched");
            assertUpdate("DROP SCHEMA memory.branch_grants");
        }
    }

    private List<String> grantedTablePrivileges(String grantee, String schema, String table)
    {
        return computeActual("SELECT privilege_type FROM memory.information_schema.table_privileges" +
                " WHERE grantee = '%s' AND table_schema = '%s' AND table_name = '%s'".formatted(grantee, schema, table))
                .getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    // Toggles BRANCH_PRIVILEGES and waits until the servers observe it. The testing server mains
    // forward only the db.* properties, so the access-control-server's flag cache refreshes on the
    // production default of one minute, after which the next privilege check reloads the role
    // cache with the new flags
    private void setBranchPrivilegesFlag(boolean enabled)
    {
        testingAccountClient.upsertAccountFeatureFlag("BRANCH_PRIVILEGES", enabled);
        assertEventually(new Duration(90, SECONDS), new Duration(2, SECONDS), () ->
                assertThat(branchGrantsAccepted()).isEqualTo(enabled));
    }

    private boolean branchGrantsAccepted()
    {
        try {
            assertUpdate("GRANT SELECT ON BRANCH flag_probe IN TABLE memory.branch_grants.branched TO ROLE public");
            assertUpdate("REVOKE SELECT ON BRANCH flag_probe IN TABLE memory.branch_grants.branched FROM ROLE public");
            return true;
        }
        catch (Throwable ignored) {
            return false;
        }
    }

    private void verifySchemaVisibility(Set<String> extraAdminSchemas, Set<String> extraPublicSchemas)
    {
        Set<String> adminSchemas = ImmutableSet.<String>builder()
                .add("information_schema")
                .add("tiny")
                .add("default")
                .addAll(extraAdminSchemas)
                .build();
        Set<String> publicSchemas;
        if (extraPublicSchemas.isEmpty()) {
            publicSchemas = ImmutableSet.of();
        }
        else {
            publicSchemas = ImmutableSet.<String>builder()
                    .add("information_schema")
                    .addAll(extraPublicSchemas)
                    .build();
        }

        assertThat(query("SHOW CATALOGS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(ImmutableSet.<String>builder()
                        .add("system")
                        .add("tpch")
                        .add("memory")
                        .addAll(extraCatalogs())
                        .build()));
        assertThat(query(publicSession(), "SHOW CATALOGS"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(ImmutableSet.<String>builder()
                        .add("system")
                        .add("tpch")
                        .addAll(extraPublicSchemas.isEmpty() ? ImmutableSet.of() : ImmutableSet.of("memory"))
                        .build()));

        assertThat(query("SHOW SCHEMAS FROM memory"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SHOW SCHEMAS FROM memory"))
                .result()
                .matches(varcharColumnResult(publicSchemas));

        assertThat(query("SELECT schema_name FROM memory.information_schema.schemata"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SELECT schema_name FROM memory.information_schema.schemata"))
                .result()
                .matches(varcharColumnResult(publicSchemas));

        assertThat(query("SHOW SCHEMAS FROM memory"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SHOW SCHEMAS FROM memory"))
                .result()
                .matches(varcharColumnResult(publicSchemas));
    }

    @Test
    public void testTable()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");
        assertUpdate("CREATE TABLE memory.my_schema.my_table (my_column varchar)");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        // GRANT INSERT
        assertUpdate("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('a')", 1);
        assertQueryFails(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('b')", "Access Denied.*");

        assertUpdate("GRANT INSERT ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(INSERT, false)));

        assertUpdate("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('c')", 1);
        assertUpdate(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('d')", 1);

        assertThat(query("select * from memory.my_schema.my_table"))
                .result()
                .matches(varcharColumnResult("a", "c", "d"));
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "line 1:8: Relation not found or not allowed");
        // INSERT makes the table visible, but its columns stay hidden without SELECT
        assertThat(query(publicSession(), MY_TABLE_COLUMNS_QUERY)).returnsEmptyResult();

        // memory does not support delete, but we can check if the security checks passed using the exception message
        assertQueryFails("DELETE FROM memory.my_schema.my_table where my_column = 'a'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "DELETE FROM memory.my_schema.my_table where  my_column = 'a'", "Access Denied.*");

        assertUpdate("GRANT DELETE ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false)));

        assertQueryFails("DELETE FROM memory.my_schema.my_table where my_column = 'a'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "DELETE FROM memory.my_schema.my_table where  my_column = 'a'", "This connector does not support modifying table rows");

        assertThat(query("select * from memory.my_schema.my_table"))
                .result()
                .matches(varcharColumnResult("a", "c", "d"));

        // GRANT UPDATE
        // memory does not support update, but we can check if the security checks passed using the exception message
        assertQueryFails("UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "UPDATE memory.my_schema.my_table SET my_column = 'changed'", "Access Denied.*");

        assertUpdate("GRANT UPDATE ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility(
                "public",
                publicSession(),
                ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        assertQueryFails("UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");

        assertThat(query("select * from memory.my_schema.my_table"))
                .result()
                .matches(varcharColumnResult("a", "c", "d"));

        // GRANT SELECT
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "line 1:8: Relation not found or not allowed");
        assertUpdate("GRANT SELECT ON TABLE memory.my_schema.my_table TO ROLE public");
        assertThat(query(publicSession(), "select * from memory.my_schema.my_table"))
                .result()
                .matches(varcharColumnResult("a", "c", "d"));
        assertThat(query(publicSession(), MY_TABLE_COLUMNS_QUERY))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("my_column"));

        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility(
                "public",
                publicSession(),
                ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false), new PrivilegeInfo(SELECT, false)));

        // REVOKE SELECT
        assertUpdate("REVOKE SELECT ON TABLE memory.my_schema.my_table FROM ROLE public");
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "line 1:8: Relation not found or not allowed");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility(
                "public",
                publicSession(),
                ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // DENY INSERT
        assertUpdate("DENY INSERT ON TABLE memory.my_schema.my_table TO ROLE public");
        assertQueryFails("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('fail')", "Access Denied.*");
        assertQueryFails(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('fail')", "Access Denied.*");

        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility(
                "public",
                publicSession(),
                ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // rename table
        assertUpdate("ALTER TABLE memory.my_schema.my_table RENAME TO  memory.my_schema.renamed_table");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(new SchemaTableName(MY_SCHEMA, "renamed_table")), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility(
                "public",
                publicSession(),
                ImmutableSet.of(new SchemaTableName(MY_SCHEMA, "renamed_table")),
                ImmutableSet.of(new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // change owner
        assertQueryFails(publicSession(), "DROP TABLE memory.my_schema.renamed_table", "Access Denied.*");
        assertUpdate("ALTER TABLE memory.my_schema.renamed_table  SET AUTHORIZATION role public");
        assertUpdate(publicSession(), "DROP TABLE memory.my_schema.renamed_table");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    @Test
    public void testFunctionSetAuthorization()
    {
        // test setting the owner of a system function
        assertQueryFails(publicSession(), "ALTER FUNCTION memory.system.unload SET AUTHORIZATION ROLE public", "Operation not allowed.*");
        assertQueryFails(adminSession(), "ALTER FUNCTION memory.system.unload SET AUTHORIZATION USER \"admin@example.com\"", "Galaxy only supports a ROLE as an owner");
        assertQuerySucceeds(adminSession(), "ALTER FUNCTION memory.system.unload SET AUTHORIZATION ROLE public");
        assertQuerySucceeds(publicSession(), "ALTER FUNCTION memory.system.unload SET AUTHORIZATION ROLE accountadmin");
    }

    private void verifyTableVisibility(String roleName, Session session, Set<SchemaTableName> visibleTables, Set<PrivilegeInfo> privileges)
    {
        assertThat(privileges.isEmpty() || !visibleTables.isEmpty())
                .overridingErrorMessage("visible table are required when privileges are provided")
                .isTrue();

        Set<String> tableSchemas = visibleTables.stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableSet());
        for (String adminTableSchema : tableSchemas) {
            assertThat(query(session, "SHOW TABLES FROM memory." + adminTableSchema))
                    .result()
                    .skippingTypesCheck()
                    .matches(varcharColumnResult(visibleTables.stream()
                            .filter(name -> adminTableSchema.equals(name.getSchemaName()))
                            .map(SchemaTableName::getTableName)
                            .collect(toImmutableSet())));
        }

        @Language("SQL")
        String selectAllVisibleTables = "" +
                "SELECT table_schema || '.' || table_name AS name " +
                "FROM memory.information_schema.tables " +
                "WHERE table_schema not in ('information_schema', 'tiny')";

        assertThat(query(session, selectAllVisibleTables))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(visibleTables.stream()
                        .map(SchemaTableName::toString)
                        .collect(toImmutableSet())));

        @Language("SQL")
        String selectAllJdbcTables = "" +
                "SELECT table_schem || '.' || table_name AS name " +
                "FROM system.jdbc.tables " +
                "WHERE table_cat = 'memory' AND table_schem not in ('information_schema', 'tiny')";

        assertThat(query(session, selectAllJdbcTables))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult(visibleTables.stream()
                        .map(SchemaTableName::toString)
                        .collect(toImmutableSet())));

        @Language("SQL")
        String selectTableGrants = "" +
                "SELECT privilege_type, is_grantable, grantee_type, grantee, table_schema, table_name " +
                "FROM memory.information_schema.table_privileges " +
                "WHERE table_schema not in ('information_schema', 'tiny') " +
                "  AND grantee = '" + roleName + "'";

        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        for (SchemaTableName visibleTable : visibleTables) {
            for (PrivilegeInfo privilege : privileges) {
                rowGrantsBuilder.row(
                        privilege.getPrivilege().name(),
                        privilege.isGrantOption() ? "YES" : "NO",
                        "ROLE",
                        roleName,
                        visibleTable.getSchemaName(),
                        visibleTable.getTableName());
            }
        }
        assertThat(query(session, selectTableGrants))
                .result()
                .skippingTypesCheck()
                .matches(rowGrantsBuilder.build());
    }

    @Test
    public void testWildcards()
    {
        assertQueryFails("GRANT CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE CREATE ON SCHEMA \"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertUpdate("GRANT CREATE ON SCHEMA memory.\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY CREATE ON SCHEMA memory.\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE CREATE ON SCHEMA memory.\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT CREATE ON SCHEMA unknown.\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("DENY CREATE ON SCHEMA unknown.\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("REVOKE CREATE ON SCHEMA unknown.\"*\" FROM ROLE PUBLIC", ".*Catalog 'unknown' does not exist");

        assertQueryFails("GRANT CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE CREATE ON SCHEMA \"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertQueryFails("GRANT SELECT ON TABLE \"*\".\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY SELECT ON TABLE \"*\".\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE SELECT ON TABLE \"*\".\"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertUpdate("GRANT SELECT ON TABLE memory.\"*\".\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY SELECT ON TABLE memory.\"*\".\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE SELECT ON TABLE memory.\"*\".\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT SELECT ON TABLE unknown.\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("DENY SELECT ON TABLE unknown.\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE unknown.\"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog 'unknown' does not exist");

        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertQueryFails("GRANT SELECT ON TABLE memory.\"*\".my_table TO ROLE PUBLIC", ".*Schema wildcard requires a table wildcard.*");
        assertQueryFails("DENY SELECT ON TABLE memory.\"*\".my_table TO ROLE PUBLIC", ".*Schema wildcard requires a table wildcard.*");
        assertQueryFails("REVOKE SELECT ON TABLE memory.\"*\".my_table FROM ROLE PUBLIC", ".*Schema wildcard requires a table wildcard.*");

        assertUpdate("GRANT SELECT ON TABLE memory.my_schema.\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY SELECT ON TABLE memory.my_schema.\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE SELECT ON TABLE memory.my_schema.\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT SELECT ON TABLE memory.unknown.\"*\" TO ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");
        assertQueryFails("DENY SELECT ON TABLE memory.unknown.\"*\" TO ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE memory.unknown.\"*\" FROM ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");

        assertQueryFails("GRANT SELECT ON TABLE unknown.my_schema.\"*\" TO ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");
        assertQueryFails("DENY SELECT ON TABLE unknown.my_schema.\"*\" TO ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE unknown.my_schema.\"*\" FROM ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    @Test
    public void testView()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertUpdate("CREATE VIEW memory.my_schema.my_view AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        assertUpdate("ALTER VIEW memory.my_schema.my_view SET AUTHORIZATION ROLE PUBLIC");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        assertUpdate("DROP VIEW memory.my_schema.my_view");

        assertUpdate("CREATE VIEW memory.my_schema.my_view SECURITY INVOKER AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("value"));
        assertUpdate("ALTER VIEW memory.my_schema.my_view SET AUTHORIZATION ROLE PUBLIC");
        assertUpdate(publicSession(), "DROP VIEW memory.my_schema.my_view");

        // Refresh view
        assertUpdate("CREATE TABLE memory.my_schema.my_table AS SELECT 'value' AS my_value", 1);
        assertUpdate("CREATE VIEW memory.my_schema.my_view AS SELECT * FROM memory.my_schema.my_table");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("value"));
        assertUpdate("ALTER TABLE memory.my_schema.my_table RENAME COLUMN my_value TO my_renamed_value");
        assertUpdate("ALTER VIEW memory.my_schema.my_view REFRESH");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("value"));
        assertUpdate("ALTER VIEW memory.my_schema.my_view SET AUTHORIZATION ROLE PUBLIC");
        assertUpdate("DROP VIEW memory.my_schema.my_view");
        assertUpdate("DROP TABLE memory.my_schema.my_table");

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    @Test
    public void testViewPrivileges()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertUpdate("CREATE VIEW memory.my_schema.my_view AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .result()
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        MaterializedResult emptyResult = MaterializedResult.resultBuilder(getSession(), createVarcharType(5)).build();

        // Revoke all privileges to make it easy to identify added ones
        assertUpdate("REVOKE ALL PRIVILEGES ON memory.my_schema.my_view FROM ROLE accountadmin");
        assertThat(query("SHOW GRANTS ON memory.my_schema.my_view")).result().skippingTypesCheck().matches(emptyResult);

        assertUpdate("CREATE ROLE view_privileges_role");

        for (Privilege privilege : ImmutableList.of(SELECT, UPDATE, INSERT, DELETE)) {
            // Show that we can grant the privilege on the view to a role
            assertUpdate("GRANT %s ON memory.my_schema.my_view TO ROLE view_privileges_role".formatted(privilege));
            assertThat(query("SHOW GRANTS ON memory.my_schema.my_view"))
                    .skippingTypesCheck()
                    .matches("VALUES('view_privileges_role', 'TABLE', 'memory.my_schema.my_view', 'ALLOW', '%s', 'false', 'accountadmin', 'true')".formatted(privilege));
            assertUpdate("REVOKE %s ON memory.my_schema.my_view FROM ROLE view_privileges_role".formatted(privilege));

            // Show that we can deny the privilege...
            assertUpdate("DENY %s ON memory.my_schema.my_view TO ROLE view_privileges_role".formatted(privilege));

            // And here is the denied privilege
            assertThat(query("SHOW GRANTS ON memory.my_schema.my_view")).result().skippingTypesCheck()
                    .matches("VALUES('view_privileges_role', 'TABLE', 'memory.my_schema.my_view', 'DENY', '%s', 'false', 'accountadmin', 'true')".formatted(privilege));

            // Revoke it
            assertUpdate("REVOKE %s ON memory.my_schema.my_view FROM ROLE view_privileges_role".formatted(privilege));
        }

        // Clean up
        assertUpdate("DROP view memory.my_schema.my_view");
        assertUpdate("DROP schema memory.my_schema");
        assertUpdate("DROP ROLE view_privileges_role");
    }

    @Test
    public void testSetAuthorizationUser()
    {
        uncontainedEntitySetAuthorizationTest(EntityKind.USER.name(), "admin@example.com");
    }

    @Test
    public void testSetAuthorizationColumnMask()
    {
        ColumnMaskId columnMaskId = getTestingAccountClient().createColumnMask("mymask", "expression", ColumnMaskType.ANY, toDispatchSession(adminSession()).roleId());
        uncontainedEntitySetAuthorizationTest(EntityKind.COLUMN_MASK.name(), "mymask");
        getTestingAccountClient().deleteColumnMask(columnMaskId);
    }

    @Test
    public void testSetAuthorizationRowFilter()
    {
        RowFilterId rowFilterId = getTestingAccountClient().createRowFilter("myfilter", "expression", toDispatchSession(adminSession()).roleId());
        uncontainedEntitySetAuthorizationTest(EntityKind.ROW_FILTER.name(), "myfilter");
        getTestingAccountClient().deleteRowFilter(rowFilterId);
    }

    @Test
    public void testSetAuthorizationTag()
    {
        TagId tagId = getTestingAccountClient().createTag("mytag");
        uncontainedEntitySetAuthorizationTest(EntityKind.TAG.name(), "mytag");
        getTestingAccountClient().deleteTag(tagId);
    }

    @Test
    public void testSetAuthorizationRole()
    {
        assertUpdate("CREATE ROLE role_x");
        uncontainedEntitySetAuthorizationTest(EntityKind.ROLE.name(), "role_x");
        assertUpdate("DROP ROLE role_x");
    }

    @Test
    public void testSetAuthorizationCluster()
    {
        ClusterId someClusterId = getTestingAccountClient().getSampleClusterId();
        Map<String, ClusterId> accountClusters = getTrinoSecurityApi().getAccountClusterNamesAndIds(toDispatchSession(adminSession()));
        String clusterName = accountClusters.entrySet().stream()
                .filter(entry -> entry.getValue().equals(someClusterId))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No cluster found for id: " + someClusterId));
        uncontainedEntitySetAuthorizationTest(EntityKind.CLUSTER.name(), clusterName);
    }

    @Test
    public void testSetAuthorizationUnsupportedEntities()
    {
        assertQueryFails(adminSession(), "ALTER MY ACCOUNT SET AUTHORIZATION ROLE public", "Galaxy does not support MY entity authorization");
        assertQueryFails(adminSession(), "ALTER NOTANENTITY foo SET AUTHORIZATION ROLE public", "Galaxy does not support NOTANENTITY entity authorization");
        assertQueryFails(adminSession(), "ALTER ACCOUNT foo SET AUTHORIZATION ROLE public", "Galaxy does not support ACCOUNT entity authorization");
        assertQueryFails(adminSession(), "ALTER ANY \"any\" SET AUTHORIZATION ROLE public", "Galaxy does not support ANY entity authorization");
        assertQueryFails(adminSession(), "ALTER COLUMN \"memory\".\"schema\".\"table\".\"column\" SET AUTHORIZATION ROLE public", "Galaxy does not support COLUMN entity authorization");
    }

    @Test
    public void testSetAuthorizationPolicy()
    {
        String catalogName = "memory";
        RowFilterId rowFilterId = getTestingAccountClient().createRowFilter("test_filter", "test_filter_description", toDispatchSession(adminSession()).roleId());
        PolicyId policyId = getTestingAccountClient().createRowFilterPolicy("mypolicy", "%s.*.*.*".formatted(catalogName), "true", toDispatchSession(publicSession()).roleId(), rowFilterId);
        uncontainedEntitySetAuthorizationTest(EntityKind.POLICY.name(), "mypolicy");
        getTestingAccountClient().deletePolicy(policyId);
        getTestingAccountClient().deleteRowFilter(rowFilterId);
    }

    @Test
    public void testQuery()
    {
        assertUpdate("CREATE ROLE my_role");
        RoleId myRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("my_role"));
        Session newUserSession = newUserSession(myRoleId);

        String randomValue = UUID.randomUUID().toString();
        getQueryRunner().execute(newUserSession, "SELECT '" + randomValue + "'");
        String queryId = getQueryRunner().execute(newUserSession, "SELECT query_id from system.runtime.queries where query = 'SELECT ''" + randomValue + "'''").getOnlyValue().toString();

        @Language("SQL")
        String findQuerySql = "SELECT 'found' FROM system.runtime.queries WHERE query_id = '" + queryId + "'";
        // user can always see their own query
        assertThat(query(newUserSession, findQuerySql))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        // admin can see query because it has my_role active
        assertThat(query(findQuerySql))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        // public can not see query
        assertThat(query(publicSession(), findQuerySql)).returnsEmptyResult();

        // create another user/role, they cannot see the query
        Session anotherUserSession = newUserSession("public");
        assertThat(query(anotherUserSession, findQuerySql)).returnsEmptyResult();

        // grant public special VIEW_ALL_QUERY_HISTORY account privilege, and both public role sessions can see the query
        TestingAccountClient accountClient = getTestingAccountClient();
        GrantDetails queryHistoryGrant = new GrantDetails(VIEW_ALL_QUERY_HISTORY, accountClient.getPublicRoleId(), ALLOW, false, accountClient.getAccountId());
        accountClient.grantAccountPrivilege(queryHistoryGrant);

        assertThat(query(publicSession(), findQuerySql))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        assertThat(query(anotherUserSession, findQuerySql))
                .result()
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));

        // user can always kill their own query
        assertQueryFails(newUserSession, "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Target query is not running.*");
        // admin can kill query because it has my_role active
        assertQueryFails("CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Target query is not running.*");
        // public role can not kill the query
        assertQueryFails(publicSession(), "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Access Denied: Cannot kill query.*");
        assertQueryFails(anotherUserSession, "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Access Denied: Cannot kill query.*");

        accountClient.revokeAccountPrivilege(queryHistoryGrant);
        assertUpdate("DROP ROLE my_role");
    }

    @Test
    public void testDefinerViews()
    {
        assertUpdate("CREATE SCHEMA memory.definer_views");
        assertUpdate("CREATE TABLE memory.definer_views.test_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate("INSERT INTO memory.definer_views.test_table VALUES('test_update', true)", 1);

        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();
        assertThat(query("SELECT * FROM memory.definer_views.test_table"))
                .result()
                .matches(tableContents);

        // Create a view of the table
        assertUpdate("CREATE VIEW memory.definer_views.test_view AS SELECT * FROM memory.definer_views.test_table");

        // Verify by select from the view
        assertThat(query("SELECT * FROM memory.definer_views.test_view"))
                .result()
                .matches(tableContents);

        // Make a view_user role
        Session viewUserSession = createUserAndGrantedRole("view_user");

        // Show that before a SELECT grant on the view, view_user can't select from the view
        assertThat(query(viewUserSession, "SELECT * FROM memory.definer_views.test_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("line 1:8: Relation not found or not allowed");

        // Grant SELECT on the view to the view_user
        assertUpdate("GRANT SELECT ON TABLE memory.definer_views.test_view TO ROLE view_user with GRANT OPTION");

        // When the user assumes role view_user, she can read through the view
        assertThat(query(viewUserSession, "SELECT * FROM memory.definer_views.test_view"))
                .result()
                .matches(tableContents);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.test_view");
        assertUpdate("DROP TABLE memory.definer_views.test_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE view_user");
    }

    @Test
    public void testDefinerViewFunctions()
    {
        assertUpdate("CREATE SCHEMA memory.definer_views");
        assertUpdate("CREATE TABLE memory.definer_views.test_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate("INSERT INTO memory.definer_views.test_table VALUES('test_update', true)", 1);

        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();
        assertThat(query("SELECT * FROM memory.definer_views.test_table"))
                .result()
                .matches(tableContents);

        // Create a view of the table
        assertUpdate("CREATE VIEW memory.definer_views.test_view AS SELECT hamming_distance(col1, 'Test_Update') AS hamming FROM memory.definer_views.test_table");

        // Verify by select from the view, which calls the function
        assertThat(computeActual("SELECT * FROM memory.definer_views.test_view").getOnlyValue()).isEqualTo(2L);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.test_view");
        assertUpdate("DROP TABLE memory.definer_views.test_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
    }

    @Test
    public void testViewGrantOptions()
    {
        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.definer_views");
        Session baseTableOwnerSession = createUserAndGrantedRole("base_table_owner");
        Session innerViewOwnerSessionNoGrantOption = createUserAndGrantedRole("inner_view_owner_no_grant_option");
        Session outerViewOwnerSession = createUserAndGrantedRole("outer_view_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE base_table_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE inner_view_owner_no_grant_option");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE outer_view_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(baseTableOwnerSession, "CREATE TABLE memory.definer_views.base_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate(baseTableOwnerSession, "INSERT INTO memory.definer_views.base_table VALUES('test_update', true)", 1);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.base_table")).result().matches(tableContents);

        // originally, the views cannot be created
        assertThat(query(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_definer AS SELECT * FROM memory.definer_views.base_table"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:79: Relation not found or not allowed");
        assertThat(query(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_invoker SECURITY INVOKER AS SELECT * FROM memory.definer_views.base_table"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:96: Relation not found or not allowed");

        // grant select without the grant option
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option");

        // The views are able to be created without the grant option
        assertUpdate(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_definer AS SELECT * FROM memory.definer_views.base_table");
        assertUpdate(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_invoker SECURITY INVOKER AS SELECT * FROM memory.definer_views.base_table");

        // they are also allowed to be queried when the view owner queries them
        assertThat(query(innerViewOwnerSessionNoGrantOption, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).result().matches(tableContents);
        assertThat(query(innerViewOwnerSessionNoGrantOption, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).result().matches(tableContents);

        // however, without the grant option, another role won't be able to query the view
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.inner_view_no_grant_option_invoker': line 1:8: Relation not found or not allowed");

        // give other role SELECT on the view, and the views will still fail:
        //  - definer view requires inner_view_owner_no_grant_option have SELECT WITH GRANT OPTION on the underlying table
        //  - invoker view requires outer_view_owner have SELECT on the underlying table
        assertUpdate(innerViewOwnerSessionNoGrantOption, "GRANT SELECT ON TABLE memory.definer_views.inner_view_no_grant_option_definer TO ROLE outer_view_owner");
        assertUpdate(innerViewOwnerSessionNoGrantOption, "GRANT SELECT ON TABLE memory.definer_views.inner_view_no_grant_option_invoker TO ROLE outer_view_owner");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Access Denied: View owner does not have sufficient privileges: View owner 'inner_view_owner_no_grant_option' cannot create view that selects from memory.definer_views.base_table: Role inner_view_owner_no_grant_option does not have the privilege SELECT WITH GRANT OPTION on the columns [col2, col1]");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.inner_view_no_grant_option_invoker': line 1:8: Relation not found or not allowed");

        // grant with grant option, and the definer view can now be queried, but invoker still needs the grant
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option WITH GRANT OPTION");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).result().matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.inner_view_no_grant_option_invoker': line 1:8: Relation not found or not allowed");

        // give base table access to outer_view_owner, and now querying should work for outer_view_owner role
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE outer_view_owner");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).result().matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).result().matches(tableContents);

        // make sure this doesn't work for base_table_owner, as they don't have view privileges
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");

        // grant inner_view_owner_no_grant_option to base_table_owner role and querying the views should succeed
        assertUpdate("GRANT inner_view_owner_no_grant_option TO ROLE base_table_owner");
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).result().matches(tableContents);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).result().matches(tableContents);

        // revoke the grant option -- now querying by outer_view_owner should fail again,
        // but base_table_owner should still succeed, as it has the view owner in its active role set
        assertUpdate(baseTableOwnerSession, "REVOKE SELECT ON TABLE memory.definer_views.base_table FROM ROLE inner_view_owner_no_grant_option");
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option");

        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).result().matches(tableContents);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).result().matches(tableContents);

        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Access Denied: View owner does not have sufficient privileges: View owner 'inner_view_owner_no_grant_option' cannot create view that selects from memory.definer_views.base_table: Role inner_view_owner_no_grant_option does not have the privilege SELECT WITH GRANT OPTION on the columns [col2, col1]");
        // invoker is still good
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).result().matches(tableContents);

        // revoke the role grant and the definer view isn't accessible to either role anymore
        assertUpdate("REVOKE inner_view_owner_no_grant_option FROM ROLE base_table_owner");
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Access Denied: View owner does not have sufficient privileges: View owner 'inner_view_owner_no_grant_option' cannot create view that selects from memory.definer_views.base_table: Role inner_view_owner_no_grant_option does not have the privilege SELECT WITH GRANT OPTION on the columns [col2, col1]");

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.inner_view_no_grant_option_invoker");
        assertUpdate("DROP VIEW memory.definer_views.inner_view_no_grant_option_definer");
        assertUpdate("DROP TABLE memory.definer_views.base_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE outer_view_owner");
        assertUpdate("DROP ROLE inner_view_owner_no_grant_option");
        assertUpdate("DROP ROLE base_table_owner");
    }

    /*
     * This entire test does not work because of https://github.com/starburstdata/stargate/issues/15073 and https://github.com/starburstdata/stargate/issues/15239
     */
    @Test
    @Disabled
    public void testViewColumnAccess()
    {
        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, BOOLEAN)
                .row("abc", "def", true)
                .build();
        MaterializedResult col1Col2Contents = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR)
                .row("abc", "def")
                .build();
        MaterializedResult col1Contents = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("abc")
                .build();

        assertUpdate("CREATE SCHEMA memory.views_test");
        Session baseTableOwnerSession = createUserAndGrantedRole("base_table_owner");
        Session viewOwnerSession = createUserAndGrantedRole("view_owner");
        Session viewViewerSession = createUserAndGrantedRole("view_viewer");
        assertUpdate("GRANT CREATE ON schema views_test TO ROLE base_table_owner");
        assertUpdate("GRANT CREATE ON schema views_test TO ROLE view_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(baseTableOwnerSession, "CREATE TABLE memory.views_test.base_table(col1 VARCHAR, col2 VARCHAR, col3 BOOLEAN)");
        assertUpdate(baseTableOwnerSession, "INSERT INTO memory.views_test.base_table VALUES('abc', 'def', true)", 1);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.views_test.base_table")).result().matches(tableContents);

        // Grant SELECT col1 and col2 on base_table to view_owner with grant option
        CatalogId memoryCatalogId = testingAccountClient.getOrCreateCatalog("memory");
        getTrinoSecurityApi().addEntityPrivileges(
                toDispatchSession(baseTableOwnerSession),
                new ColumnId(memoryCatalogId, "views_test", "base_table", "col1"),
                ImmutableSet.of(new CreateEntityPrivilege(io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT, ALLOW, new RoleName("view_owner"), true)));
        getTrinoSecurityApi().addEntityPrivileges(
                toDispatchSession(baseTableOwnerSession),
                new ColumnId(memoryCatalogId, "views_test", "base_table", "col2"),
                ImmutableSet.of(new CreateEntityPrivilege(io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT, ALLOW, new RoleName("view_owner"), true)));
        assertThat(query(viewOwnerSession, "SELECT * FROM memory.views_test.base_table")).result().matches(col1Col2Contents);

        // Create new definer and invoker views
        assertUpdate(viewOwnerSession, "CREATE VIEW memory.views_test.definer_view AS SELECT * FROM memory.views_test.base_table");
        assertUpdate(viewOwnerSession, "CREATE VIEW memory.views_test.invoker_view SECURITY INVOKER AS SELECT * FROM memory.views_test.base_table");

        // view_owner sees only col1 when querying the views
        assertThat(query(viewOwnerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Col2Contents);
        assertThat(query(viewOwnerSession, "SELECT * FROM memory.views_test.invoker_view")).result().matches(col1Col2Contents);

        // view_viewer cannot query the views
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.views_test.invoker_view': line 1:8: Relation not found or not allowed");

        // Grant view_viewer SELECT on the views. The definer view can be queried, but not the invoker
        assertUpdate("GRANT SELECT ON TABLE memory.views_test.definer_view TO ROLE view_viewer");
        assertUpdate("GRANT SELECT ON TABLE memory.views_test.invoker_view TO ROLE view_viewer");
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Col2Contents);
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.views_test.invoker_view': line 1:8: Relation not found or not allowed");

        // Grant view_viewer SELECT on the base table.
        assertUpdate("GRANT SELECT ON TABLE memory.views_test.base_table TO ROLE view_viewer");
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Col2Contents);
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view")).result().matches(col1Col2Contents);

        // Revoke view_viewer SELECT on the base table and then grant it SELECT on col1
        assertUpdate("REVOKE SELECT ON TABLE memory.views_test.base_table FROM ROLE view_viewer");
        getTrinoSecurityApi().addEntityPrivileges(
                toDispatchSession(baseTableOwnerSession),
                new ColumnId(memoryCatalogId, "views_test", "base_table", "col1"),
                ImmutableSet.of(new CreateEntityPrivilege(io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT, ALLOW, new RoleName("view_viewer"), true)));
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Contents);
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view")).result().matches(col1Contents);

        // Revoke view_viewer SELECT on both views. Grant SELECT on col1 to both views.
        assertUpdate("REVOKE SELECT ON TABLE memory.views_test.definer_view FROM ROLE view_viewer");
        assertUpdate("REVOKE SELECT ON TABLE memory.views_test.invoker_view FROM ROLE view_viewer");
        getTrinoSecurityApi().addEntityPrivileges(
                toDispatchSession(baseTableOwnerSession),
                new ColumnId(memoryCatalogId, "views_test", "definer_view", "col1"),
                ImmutableSet.of(new CreateEntityPrivilege(io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT, ALLOW, new RoleName("view_viewer"), true)));
        getTrinoSecurityApi().addEntityPrivileges(
                toDispatchSession(baseTableOwnerSession),
                new ColumnId(memoryCatalogId, "views_test", "invoker_view", "col1"),
                ImmutableSet.of(new CreateEntityPrivilege(io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT, ALLOW, new RoleName("view_viewer"), true)));
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Contents);
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view")).result().matches(col1Contents);

        // Grant view_viewer SELECT on the base table again
        assertUpdate("GRANT SELECT ON TABLE memory.views_test.base_table TO ROLE view_viewer");
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.definer_view")).result().matches(col1Contents);
        assertThat(query(viewViewerSession, "SELECT * FROM memory.views_test.invoker_view")).result().matches(col1Contents);

        // Clean up
        assertUpdate("DROP VIEW memory.views_test.invoker_view");
        assertUpdate("DROP VIEW memory.views_test.definer_view");
        assertUpdate("DROP TABLE memory.views_test.base_table");
        assertUpdate("DROP SCHEMA memory.views_test");
        assertUpdate("DROP ROLE view_viewer");
        assertUpdate("DROP ROLE view_owner");
        assertUpdate("DROP ROLE base_table_owner");
    }

    @Test
    public void testNestedViews()
    {
        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.definer_views");
        Session baseTableOwnerSession = createUserAndGrantedRole("base_table_owner");
        Session innerViewOwnerSession = createUserAndGrantedRole("inner_view_owner");
        Session outerViewOwnerSession = createUserAndGrantedRole("outer_view_owner");
        Session testRoleSession = createUserAndGrantedRole("test_role");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE base_table_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE inner_view_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE outer_view_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(baseTableOwnerSession, "CREATE TABLE memory.definer_views.base_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate(baseTableOwnerSession, "INSERT INTO memory.definer_views.base_table VALUES('test_update', true)", 1);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.base_table")).result().matches(tableContents);

        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner WITH GRANT OPTION");

        // Create the inner view and verify contents
        assertUpdate(innerViewOwnerSession, "CREATE VIEW memory.definer_views.inner_view AS SELECT * FROM memory.definer_views.base_table");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).result().matches(tableContents);

        // Grant select on inner_view to role outer_view_owner with grant option
        assertUpdate(innerViewOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.inner_view TO ROLE outer_view_owner WITH GRANT OPTION");

        // Create the outer view and verify contents
        assertUpdate(outerViewOwnerSession, "CREATE VIEW memory.definer_views.outer_view AS SELECT * FROM memory.definer_views.inner_view");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);

        // Verify that outer_view_owner cannot access base_table
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.base_table"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("line 1:8: Relation not found or not allowed");

        // Grant SELECT on outer_view to test_role and verify contents
        assertUpdate(outerViewOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.outer_view TO ROLE test_role");
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);

        // Verify that test_role cannot access either inner_view or base_table
        for (String tableName : ImmutableList.of("inner_view", "base_table")) {
            assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views." + tableName))
                    .failure()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("line 1:8: Relation not found or not allowed");
        }

        // Revoke the SELECT grant to inner_view_owner on the base_table and verify that
        // neither inner_view_owner or outer_view_owner or test_role can access the view.
        assertUpdate(innerViewOwnerSession, "REVOKE SELECT ON TABLE memory.definer_views.base_table FROM ROLE inner_view_owner");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.inner_view': line 1:8: Relation not found or not allowed");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.outer_view': line 3:3: Failed analyzing stored view 'memory.definer_views.inner_view': line 1:8: Relation not found or not allowed");
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.outer_view': line 3:3: Failed analyzing stored view 'memory.definer_views.inner_view': line 1:8: Relation not found or not allowed");

        // Grant it again and they all work again
        assertUpdate(getSession(), "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner WITH GRANT OPTION");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).result().matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);

        // Revoke the SELECT grant to outer_view_owner on the inner_view and verify that
        // inner_view_owner can still access the view, but outer_view_owner and test_role cannot
        assertUpdate(getSession(), "REVOKE SELECT ON TABLE memory.definer_views.inner_view FROM ROLE outer_view_owner");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).result().matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.outer_view': line 1:8: Relation not found or not allowed");
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view"))
                .failure()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:15: Failed analyzing stored view 'memory.definer_views.outer_view': line 1:8: Relation not found or not allowed");

        // Grant it again and they all work again
        assertUpdate(getSession(), "GRANT SELECT ON TABLE memory.definer_views.inner_view TO ROLE outer_view_owner WITH GRANT OPTION");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).result().matches(tableContents);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.outer_view");
        assertUpdate("DROP VIEW memory.definer_views.inner_view");
        assertUpdate("DROP TABLE memory.definer_views.base_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE test_role");
        assertUpdate("DROP ROLE outer_view_owner");
        assertUpdate("DROP ROLE inner_view_owner");
        assertUpdate("DROP ROLE base_table_owner");
    }

    @Test
    public void testRowFilterOwnership()
    {
        MaterializedResult baseTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("filtered", true)
                .row("unfiltered", false)
                .build();
        MaterializedResult filteredTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("filtered", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.row_filters");

        // filterOwner gets CREATE_TABLE on the schema
        Session filterOwner = createUserAndGrantedRole("filter_owner");
        RoleId filterOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("filter_owner"));

        assertUpdate("GRANT CREATE ON schema memory.row_filters TO ROLE filter_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(filterOwner, "CREATE TABLE memory.row_filters.base_table(base_col1 VARCHAR, base_col2 BOOLEAN)");
        assertUpdate(filterOwner, "INSERT INTO memory.row_filters.base_table VALUES('filtered', true), ('unfiltered', false)", 2);
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.base_table")).result().matches(baseTableContents);

        // Create the subquery table, insert a row and verify the contents
        assertUpdate(filterOwner, "CREATE TABLE memory.row_filters.subquery_table(match_column VARCHAR)");
        assertUpdate(filterOwner, "INSERT INTO memory.row_filters.subquery_table VALUES('filtered')", 1);
        MaterializedResult subqueryTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("filtered")
                .build();
        TestingAccountClient client = getTestingAccountClient();
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.subquery_table")).result().matches(subqueryTableContents);

        // Make the row filter, owned by filterOwner
        RowFilterId rowFilterId = client.createRowFilter(
                "base_filter",
                "EXISTS (SELECT 1 FROM memory.row_filters.subquery_table WHERE match_column = base_col1)",
                filterOwnerRoleId);

        // Make the policy for the filter
        createUserAndGrantedRole("policy_enabler");
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("policy_enabler"));

        TagId tagId = client.createTag("tag1");
        PolicyId policyId = client.createRowFilterPolicy("row_filter_policy", "memory.row_filters.base_table", "has_tag(tag1)", policyEnablerId, rowFilterId);

        // Apply the tag to the base_table
        client.applyTag(tagId, EntityKind.TABLE, "memory.row_filters.base_table");

        // Show that the row filter is in effect for filterOwner
        assertUpdate("GRANT policy_enabler TO ROLE filter_owner");
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.base_table")).result().matches(filteredTableContents);

        // Create a "reader" who doesn't have access to subquery_table
        Session tableReaderSession = createUserAndGrantedRole("table_reader");
        assertUpdate("GRANT SELECT ON table memory.row_filters.base_table TO ROLE table_reader");
        assertUpdate("GRANT policy_enabler TO ROLE table_reader");

        // Show that directly accessing the subquery_table fails
        assertThat(query(tableReaderSession, "SELECT * FROM memory.row_filters.subquery_table"))
                .failure()
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");

        // Show that we only get the filtered row when querying the base_table
        assertThat(query(tableReaderSession, "SELECT * FROM memory.row_filters.base_table")).result().matches(filteredTableContents);

        // Clean up
        client.deletePolicy(policyId);
        client.deleteRowFilter(rowFilterId);
        assertUpdate("DROP TABLE memory.row_filters.subquery_table");
        assertUpdate("DROP TABLE memory.row_filters.base_table");
        client.deleteTag(tagId);
        assertUpdate("DROP ROLE policy_enabler");
        assertUpdate("DROP ROLE table_reader");
        assertUpdate("DROP ROLE filter_owner");
        assertUpdate("DROP schema row_filters");
    }

    @Test
    public void testColumnMasks()
    {
        String maskedColumnValue = "I_AM_MASKED";
        MaterializedResult unmaskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("unmasked_1", true)
                .row("unmasked_2", false)
                .build();
        MaterializedResult maskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row(maskedColumnValue, true)
                .row(maskedColumnValue, false)
                .build();

        assertUpdate("CREATE SCHEMA memory.column_masks");

        // maskOwner gets CREATE_TABLE on the schema
        Session maskOwner = createUserAndGrantedRole("mask_owner");
        RoleId maskOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("mask_owner"));

        assertUpdate("GRANT CREATE ON schema memory.column_masks TO ROLE mask_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(maskOwner, "CREATE TABLE memory.column_masks.column_mask_test_table(base_col1 VARCHAR, base_col2 BOOLEAN)");
        assertUpdate(maskOwner, "INSERT INTO memory.column_masks.column_mask_test_table VALUES('unmasked_1', true), ('unmasked_2', false)", 2);
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);

        TestingAccountClient client = getTestingAccountClient();

        // Make the column mask, owned by maskOwner
        ColumnMaskId columnMaskId = client.createColumnMask(
                "base_mask",
                "'I_AM_MASKED'",
                ColumnMaskType.VARCHAR,
                maskOwnerRoleId);

        // Make the policy for the mask
        createUserAndGrantedRole("policy_enabler");
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("policy_enabler"));

        TagId tagId = client.createTag("tag1");
        PolicyId policyId = client.createColumnMaskPolicy("column_mask_policy", "memory.column_masks.column_mask_test_table", "has_tag(tag1)", policyEnablerId, ImmutableSet.of(columnMaskId));

        // Show that before the tag is applied, the mask owner sees unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);

        // Apply the tag to base_col1 of the column_mask_test_table
        client.applyTag(tagId, EntityKind.COLUMN, "memory.column_masks.column_mask_test_table.base_col1");

        // Show that before the enabling role is granted, the mask_owner sees unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);

        // Grant the enabling role to mask_owner
        assertUpdate("GRANT policy_enabler TO ROLE mask_owner");

        // Show that mask_owner sees masked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(maskedTableContents);

        // Create an unrelated "reader" role, and grant SELECT on the table
        Session tableReader = createUserAndGrantedRole("table_reader");
        assertUpdate("GRANT SELECT ON table memory.column_masks.column_mask_test_table TO ROLE table_reader");

        // Show that before being granted , the table_reader sees unmasked results
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);

        // Grant the enabling role to table_reader
        assertUpdate("GRANT policy_enabler TO ROLE table_reader");

        // Show that table_reader now sees masked results
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(maskedTableContents);

        // Update the column mask expression
        client.updateColumnMask(columnMaskId, "UPDATED_MASK", "'UPDATED_MASK'", ColumnMaskType.VARCHAR);
        MaterializedResult updatedMaskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("UPDATED_MASK", true)
                .row("UPDATED_MASK", false)
                .build();

        // Show that mask_owner and table_reader see new results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(updatedMaskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(updatedMaskedTableContents);

        // Delete that policy and mask
        client.deletePolicy(policyId);
        client.deleteColumnMask(columnMaskId);

        // Show that mask_owner and table_reader see unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(unmaskedTableContents);

        // Create a policy and mask that depends on a table that only mask_owner can access
        assertUpdate(maskOwner, "CREATE TABLE memory.column_masks.subquery_table(match_column VARCHAR)");
        assertUpdate(maskOwner, "INSERT INTO memory.column_masks.subquery_table VALUES('_match_proof')", 1);
        MaterializedResult subqueryTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("_match_proof")
                .build();

        // mask_owner can query subquery_table
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.subquery_table")).result().matches(subqueryTableContents);

        // table_reader cannot query subquery_table
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.subquery_table"))
                .failure()
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("line 1:8: Relation not found or not allowed");

        // The column mask depends on subquery_table
        columnMaskId = client.createColumnMask(
                "new_mask",
                "concat(base_col1, (SELECT match_column FROM memory.column_masks.subquery_table))",
                ColumnMaskType.VARCHAR,
                maskOwnerRoleId);

        // Make the new policy
        policyId = client.createColumnMaskPolicy("column_mask_policy", "memory.column_masks.column_mask_test_table", "has_tag(tag1)", policyEnablerId, ImmutableSet.of(columnMaskId));

        maskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("unmasked_1_match_proof", true)
                .row("unmasked_2_match_proof", false)
                .build();

        // Both mask_owner and table_reader see the masked values when querying column_mask_test_table
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(maskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).result().matches(maskedTableContents);

        // Clean up
        client.deletePolicy(policyId);
        client.deleteColumnMask(columnMaskId);
        assertUpdate("DROP TABLE memory.column_masks.column_mask_test_table");
        assertUpdate("DROP TABLE memory.column_masks.subquery_table");
        client.deleteTag(tagId);
        assertUpdate("DROP ROLE policy_enabler");
        assertUpdate("DROP ROLE table_reader");
        assertUpdate("DROP ROLE mask_owner");
        assertUpdate("DROP schema column_masks");
    }

    @Test
    public void testSecurityDefinerViewWithColumnMaskReferencingColumnOrRowFilter()
    {
        assertUpdate("CREATE SCHEMA memory.definer_view_test");

        // Create view owner role
        Session viewOwner = createUserAndGrantedRole("view_owner");
        RoleId viewOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("view_owner"));

        assertUpdate("GRANT CREATE ON schema memory.definer_view_test TO ROLE view_owner");

        // Create base table with data
        assertUpdate(viewOwner, "CREATE TABLE memory.definer_view_test.test_table(id INTEGER, data VARCHAR)");
        assertUpdate(viewOwner, "INSERT INTO memory.definer_view_test.test_table VALUES(1, 'value1'), (2, 'value2')", 2);

        // Verify unmasked data
        MaterializedResult unmaskedData = MaterializedResult.resultBuilder(getSession(), INTEGER, VARCHAR)
                .row(1, "value1")
                .row(2, "value2")
                .build();
        assertThat(query(viewOwner, "SELECT * FROM memory.definer_view_test.test_table")).result().matches(unmaskedData);

        TestingAccountClient client = getTestingAccountClient();

        // Create a column mask that has a column reference
        ColumnMaskId columnMaskId = client.createColumnMask(
                "column_mask_with_column_ref",
                "\"$sb_internal_mask_varchar_all_but_first_four\"(cast(\"@column\" as varchar))",
                ColumnMaskType.VARCHAR,
                viewOwnerRoleId);

        // Create policy enabler role
        createUserAndGrantedRole("policy_enabler");
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("policy_enabler"));

        // Create tag and policy
        PolicyId columnMaskPolicy = client.createColumnMaskPolicy(
                "mask_policy",
                "memory.definer_view_test.test_table",
                "true",
                policyEnablerId,
                ImmutableSet.of(columnMaskId));

        // Grant the enabling role to view_owner
        assertUpdate("GRANT policy_enabler TO ROLE view_owner");

        // Verify the mask works on direct query
        MaterializedResult maskedData = MaterializedResult.resultBuilder(getSession(), INTEGER, VARCHAR)
                .row(1, "valuxx")
                .row(2, "valuxx")
                .build();
        assertThat(query(viewOwner, "SELECT * FROM memory.definer_view_test.test_table")).result().matches(maskedData);

        // Create SECURITY DEFINER view on table with the column mask
        assertUpdate(viewOwner, "CREATE VIEW memory.definer_view_test.definer_view SECURITY DEFINER AS " +
                "SELECT * FROM memory.definer_view_test.test_table");

        // Verify we can select from the view and see the masked data
        assertThat(query(viewOwner, "SELECT * FROM memory.definer_view_test.definer_view")).result().matches(maskedData);

        // Clean up column mask policy and column mask
        client.deletePolicy(columnMaskPolicy);
        client.deleteColumnMask(columnMaskId);

        // Make the row filter, owned by filterOwner
        RowFilterId rowFilterId = client.createRowFilter(
                "test_filter",
                "id = 1",
                viewOwnerRoleId);

        // Make the policy for the filter
        PolicyId rowFilterPolicy = client.createRowFilterPolicy("row_filter_policy", "memory.definer_view_test.test_table", "true", policyEnablerId, rowFilterId);

        // Verify the row filter works on direct query
        MaterializedResult filteredData = MaterializedResult.resultBuilder(getSession(), INTEGER, VARCHAR)
                .row(1, "value1")
                .build();
        assertThat(query(viewOwner, "SELECT * FROM memory.definer_view_test.test_table")).result().matches(filteredData);

        // Verify we can select from the view and see the filtered data
        assertThat(query(viewOwner, "SELECT * FROM memory.definer_view_test.definer_view")).result().matches(filteredData);

        // Clean up
        client.deletePolicy(rowFilterPolicy);
        client.deleteRowFilter(rowFilterId);

        assertUpdate(viewOwner, "DROP VIEW memory.definer_view_test.definer_view");
        assertUpdate(viewOwner, "DROP TABLE memory.definer_view_test.test_table");
        assertUpdate("DROP ROLE policy_enabler");
        assertUpdate("DROP ROLE view_owner");
        assertUpdate("DROP SCHEMA memory.definer_view_test");
    }

    public record TypedColumnMaskParameter(
            ColumnMaskType type,
            Object expectedMaskedObject,
            String columnMaskExpression,
            Map<ColumnMaskType, Object> expectedPerTypeMaskedObjectOverrides)
    {
        public TypedColumnMaskParameter
        {
            requireNonNull(type, "type is null");
            requireNonNull(expectedMaskedObject, "expectedMaskedObject is null");
            requireNonNull(columnMaskExpression, "columnMaskExpression is null");
            requireNonNull(expectedPerTypeMaskedObjectOverrides, "expectedPerTypeMaskedObjectOverrides is null");
        }
    }

    Stream<Arguments> typedColumnMaskArguments()
    {
        return Stream.of(
                arguments(new TypedColumnMaskParameter(ColumnMaskType.BIGINT, 42L, "42",
                        ImmutableMap.of(
                                ColumnMaskType.INTEGER, 42,
                                ColumnMaskType.SMALLINT, (short) 42,
                                ColumnMaskType.TINYINT, (byte) 42,
                                ColumnMaskType.DECIMAL, new BigDecimal("42.000"),
                                ColumnMaskType.REAL, 42f,
                                ColumnMaskType.DOUBLE, 42.0))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.INTEGER, 99, "99",
                        ImmutableMap.of(
                                ColumnMaskType.BIGINT, 99L,
                                ColumnMaskType.SMALLINT, (short) 99,
                                ColumnMaskType.TINYINT, (byte) 99,
                                ColumnMaskType.DECIMAL, new BigDecimal("99.000"),
                                ColumnMaskType.REAL, 99f,
                                ColumnMaskType.DOUBLE, 99.0))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.SMALLINT, (short) 11, "11",
                        ImmutableMap.of(
                                ColumnMaskType.BIGINT, 11L,
                                ColumnMaskType.INTEGER, 11,
                                ColumnMaskType.TINYINT, (byte) 11,
                                ColumnMaskType.DECIMAL, new BigDecimal("11.000"),
                                ColumnMaskType.REAL, 11f,
                                ColumnMaskType.DOUBLE, 11.0))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.TINYINT, (byte) 3, "3",
                        ImmutableMap.of(
                                ColumnMaskType.BIGINT, 3L,
                                ColumnMaskType.SMALLINT, (short) 3,
                                ColumnMaskType.INTEGER, 3,
                                ColumnMaskType.DECIMAL, new BigDecimal("3.000"),
                                ColumnMaskType.REAL, 3f,
                                ColumnMaskType.DOUBLE, 3.0))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.BOOLEAN, false, "false", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.DATE, LocalDate.of(2024, 2, 17), "DATE '2024-02-17'",
                        ImmutableMap.of(
                                ColumnMaskType.TIMESTAMP, LocalDateTime.of(2024, 2, 17, 0, 0, 0, 0),
                                ColumnMaskType.TIMESTAMP_WITH_TIME_ZONE, ZonedDateTime.of(LocalDateTime.of(2024, 2, 17, 0, 0, 0, 0), ZoneId.of("Pacific/Apia"))))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.DECIMAL, new BigDecimal("53.453"), "53.453",
                        ImmutableMap.of(
                                ColumnMaskType.REAL, 53.453f,
                                ColumnMaskType.DOUBLE, 53.453,
                                ColumnMaskType.INTEGER, 53,
                                ColumnMaskType.SMALLINT, (short) 53,
                                ColumnMaskType.BIGINT, 53L,
                                ColumnMaskType.TINYINT, (byte) 53))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.REAL, 123.45f, "123.45",
                        ImmutableMap.of(
                                ColumnMaskType.DOUBLE, 123.45,
                                ColumnMaskType.DECIMAL, new BigDecimal("123.450"),
                                ColumnMaskType.INTEGER, 123,
                                ColumnMaskType.SMALLINT, (short) 123,
                                ColumnMaskType.BIGINT, 123L,
                                ColumnMaskType.TINYINT, (byte) 123))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.DOUBLE, 94.99, "94.99",
                        ImmutableMap.of(
                                ColumnMaskType.INTEGER, 95,
                                ColumnMaskType.SMALLINT, (short) 95,
                                ColumnMaskType.BIGINT, 95L,
                                ColumnMaskType.REAL, 94.99f,
                                ColumnMaskType.TINYINT, (byte) 95,
                                ColumnMaskType.DECIMAL, new BigDecimal("94.990")))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.TIMESTAMP, LocalDateTime.of(2000, 2, 14, 12, 10, 0, 0), "TIMESTAMP '2000-02-14 12:10:00'",
                        ImmutableMap.of(
                                ColumnMaskType.DATE, LocalDate.of(2000, 2, 14),
                                ColumnMaskType.TIMESTAMP_WITH_TIME_ZONE, ZonedDateTime.of(LocalDateTime.of(2000, 2, 14, 12, 10, 0, 0), ZoneId.of("Pacific/Apia"))))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.TIMESTAMP_WITH_TIME_ZONE, ZonedDateTime.of(LocalDateTime.of(2025, 5, 14, 12, 10, 0, 0), ZoneId.of("UTC")), "TIMESTAMP '2025-05-14 12:10:00 UTC'",
                        ImmutableMap.of(
                                ColumnMaskType.DATE, LocalDate.of(2025, 5, 14),
                                ColumnMaskType.TIMESTAMP, LocalDateTime.of(2025, 5, 14, 12, 10, 0, 0)))),
                arguments(new TypedColumnMaskParameter(
                        ColumnMaskType.TIME,
                        LocalTime.of(8, 0, 0),
                        "TIME '8:00:00'",
                        ImmutableMap.of(ColumnMaskType.TIME_WITH_TIME_ZONE, OffsetTime.of(LocalTime.of(8, 0, 0), ZoneOffset.ofHours(13))))),
                arguments(new TypedColumnMaskParameter(
                        ColumnMaskType.TIME_WITH_TIME_ZONE,
                        OffsetTime.of(LocalTime.of(18, 0, 0), ZoneOffset.UTC),
                        "TIME '18:00:00 -00:00'",
                        ImmutableMap.of(ColumnMaskType.TIME, LocalTime.of(18, 0, 0)))),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.VARBINARY, new byte[] {'e', 'h', '?'}, "X'65683F'", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.VARCHAR, "this is masked", "'this is masked'", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.CHAR, "C", "'C'", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.JSON, "{\"someOtherKey\":\"someOtherValue\"}", "JSON '{ \"someOtherKey\": \"someOtherValue\" }'", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.IPADDRESS, "192.168.1.1", "IPADDRESS '192.168.1.1'", ImmutableMap.of())),
                arguments(new TypedColumnMaskParameter(ColumnMaskType.UUID, UUID.fromString("ed42d062-76d3-4ee3-9f2f-9eb1e5f03879"), "UUID 'ed42d062-76d3-4ee3-9f2f-9eb1e5f03879'", ImmutableMap.of())));
    }

    private record TrinoTypeAndValue(Type trinoType, String sqlType, Object value, Object sqlInsert) {}

    private static final Map<ColumnMaskType, TrinoTypeAndValue> SAMPLE_VALUES_FOR_TYPES = ImmutableMap.<ColumnMaskType, TrinoTypeAndValue>builder()
            .put(ColumnMaskType.INTEGER, new TrinoTypeAndValue(INTEGER, "INTEGER", 9, 9))
            .put(ColumnMaskType.SMALLINT, new TrinoTypeAndValue(SMALLINT, "SMALLINT", (short) 2, 2))
            .put(ColumnMaskType.BIGINT, new TrinoTypeAndValue(BIGINT, "BIGINT", 102L, 102))
            .put(ColumnMaskType.TINYINT, new TrinoTypeAndValue(TinyintType.TINYINT, "TINYINT", (byte) 4, 4))
            .put(ColumnMaskType.BOOLEAN, new TrinoTypeAndValue(BOOLEAN, "BOOLEAN", true, true))
            .put(ColumnMaskType.DATE, new TrinoTypeAndValue(DATE, "DATE", LocalDate.of(2024, 2, 13), "DATE '2024-02-13'"))
            .put(ColumnMaskType.DECIMAL, new TrinoTypeAndValue(createDecimalType(30, 3), "DECIMAL(30, 3)", new BigDecimal("88.888"), 88.888))
            .put(ColumnMaskType.REAL, new TrinoTypeAndValue(REAL, "REAL", 66.6f, 66.6f))
            .put(ColumnMaskType.DOUBLE, new TrinoTypeAndValue(DOUBLE, "DOUBLE", 42.1, 42.1))
            .put(ColumnMaskType.TIMESTAMP, new TrinoTypeAndValue(TIMESTAMP_SECONDS, "TIMESTAMP(0)", LocalDateTime.of(2021, 2, 14, 12, 0, 0, 0), "TIMESTAMP '2021-02-14 12:00:00'"))
            .put(ColumnMaskType.TIMESTAMP_WITH_TIME_ZONE, new TrinoTypeAndValue(TIMESTAMP_TZ_SECONDS, "TIMESTAMP(0) WITH TIME ZONE", ZonedDateTime.of(LocalDateTime.of(2024, 4, 14, 12, 0, 0, 0), ZoneId.of("UTC")), "TIMESTAMP '2024-04-14 12:00:00 UTC'"))
            .put(ColumnMaskType.TIME, new TrinoTypeAndValue(TIME_SECONDS, "TIME(0)", LocalTime.of(12, 0, 0), "TIME '12:00:00'"))
            .put(ColumnMaskType.TIME_WITH_TIME_ZONE, new TrinoTypeAndValue(TIME_TZ_SECONDS, "TIME WITH TIME ZONE", OffsetTime.of(LocalTime.of(1, 0, 0), ZoneOffset.UTC), "TIME '1:00:00 -00:00'"))
            .put(ColumnMaskType.VARCHAR, new TrinoTypeAndValue(createVarcharType(80), "VARCHAR(80)", "this is not masked", "'this is not masked'"))
            .put(ColumnMaskType.VARBINARY, new TrinoTypeAndValue(VARBINARY, "VARBINARY", new byte[] {1}, "X'01'"))
            .put(ColumnMaskType.CHAR, new TrinoTypeAndValue(createCharType(1), "CHAR", "A", "'A'"))
            .put(ColumnMaskType.JSON, new TrinoTypeAndValue(JSON, "JSON", "{\"key\":\"value\"}", "JSON '{ \"key\": \"value\" }'"))
            .put(ColumnMaskType.IPADDRESS, new TrinoTypeAndValue(IPADDRESS, "IPADDRESS", "10.0.0.1", "IPADDRESS '10.0.0.1'"))
            .put(ColumnMaskType.UUID, new TrinoTypeAndValue(UuidType.UUID, "UUID", UUID.fromString("12151fd2-7586-11e9-8f9e-2a86e4085a59"), "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'"))
            .buildOrThrow();

    @ParameterizedTest
    @MethodSource(value = "typedColumnMaskArguments")
    public void testTypedColumnMasks(TypedColumnMaskParameter maskInfo)
    {
        TestingAccountClient client = getTestingAccountClient();
        String maskOwnerRoleName = "mask%s".formatted(maskInfo.type().name().replace("_", "").toLowerCase(Locale.ROOT));
        String policyEnablerRoleName = "policyenabler%s".formatted(maskInfo.type().name().toLowerCase(Locale.ROOT));
        String tagName = "tag%s".formatted(maskInfo.type().name().replace("_", "").toLowerCase(Locale.ROOT));
        String policyName = "policy%s".formatted(maskInfo.type().name().replace("_", "").toLowerCase(Locale.ROOT));
        Session maskOwner = createUserAndGrantedRole(maskOwnerRoleName);
        RoleId maskOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName(maskOwnerRoleName));
        createUserAndGrantedRole(policyEnablerRoleName);
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName(policyEnablerRoleName));

        String baseTable = null;
        ColumnMaskId mask = null;
        TagId tagId = null;
        PolicyId policyId = null;
        ColumnMaskId nullMaskAnyType = null;
        try {
            assertUpdate("CREATE SCHEMA IF NOT EXISTS memory.typed_column_masks");
            assertUpdate("GRANT CREATE ON schema memory.typed_column_masks TO ROLE %s".formatted(maskOwnerRoleName));

            // Create the base table, insert a row and verify the contents
            baseTable = "types%s".formatted(maskInfo.type().name().toLowerCase(Locale.ROOT));
            TrinoTypeAndValue baseValue = requireNonNull(SAMPLE_VALUES_FOR_TYPES.get(maskInfo.type()));
            assertUpdate(maskOwner, "CREATE TABLE memory.typed_column_masks.%s (mycol %s NOT NULL )".formatted(baseTable, baseValue.sqlType()));
            assertUpdate(maskOwner, "INSERT INTO memory.typed_column_masks.%s (mycol) VALUES %s ".formatted(baseTable, baseValue.sqlInsert()), 1);
            assertThat(computeActual(maskOwner, "SELECT mycol FROM memory.typed_column_masks.%s".formatted(baseTable)).getOnlyValue())
                    .isEqualTo(baseValue.value());

            // Create the column mask to apply, as well as the policy and tag
            mask = client.createColumnMask("mask%s".formatted(maskInfo.type().name().toLowerCase(Locale.ROOT)), maskInfo.columnMaskExpression(), maskInfo.type(), maskOwnerRoleId);
            tagId = client.createTag(tagName);
            policyId = client.createColumnMaskPolicy(policyName, "memory.typed_column_masks.*", "has_tag(%s)".formatted(tagName), policyEnablerId, ImmutableSet.of(mask));

            // Apply the tag to base_col1 of the column_mask_test_table
            client.applyTag(tagId, EntityKind.COLUMN, "memory.typed_column_masks.%s.mycol".formatted(baseTable));

            // Show that before the enabling role is granted, the mask_owner sees unmasked results
            assertThat(computeActual(maskOwner, "SELECT mycol FROM memory.typed_column_masks.%s".formatted(baseTable)).getOnlyValue())
                    .isEqualTo(baseValue.value());

            // Grant the enabling role to mask_owner
            assertUpdate("GRANT %s TO ROLE %s".formatted(policyEnablerRoleName, maskOwnerRoleName));

            // Show that mask_owner sees masked results
            assertThat(computeActual(maskOwner, "SELECT mycol FROM memory.typed_column_masks.%s".formatted(baseTable)).getOnlyValue())
                    .isEqualTo(maskInfo.expectedMaskedObject());

            // fetch a type which is not coercible with it, to test that ANY can act as a catch-all
            nullMaskAnyType = client.createColumnMask("any%s".formatted(maskOwnerRoleName), "NULL", ColumnMaskType.ANY, maskOwnerRoleId);
            client.deletePolicy(policyId);
            policyId = client.createColumnMaskPolicy(
                    policyName,
                    "memory.typed_column_masks.*",
                    "has_tag(%s)".formatted(tagName),
                    policyEnablerId,
                    ImmutableSet.of(mask, nullMaskAnyType));

            ColumnMaskType nonCoercibleType = Arrays.stream(ColumnMaskType.values())
                    .filter(type -> type != maskInfo.type() && type != ColumnMaskType.ANY && !type.getCompatibleWith().contains(maskInfo.type()))
                    .findFirst().orElseThrow();
            String nonCoercibleTable = "types%s".formatted(nonCoercibleType.name().toLowerCase(Locale.ROOT));
            TrinoTypeAndValue nonCoercibleValue = requireNonNull(SAMPLE_VALUES_FOR_TYPES.get(nonCoercibleType));
            assertUpdate(maskOwner, "CREATE TABLE memory.typed_column_masks.%s ( mycol %s NOT NULL )".formatted(nonCoercibleTable, nonCoercibleType.getDisplayName()));
            assertUpdate(maskOwner, "INSERT INTO memory.typed_column_masks.%s (mycol) VALUES (%s) ".formatted(nonCoercibleTable, nonCoercibleValue.value()), 1);
            assertThat(computeActual(maskOwner, "SELECT * FROM memory.typed_column_masks.%s".formatted(nonCoercibleTable)).getOnlyValue())
                    .isEqualTo(nonCoercibleValue.value());

            // tag the entity, and the ANY catch-all should apply (the column mask is NULL, so the value should be null)
            client.applyTag(tagId, EntityKind.COLUMN, "memory.typed_column_masks.%s.mycol".formatted(nonCoercibleTable));
            assertThat(computeActual(maskOwner, "SELECT * FROM memory.typed_column_masks.%s".formatted(nonCoercibleTable)).getOnlyValue())
                    .isNull();

            // Clean up
            assertUpdate("DROP TABLE memory.typed_column_masks.%s".formatted(nonCoercibleTable));

            // test that type coercion works for all configured 'compatible with' types
            for (ColumnMaskType coercibleType : maskInfo.type().getCompatibleWith()) {
                if (coercibleType == maskInfo.type()) {
                    continue;
                }
                // create a table of this coercible type, make sure we get the info back
                String coercibleTable = "types%s".formatted(coercibleType.name().toLowerCase(Locale.ROOT));
                TrinoTypeAndValue coercibleValue = requireNonNull(SAMPLE_VALUES_FOR_TYPES.get(coercibleType));
                assertUpdate(maskOwner, "CREATE TABLE memory.typed_column_masks.%s (coerciblecol %s NOT NULL )".formatted(coercibleTable, coercibleValue.sqlType()));
                assertUpdate(maskOwner, "INSERT INTO memory.typed_column_masks.%s (coerciblecol) VALUES %s ".formatted(coercibleTable, coercibleValue.sqlInsert()), 1);
                assertThat(computeActual(maskOwner, "SELECT * FROM memory.typed_column_masks.%s".formatted(coercibleTable)).getOnlyValue())
                        .describedAs("Testing type coercion from %s to %s, default value is correct".formatted(maskInfo.type(), coercibleType))
                        .isEqualTo(coercibleValue.value());

                // tag the column, now the coercible type should show up as the masked type's data
                client.applyTag(tagId, EntityKind.COLUMN, "memory.typed_column_masks.%s.coerciblecol".formatted(coercibleTable));
                assertThat(computeActual(maskOwner, "SELECT * FROM memory.typed_column_masks.%s".formatted(coercibleTable)).getOnlyValue())
                        .describedAs("Testing if %s can be coerced to type %s, masked value is correct".formatted(maskInfo.type(), coercibleType))
                        .isEqualTo(maskInfo.expectedPerTypeMaskedObjectOverrides().getOrDefault(coercibleType, maskInfo.expectedMaskedObject()));

                // untag the entity, and make sure we get the valid results back again
                client.revokeApplyTag(tagId, EntityKind.COLUMN, "memory.typed_column_masks.%s.coerciblecol".formatted(coercibleTable));
                assertThat(computeActual(maskOwner, "SELECT * FROM memory.typed_column_masks.%s".formatted(coercibleTable)).getOnlyValue())
                        .describedAs("Testing if %s can be coerced to type %s, the mask has been removed, the default value remains".formatted(maskInfo.type(), coercibleType))
                        .isEqualTo(coercibleValue.value());

                // clean up
                assertUpdate("DROP TABLE memory.typed_column_masks.%s".formatted(coercibleTable));
            }
        }
        finally {
            // Clean up
            assertUpdate("DROP TABLE memory.typed_column_masks.%s".formatted(baseTable));
            if (policyId != null) {
                client.deletePolicy(policyId);
            }
            if (mask != null) {
                client.deleteColumnMask(mask);
            }
            if (nullMaskAnyType != null) {
                client.deleteColumnMask(nullMaskAnyType);
            }
            assertUpdate("DROP SCHEMA typed_column_masks CASCADE ");
            if (tagId != null) {
                client.deleteTag(tagId);
            }
            assertUpdate("DROP ROLE %s".formatted(policyEnablerRoleName));
            assertUpdate("DROP ROLE %s".formatted(maskOwnerRoleName));
        }
    }

    @Test
    public void testColumnMasksAreCastToColumnType()
    {
        assertColumnMasksAreCastToColumnType(
                // An ANY mask whose value has no common supertype with the column is cast to the column type
                new CastColumnMask("integer_mask_on_varchar", "VARCHAR", ImmutableList.of("'secret'"), "42", ColumnMaskType.ANY, "VALUES CAST('42' AS VARCHAR)"),
                // Values that cannot be cast to the column type are masked as NULL instead of failing the query
                new CastColumnMask("uncastable_mask", "INTEGER", ImmutableList.of("7"), "'not a number'", ColumnMaskType.ANY, "VALUES CAST(NULL AS INTEGER)"),
                // BIGINT is compatible with INTEGER so this mask is selected, but the value does not fit
                new CastColumnMask("overflowing_mask", "INTEGER", ImmutableList.of("7"), "9999999999", ColumnMaskType.BIGINT, "VALUES CAST(NULL AS INTEGER)"));
    }

    @Test
    public void testBuiltInColumnMaskFunctionsAreCastToColumnType()
    {
        assertColumnMasksAreCastToColumnType(
                // An ANY mask using the integer-masking function produces a varchar, which has no common supertype with bigint
                new CastColumnMask(
                        "varchar_mask_on_bigint",
                        "BIGINT",
                        ImmutableList.of("12345", "-678"),
                        "\"$sb_internal_mask_integer_as_varchar\"(cast(\"@column\" as varchar))",
                        ColumnMaskType.ANY,
                        "VALUES BIGINT '11111', BIGINT '-111'"));
    }

    private void assertColumnMasksAreCastToColumnType(CastColumnMask... columnMasks)
    {
        TestingAccountClient client = getTestingAccountClient();
        ImmutableList.Builder<ColumnMaskId> masks = ImmutableList.builder();
        ImmutableList.Builder<PolicyId> policies = ImmutableList.builder();
        try {
            Session maskOwner = createUserAndGrantedRole("cast_mask_owner");
            RoleId maskOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                    .get(new RoleName("cast_mask_owner"));
            createUserAndGrantedRole("cast_mask_enabler");
            RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                    .get(new RoleName("cast_mask_enabler"));

            assertUpdate("CREATE SCHEMA memory.cast_masks");
            assertUpdate("GRANT CREATE ON SCHEMA memory.cast_masks TO ROLE cast_mask_owner");
            for (CastColumnMask columnMask : columnMasks) {
                String table = "memory.cast_masks." + columnMask.table();
                String values = String.join(", ", columnMask.values());
                assertUpdate(maskOwner, "CREATE TABLE %s (col %s)".formatted(table, columnMask.columnType()));
                assertUpdate(maskOwner, "INSERT INTO %s VALUES %s".formatted(table, values), columnMask.values().size());
                ColumnMaskId mask = client.createColumnMask("cast_" + columnMask.table(), columnMask.expression(), columnMask.type(), maskOwnerRoleId);
                masks.add(mask);
                policies.add(client.createColumnMaskPolicy("cast_" + columnMask.table(), table, "true", policyEnablerId, ImmutableSet.of(mask)));
                // The mask doesn't apply until the role that enables the policy is granted
                assertThat(query(maskOwner, "SELECT col FROM " + table))
                        .matches("SELECT CAST(col AS %s) FROM (VALUES %s) t(col)".formatted(columnMask.columnType(), values));
            }

            assertUpdate("GRANT cast_mask_enabler TO ROLE cast_mask_owner");

            for (CastColumnMask columnMask : columnMasks) {
                assertThat(query(maskOwner, "SELECT col FROM memory.cast_masks." + columnMask.table()))
                        .matches(columnMask.expected());
            }
        }
        finally {
            policies.build().forEach(client::deletePolicy);
            masks.build().forEach(client::deleteColumnMask);
            assertUpdate("DROP SCHEMA IF EXISTS memory.cast_masks CASCADE");
            assertUpdate("DROP ROLE IF EXISTS cast_mask_enabler");
            assertUpdate("DROP ROLE IF EXISTS cast_mask_owner");
        }
    }

    private record CastColumnMask(String table, String columnType, List<String> values, String expression, ColumnMaskType type, @Language("SQL") String expected) {}

    @Test
    public void testSystemBuiltinTableFunctions()
    {
        // Test the functions with and without the system.builtin. prefix
        for (String catalogAndSchema : ImmutableList.of("", "system.builtin.")) {
            // Test exclude_columns
            assertThat(query(
                    """
                    SELECT *
                    FROM TABLE(%sexclude_columns(
                                        input => TABLE(tpch.tiny.region),
                                        columns => DESCRIPTOR(regionkey, comment)))
                    """.formatted(catalogAndSchema)))
                    .skippingTypesCheck()
                    .matches("VALUES ('AFRICA'), ('AMERICA'), ('ASIA'), ('EUROPE'), ('MIDDLE EAST')");

            // Test sequence
            assertThat(query(
                    """
                    SELECT *
                    FROM TABLE(%ssequence(
                                    start => 1,
                                    stop => 3,
                                    step => 1))
                    """.formatted(catalogAndSchema)))
                    .skippingTypesCheck()
                    .matches("VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)");
        }
    }

    @Test
    public void testBadFunctionSchemas()
    {
        assertQueryFails("SELECT memory.funzies.add(2, 3)", "line 1:8: The schema name for function 'memory.funzies.add' must be 'system', 'ai', 'crypto', 'functions', or 'api'");
        String catalogName = "catalog_" + randomNameSuffix();
        assertQueryFails("SELECT %s.functionsnot.sub(2, 3)".formatted(catalogName), "line 1:8: The schema name for function '%s.functionsnot.sub' must be 'system', 'ai', 'crypto', 'functions', or 'api'".formatted(catalogName));
    }

    @Test
    public void testAccountPrivileges()
    {
        Set<io.starburst.stargate.accesscontrol.privilege.Privilege> eligiblePrivileges = Sets.difference(
                io.starburst.stargate.accesscontrol.privilege.Privilege.getEntityKindPrivileges(EntityKind.ACCOUNT),
                testingAccountClient.getDisabledAccountPrivileges());

        // Remove account privileges on role public, since they are inherited and mess up the test
        ImmutableList.of(SSO_USER_PASSWORD_LOGIN, VIEW_PUBLIC_OAUTH_CLIENT).forEach(privilege ->
                assertUpdate("REVOKE %s ON my account FROM ROLE public".formatted(privilege.name())));

        verifyCurrentRoles(ImmutableSet.of(), ImmutableSet.of());
        assertUpdate("CREATE ROLE account_privilege_role");
        verifyCurrentRoles(ImmutableSet.of("account_privilege_role"), ImmutableSet.of());
        RoleId targetRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("account_privilege_role"));

        eligiblePrivileges.forEach(privilege -> {
            // Show that granting and revoking account privileges without grant option works correctly
            assertUpdate("GRANT %s ON my account TO ROLE account_privilege_role".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .containsOnly(new GalaxyPrivilegeInfo(privilege, false));
            assertUpdate("REVOKE %s ON my account FROM ROLE account_privilege_role".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .isEmpty();

            // Show that DENYing account-level privileges fails
            assertThatThrownBy(() -> assertUpdate("DENY %s ON my account TO ROLE account_privilege_role".formatted(privilege.name())))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("Deny grant kind is not supported for entity kind ACCOUNT");

            // Show that granting and revoking account privileges with grant option works correctly
            assertUpdate("GRANT %s ON my account TO ROLE account_privilege_role WITH GRANT OPTION".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, true));
            assertUpdate("REVOKE %s ON my account FROM ROLE account_privilege_role".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .isEmpty();

            assertUpdate("GRANT %s ON my account TO ROLE account_privilege_role WITH GRANT OPTION".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, true));
            assertUpdate("REVOKE GRANT OPTION FOR %s ON my account FROM ROLE account_privilege_role".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, false));
            assertUpdate("REVOKE %s ON my account FROM ROLE account_privilege_role".formatted(privilege.name()));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId()).getPrivileges())
                    .isEmpty();
        });
        assertUpdate("GRANT ALL PRIVILEGES ON my account TO ROLE account_privilege_role WITH GRANT OPTION");
        EntityPrivileges entityPrivileges = getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), targetRoleId, testingAccountClient.getAccountId());

        assertThat(entityPrivileges.getPrivileges().stream().map(GalaxyPrivilegeInfo::getPrivilege).collect(toImmutableSet()))
                .containsExactlyInAnyOrderElementsOf(eligiblePrivileges);

        assertUpdate("DROP ROLE account_privilege_role");
    }

    @Test
    public void testClusterPrivileges()
    {
        Map<String, ClusterId> clusters = getTrinoSecurityApi().getAccountClusterNamesAndIds(toDispatchSession(getSession()));
        assertThat(clusters.size()).isEqualTo(1);
        String clusterName = clusters.entrySet().stream().findFirst().orElseThrow().getKey();
        ClusterId clusterId = clusters.entrySet().stream().findFirst().orElseThrow().getValue();
        String entityName = "\"%s\"".formatted(clusterName);

        // Role public has privilege USE_CLUSTER, and inheritance from public messes up the test,
        // so remove it for the duration of the test
        assertUpdate("REVOKE USE_CLUSTER ON cluster %s FROM ROLE public".formatted(entityName));

        String roleName = "cluster_privilege_role";
        assertUpdate("CREATE ROLE " + roleName);
        RoleId roleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName(roleName));
        testingAccountClient.getSampleClusterId();
        String entityKind = "CLUSTER";
        for (io.starburst.stargate.accesscontrol.privilege.Privilege privilege : io.starburst.stargate.accesscontrol.privilege.Privilege.getEntityKindPrivileges(EntityKind.CLUSTER)) {
            // Show that granting and revoking the privilege with grant option works correctly
            assertUpdate("GRANT %s ON %s %s TO ROLE %s WITH GRANT OPTION".formatted(privilege.name(), entityKind, entityName, roleName));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, true));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, true));
            assertUpdate("REVOKE GRANT OPTION FOR %s ON %s %s FROM ROLE %s".formatted(privilege.name(), entityKind, entityName, roleName));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                    .contains(new GalaxyPrivilegeInfo(privilege, false));
            assertUpdate("REVOKE %s ON %s %s FROM ROLE %s".formatted(privilege.name(), entityKind, entityName, roleName));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                    .isEmpty();

            // Show that denying the privilege works as expected
            assertUpdate("DENY %s ON %s %s TO ROLE %s".formatted(privilege.name(), entityKind, entityName, roleName));
            assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                    .isEmpty();
            assertUpdate("REVOKE %s ON %s %s FROM ROLE %s".formatted(privilege.name(), entityKind, entityName, roleName));
        }

        assertUpdate("GRANT ALL PRIVILEGES ON %s %s TO ROLE %s WITH GRANT OPTION".formatted("CLUSTER", entityName, roleName));
        assertThat(getTrinoSecurityApi().getEntityPrivileges(toDispatchSession(adminSession()), roleId, clusterId).getPrivileges())
                .containsOnly(
                        new GalaxyPrivilegeInfo(USE_CLUSTER, true),
                        new GalaxyPrivilegeInfo(MONITOR_CLUSTER, true),
                        new GalaxyPrivilegeInfo(ENABLE_DISABLE_CLUSTER, true),
                        new GalaxyPrivilegeInfo(TROUBLESHOOT_QUERY, true));

        assertUpdate("DROP ROLE " + roleName);

        // Restore USE_CLUSTER on role public
        assertUpdate("GRANT USE_CLUSTER ON cluster %s TO ROLE public".formatted(entityName));

        // Non-existent cluster and invalidly named cluster queries should fail
        assertQueryFails("GRANT USE_CLUSTER ON cluster \"nonexistent\" TO ROLE public", "Error accessing Galaxy Access Control: entityId is null");
        assertQueryFails("GRANT USE_CLUSTER ON cluster \"invalid.name\" TO ROLE public", "Error accessing Galaxy Access Control: entityId is null");
    }

    @Test
    public void testLocationPrivileges()
    {
        String roleName = "location_privilege_role";
        assertUpdate("CREATE ROLE " + roleName);
        RoleId roleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName(roleName));

        StorageLocation location = new StorageLocation("s3://foo/bar/*");
        // Show that granting and revokimg the privilege with grant option works correctly
        String privilege = "CREATE_SQL";
        String entityKind = "LOCATION";
        String entityName = "\"s3://foo/bar/*\"";
        assertUpdate("GRANT %s ON %s %s TO ROLE %s WITH GRANT OPTION".formatted(privilege, entityKind, entityName, roleName));
        assertThat(getTrinoSecurityApi().canUseLocation(toDispatchSession(newUserSession(roleId)), location.getBaseEntityIdString())).isTrue();
        assertUpdate("REVOKE GRANT OPTION FOR %s ON %s %s FROM ROLE %s".formatted(privilege, entityKind, entityName, roleName));
        assertThat(getTrinoSecurityApi().canUseLocation(toDispatchSession(newUserSession(roleId)), location.getBaseEntityIdString())).isTrue();
        assertUpdate("REVOKE %s ON %s %s FROM ROLE %s".formatted(privilege, entityKind, entityName, roleName));
        assertThat(getTrinoSecurityApi().canUseLocation(toDispatchSession(newUserSession(roleId)), location.getBaseEntityIdString())).isFalse();

        // Show that DENYing location-level privileges fails
        assertThatThrownBy(() -> assertUpdate("DENY %s ON %s %s TO ROLE %s".formatted(privilege, entityKind, entityName, roleName)))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageContaining("Deny grant kind is not supported for entity kind LOCATION");

        // Show that ALL PRIVILEGES, which consist of the one privilege CREATE_SQL, allows access
        assertThat(getTrinoSecurityApi().canUseLocation(toDispatchSession(newUserSession(roleId)), location.getBaseEntityIdString())).isFalse();
        assertUpdate("GRANT ALL PRIVILEGES ON %s %s TO ROLE %s WITH GRANT OPTION".formatted(entityKind, entityName, roleName));
        assertThat(getTrinoSecurityApi().canUseLocation(toDispatchSession(newUserSession(roleId)), location.getBaseEntityIdString())).isTrue();

        assertUpdate("DROP ROLE " + roleName);
    }

    @Test
    public void testShowGrants()
    {
        assertUpdate("CREATE SCHEMA memory.test_schema");
        assertUpdate("CREATE TABLE memory.test_schema.test_table (test_column int)");

        String roleName = "show_grants_role";
        Session showGrantsSession = createUserAndGrantedRole(roleName);
        assertUpdate("GRANT ALL PRIVILEGES ON CATALOG memory TO ROLE %s".formatted(roleName));
        assertUpdate("GRANT ALL PRIVILEGES ON SCHEMA memory.test_schema TO ROLE %s".formatted(roleName));
        assertUpdate("GRANT ALL PRIVILEGES ON TABLE memory.test_schema.test_table TO ROLE %s".formatted(roleName));

        assertThat(query(showGrantsSession, "SHOW GRANTS ON CATALOG memory")).result().skippingTypesCheck()
                .matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("show_grants_role", "CATALOG", "memory", "ALLOW", "CREATE_SCHEMA", "false", "accountadmin", "true")
                        .build());

        assertThat(query(showGrantsSession, "SHOW GRANTS ON SCHEMA memory.test_schema")).result().skippingTypesCheck()
                .matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("show_grants_role", "SCHEMA", "memory.test_schema", "ALLOW", "CREATE_TABLE", "false", "accountadmin", "true")
                        .build());

        assertThat(query(showGrantsSession, "SHOW GRANTS ON TABLE memory.test_schema.test_table")).result().skippingTypesCheck()
                .matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("show_grants_role", "TABLE", "memory.test_schema.test_table", "ALLOW", "SELECT", "false", "accountadmin", "true")
                        .row("show_grants_role", "TABLE", "memory.test_schema.test_table", "ALLOW", "INSERT", "false", "accountadmin", "true")
                        .row("show_grants_role", "TABLE", "memory.test_schema.test_table", "ALLOW", "UPDATE", "false", "accountadmin", "true")
                        .row("show_grants_role", "TABLE", "memory.test_schema.test_table", "ALLOW", "DELETE", "false", "accountadmin", "true")
                        .row("show_grants_role", "TABLE", "memory.test_schema.test_table", "ALLOW", "MANAGE_DATA_OBSERVABILITY", "false", "accountadmin", "true")
                        .build());

        assertThat(query(showGrantsSession, "SHOW GRANTS ON COLUMN memory.test_schema.test_table.test_column")).result().skippingTypesCheck()
                .matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("show_grants_role", "COLUMN", "memory.test_schema.test_table.*", "ALLOW", "SELECT", "false", "accountadmin", "false")
                        .row("show_grants_role", "COLUMN", "memory.test_schema.test_table.*", "ALLOW", "INSERT", "false", "accountadmin", "false")
                        .row("show_grants_role", "COLUMN", "memory.test_schema.test_table.*", "ALLOW", "UPDATE", "false", "accountadmin", "false")
                        .row("show_grants_role", "COLUMN", "memory.test_schema.test_table.*", "ALLOW", "DELETE", "false", "accountadmin", "false")
                        .row("show_grants_role", "COLUMN", "memory.test_schema.test_table.*", "ALLOW", "MANAGE_DATA_OBSERVABILITY", "false", "accountadmin", "false")
                        .build());

        assertUpdate("DROP TABLE memory.test_schema.test_table");
        assertUpdate("DROP SCHEMA memory.test_schema");
        assertUpdate("DROP ROLE %s".formatted(roleName));
    }

    private void uncontainedEntitySetAuthorizationTest(String entityKind, String entityName)
    {
        assertQueryFails(publicSession(), "ALTER %s \"%s\" SET AUTHORIZATION ROLE public".formatted(entityKind, entityName), "Operation not allowed.*");
        assertQueryFails(adminSession(), "ALTER %s \"%sdoesnotexist\" SET AUTHORIZATION ROLE public".formatted(entityKind, entityName), "Could not find %s entity.*".formatted(entityKind));
        assertQueryFails(adminSession(), "ALTER %s \"%s\" SET AUTHORIZATION USER \"admin@example.com\"".formatted(entityKind, entityName), "Galaxy only supports a ROLE as an owner");
        assertQuerySucceeds(adminSession(), "ALTER %s \"%s\" SET AUTHORIZATION ROLE public".formatted(entityKind, entityName));
        assertQuerySucceeds(publicSession(), "ALTER %s \"%s\" SET AUTHORIZATION ROLE accountadmin".formatted(entityKind, entityName));
    }

    private MaterializedResult varcharColumnResult(String... values)
    {
        return varcharColumnResult(ImmutableSet.copyOf(values));
    }

    private MaterializedResult varcharColumnResult(Set<String> values)
    {
        MaterializedResult.Builder resultBuilder = resultBuilder(getSession(), VARCHAR);
        values.forEach(resultBuilder::row);
        return resultBuilder.build();
    }

    protected Session publicSession()
    {
        return sessionFor(testingAccountClient, testingAccountClient.getPublicRoleId());
    }

    protected Session adminSession()
    {
        return sessionFor(testingAccountClient, testingAccountClient.getAdminRoleId());
    }

    private Session sessionFor(TestingAccountClient accountClient, RoleId roleId)
    {
        Identity identity = createIdentity(
                accountClient.getAdminEmail(),
                accountClient.getAccountId(),
                accountClient.getAdminUserId(),
                roleId,
                Optional.empty(),
                accountClient.getAdminTrinoAccessToken(),
                PORTAL);

        return Session.builder(getSession())
                .setIdentity(identity)
                .build();
    }

    private Session createUserAndGrantedRole(String roleName)
    {
        assertUpdate("CREATE ROLE " + roleName);
        Session session = newUserSession(roleName);
        assertUpdate("GRANT %s TO USER \"%s\"".formatted(roleName, session.getUser()));
        return session;
    }

    private Session newUserSession(String roleName)
    {
        RoleId roleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession())).get(new RoleName(roleName));
        verifyNotNull(roleId, "roleId for roleName %s is null", roleName);
        return newUserSession(roleId);
    }

    protected Session newUserSession(RoleId roleId)
    {
        TestingAccountClient accountClient = getTestingAccountClient();
        TestUser user = accountClient.createUser();

        Identity identity = createIdentity(
                user.getEmail(),
                accountClient.getAccountId(),
                user.getUserId(),
                roleId,
                Optional.empty(),
                accountClient.getAdminTrinoAccessToken(),
                PORTAL);

        SessionBuilder builder = Session.builder(getSession()).setIdentity(identity);

        return builder.build();
    }

    private TestingAccountClient getTestingAccountClient()
    {
        TestingTrinoServer coordinator = this.getDistributedQueryRunner().getCoordinator();
        TestingAccountClient accountClient = coordinator.getInstance(Key.get(TestingAccountClient.class));
        return accountClient;
    }

    protected TrinoSecurityApi getTrinoSecurityApi()
    {
        TestingTrinoServer coordinator = this.getDistributedQueryRunner().getCoordinator();
        return coordinator.getInstance(Key.get(TrinoSecurityApi.class));
    }
}
