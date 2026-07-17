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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.Grants;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MutableGrants;
import io.trino.execution.starburst.GalaxyPrivilegeUtilities;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.EnumSet;
import java.util.Set;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractTestRevokeOnTable
{
    private static final Session admin = sessionOf("admin");
    private static final Set<Privilege> TABLE_PRIVILEGES = ImmutableSet.of(Privilege.SELECT, Privilege.UPDATE, Privilege.INSERT, Privilege.DELETE, Privilege.MANAGE_DATA_OBSERVABILITY, Privilege.CREATE_BRANCH);
    private static final Session userWithAllTablePrivileges = sessionOf(randomUsername());
    private static final Session userWithCreate = sessionOf(randomUsername());
    private static final Session userWithSelect = sessionOf(randomUsername());
    private static final Session userWithInsert = sessionOf(randomUsername());
    private static final Session userWithUpdate = sessionOf(randomUsername());
    private static final Session userWithDelete = sessionOf(randomUsername());
    private static final Session userWithManageDataObservability = sessionOf(randomUsername());
    private static final Session userWithCreateBranch = sessionOf(randomUsername());

    private final boolean isGalaxyEnabled;

    private QueryRunner queryRunner;
    private QueryAssertions assertions;

    public AbstractTestRevokeOnTable(boolean isGalaxyEnabled)
    {
        this.isGalaxyEnabled = isGalaxyEnabled;
    }

    @BeforeAll
    public void initClass()
            throws Exception
    {
        SchemaTableName table = new SchemaTableName("default", "table_one");
        queryRunner = DistributedQueryRunner
                .builder(userWithAllTablePrivileges)
                .setAdditionalModule(binder -> {
                    if (isGalaxyEnabled) {
                        newOptionalBinder(binder, PrivilegeUtilitiesApi.class).setBinding().to(GalaxyPrivilegeUtilities.class).in(SINGLETON);
                    }
                })
                .build();
        Grants<SchemaTableName> tableGrants = new MutableGrants<>();
        if (isGalaxyEnabled) {
            tableGrants.grant(new TrinoPrincipal(USER, admin.getUser()), table, TABLE_PRIVILEGES, true);
            tableGrants.grant(new TrinoPrincipal(USER, userWithAllTablePrivileges.getUser()), table, TABLE_PRIVILEGES, true);
        }
        else {
            tableGrants.grant(new TrinoPrincipal(USER, admin.getUser()), table, EnumSet.allOf(Privilege.class), true);
            tableGrants.grant(new TrinoPrincipal(USER, userWithAllTablePrivileges.getUser()), table, EnumSet.allOf(Privilege.class), true);
        }
        tableGrants.grant(new TrinoPrincipal(USER, userWithCreate.getUser()), table, ImmutableSet.of(Privilege.CREATE), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithSelect.getUser()), table, ImmutableSet.of(Privilege.SELECT), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithInsert.getUser()), table, ImmutableSet.of(Privilege.INSERT), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithUpdate.getUser()), table, ImmutableSet.of(Privilege.UPDATE), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithDelete.getUser()), table, ImmutableSet.of(Privilege.DELETE), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithManageDataObservability.getUser()), table, ImmutableSet.of(Privilege.MANAGE_DATA_OBSERVABILITY), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithCreateBranch.getUser()), table, ImmutableSet.of(Privilege.CREATE_BRANCH), true);
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of("default"))
                .withListTables((_, schemaName) -> "default".equalsIgnoreCase(schemaName) ? ImmutableList.of(table.getTableName()) : ImmutableList.of())
                .withGetTableHandle((_, tableName) -> tableName.equals(table) ? new MockConnectorTableHandle(tableName) : null)
                .withSchemaGrants(new MutableGrants<>())
                .withTableGrants(tableGrants)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
        queryRunner = null; // closed by assertions.close
    }

    @Test
    public void testRevokeOnSchema()
    {
        testRevokeOnSchema("SELECT", userWithSelect);
        testRevokeOnSchema("INSERT", userWithInsert);
        testRevokeOnSchema("UPDATE", userWithUpdate);
        testRevokeOnSchema("DELETE", userWithDelete);
        testRevokeOnSchema("MANAGE_DATA_OBSERVABILITY", userWithManageDataObservability);
        testRevokeOnSchema("CREATE BRANCH", userWithCreateBranch);
        testRevokeOnSchema("ALL PRIVILEGES", userWithAllTablePrivileges);
    }

    private void testRevokeOnSchema(String privilege, Session user)
    {
        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");

        queryRunner.execute(admin, format("REVOKE %s ON TABLE table_one FROM %s", privilege, user.getUser()));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).returnsEmptyResult();
    }

    @Test
    public void testRevokeOnNonExistingCatalog()
    {
        testRevokeOnNonExistingCatalog("CREATE", userWithCreate);
        testRevokeOnNonExistingCatalog("SELECT", userWithSelect);
        testRevokeOnNonExistingCatalog("INSERT", userWithInsert);
        testRevokeOnNonExistingCatalog("UPDATE", userWithUpdate);
        testRevokeOnNonExistingCatalog("DELETE", userWithDelete);
        testRevokeOnNonExistingCatalog("MANAGE_DATA_OBSERVABILITY", userWithManageDataObservability);
        testRevokeOnNonExistingCatalog("CREATE BRANCH", userWithCreateBranch);
        testRevokeOnNonExistingCatalog("ALL PRIVILEGES", userWithAllTablePrivileges);
    }

    private void testRevokeOnNonExistingCatalog(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE missing_catalog.missing_schema.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testRevokeOnNonExistingSchema()
    {
        testRevokeOnNonExistingSchema("CREATE", userWithCreate);
        testRevokeOnNonExistingSchema("SELECT", userWithSelect);
        testRevokeOnNonExistingSchema("INSERT", userWithInsert);
        testRevokeOnNonExistingSchema("UPDATE", userWithUpdate);
        testRevokeOnNonExistingSchema("DELETE", userWithDelete);
        testRevokeOnNonExistingSchema("MANAGE_DATA_OBSERVABILITY", userWithManageDataObservability);
        testRevokeOnNonExistingSchema("CREATE BRANCH", userWithCreateBranch);
        testRevokeOnNonExistingSchema("ALL PRIVILEGES", userWithAllTablePrivileges);
    }

    private void testRevokeOnNonExistingSchema(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE missing_schema.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testRevokeOnNonExistingTable()
    {
        testRevokeOnNonExistingTable("CREATE", userWithCreate);
        testRevokeOnNonExistingTable("SELECT", userWithSelect);
        testRevokeOnNonExistingTable("INSERT", userWithInsert);
        testRevokeOnNonExistingTable("UPDATE", userWithUpdate);
        testRevokeOnNonExistingTable("DELETE", userWithDelete);
        testRevokeOnNonExistingTable("MANAGE_DATA_OBSERVABILITY", userWithManageDataObservability);
        testRevokeOnNonExistingTable("CREATE BRANCH", userWithCreateBranch);
        testRevokeOnNonExistingTable("ALL PRIVILEGES", userWithAllTablePrivileges);
    }

    private void testRevokeOnNonExistingTable(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE default.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
    }

    @Test
    public void testAccessDenied()
    {
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE SELECT ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining(
                        "Access Denied: Cannot revoke privilege SELECT on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE SELECT ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege SELECT on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE INSERT ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege INSERT on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE UPDATE ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege UPDATE on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE DELETE ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege DELETE on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE MANAGE_DATA_OBSERVABILITY ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege MANAGE_DATA_OBSERVABILITY on table default.table_one");

        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE CREATE BRANCH ON TABLE table_one FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege CREATE BRANCH on table default.table_one");

        if (isGalaxyEnabled) {
            assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE ALL PRIVILEGES ON TABLE table_one FROM %s", randomUsername())))
                    .hasMessageContaining("Access Denied: Cannot revoke privilege SELECT on table default.table_one");
        }
        else {
            assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE ALL PRIVILEGES ON TABLE table_one FROM %s", randomUsername())))
                    .hasMessageContaining("Access Denied: Cannot revoke privilege CREATE on table default.table_one");
        }
    }

    private static Session sessionOf(String username)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(username))
                .setCatalog("local")
                .setSchema("default")
                .build();
    }
}
