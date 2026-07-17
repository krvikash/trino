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
import io.trino.connector.MutableGrants;
import io.trino.execution.starburst.GalaxyPrivilegeUtilities;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestGrantOnSchema
        extends AbstractTestQueryFramework
{
    private static final Set<Privilege> SCHEMA_PRIVILEGES = ImmutableSet.of(Privilege.CREATE);

    private final boolean isGalaxyEnabled;
    private final Session admin = sessionOf("admin");
    private final Grants<String> schemaGrants = new MutableGrants<>();

    public AbstractTestGrantOnSchema(boolean isGalaxyEnabled)
    {
        this.isGalaxyEnabled = isGalaxyEnabled;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(admin)
                .setAdditionalSetup(queryRunner -> {
                    MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                            .withListSchemaNames(_ -> ImmutableList.of("information_schema", "default"))
                            .withListTables((_, schema) ->
                                    "default".equalsIgnoreCase(schema) ? ImmutableList.of("table_one") : ImmutableList.of())
                            .withSchemaGrants(schemaGrants)
                            .build();
                    queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
                    queryRunner.createCatalog("local", "mock");
                })
                .setAdditionalModule(binder -> {
                    if (isGalaxyEnabled) {
                        newOptionalBinder(binder, PrivilegeUtilitiesApi.class).setBinding().to(GalaxyPrivilegeUtilities.class).in(SINGLETON);
                    }
                })
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        schemaGrants.grant(new TrinoPrincipal(USER, admin.getUser()), "default", EnumSet.allOf(Privilege.class), true);
    }

    @Test
    public void testExistingGrants()
    {
        testExistingGrants(true);
        testExistingGrants(false);
    }

    private void testExistingGrants(boolean grantOption)
    {
        String username = randomUsername();
        Session user = sessionOf(username);
        if (isGalaxyEnabled) {
            schemaGrants.grant(new TrinoPrincipal(USER, user.getUser()), "default", SCHEMA_PRIVILEGES, grantOption);
        }
        else {
            schemaGrants.grant(new TrinoPrincipal(USER, user.getUser()), "default", EnumSet.allOf(Privilege.class), grantOption);
        }

        assertThat(query(admin, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(query(admin, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test
    public void testValidGrant()
    {
        testValidGrant("CREATE");
        testValidGrant("ALL PRIVILEGES");
    }

    private void testValidGrant(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        assertUpdate(admin, format("GRANT %s ON SCHEMA default TO %s", privilege, username));

        assertThat(query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(query(admin, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test
    public void testValidGrantWithGrantOption()
    {
        testValidGrantWithGrantOption("CREATE");
        testValidGrantWithGrantOption("ALL PRIVILEGES");
    }

    private void testValidGrantWithGrantOption(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        assertUpdate(admin, format("GRANT %s ON SCHEMA default TO %s WITH GRANT OPTION", privilege, username));

        assertThat(query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertUpdate(user, format("GRANT %s ON SCHEMA default TO %s", privilege, randomUsername()));
        assertUpdate(user, format("GRANT %s ON SCHEMA default TO %s WITH GRANT OPTION", privilege, randomUsername()));
    }

    @Test
    public void testGrantOnNonExistingCatalog()
    {
        assertThat(query(admin, format("GRANT SELECT ON SCHEMA missing_catalog.missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
        assertThat(query(admin, format("GRANT CREATE ON SCHEMA missing_catalog.missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
        assertThat(query(admin, format("GRANT ALL PRIVILEGES ON SCHEMA missing_catalog.missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
    }

    @Test
    public void testGrantOnNonExistingSchema()
    {
        assertThat(query(admin, format("GRANT SELECT ON SCHEMA missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'local.missing_schema' does not exist");
        assertThat(query(admin, format("GRANT CREATE ON SCHEMA missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'local.missing_schema' does not exist");
        assertThat(query(admin, format("GRANT ALL PRIVILEGES ON SCHEMA missing_schema TO %s", randomUsername())))
                .failure().hasMessageContaining("Schema 'local.missing_schema' does not exist");
    }

    @Test
    public void testAccessDenied()
    {
        assertThat(query(sessionOf(randomUsername()), format("GRANT CREATE ON SCHEMA default TO %s", randomUsername())))
                .failure().hasMessageContaining("Access Denied: Cannot grant privilege CREATE on schema default");
        assertThat(query(sessionOf(randomUsername()), format("GRANT ALL PRIVILEGES ON SCHEMA default TO %s", randomUsername())))
                .failure().hasMessageContaining("Access Denied: Cannot grant privilege CREATE on schema default");
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
