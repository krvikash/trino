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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMaterializedViewsOperation
        extends AbstractTestQueryFramework
{
    private static final String EXTRA_CREDENTIAL_KEY = "alternate_table_name";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("mock")
                        .setSchema("mock")
                        .build())
                .build();

        queryRunner.installPlugin(
                new MockConnectorPlugin(
                        MockConnectorFactory.builder()
                                .withRowFilter(_ -> null)
                                .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("table_name", VARCHAR)))
                                .withRedirectTable((session, schemaTableName) -> {
                                    String virtualTableName = session.getUser() + "_" + session.getIdentity().getExtraCredentials().getOrDefault(EXTRA_CREDENTIAL_KEY, "");
                                    // Redirect to a virtual table named as current user
                                    if (!schemaTableName.getTableName().equals(virtualTableName)) {
                                        return Optional.of(new CatalogSchemaTableName(
                                                "mock",
                                                schemaTableName.getSchemaName(),
                                                virtualTableName));
                                    }
                                    return Optional.empty();
                                })
                                .withGetMaterializedViewsFreshness((_, _) -> new MaterializedViewFreshness(STALE, Optional.empty()))
                                .withGetMaterializedViews((_, _) -> ImmutableMap.of(
                                        new SchemaTableName("default", "materialized_view_with_no_owner_and_should_use_invoker"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                true),
                                        new SchemaTableName("default", "materialized_view_with_no_owner"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                false),
                                        new SchemaTableName("default", "materialized_view_with_owner_and_should_use_invoker"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                true),
                                        new SchemaTableName("default", "materialized_view_with_owner"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.of("mv_owner"),
                                                ImmutableList.of(),
                                                false)))
                                .withData(schemaTableName -> ImmutableList.of(ImmutableList.of(schemaTableName.getTableName())))
                                .build()));
        queryRunner.createCatalog("mock", "mock");

        return queryRunner;
    }

    @Test
    public void testSelectFromMaterializedViewWithNoOwnerSpecifiedAndShouldUseInvoker()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'alice_credential'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'bob_new_credential'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'charlie_credential'");
    }

    @Test
    public void testSelectFromMaterializedViewWithNoOwnerSpecified()
    {
        assertQueryFails(
                sessionForUser("alice", "credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
        assertQueryFails(
                sessionForUser("bob", "new_credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
        assertQueryFails(
                sessionForUser("charlie", "credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
    }

    @Test
    public void testSelectFromMaterializedViewWithSpecificOwnerAndShouldUseInvoker()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'alice_credential'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'bob_new_credential'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'charlie_credential'");
    }

    @Test
    public void testSelectFromMaterializedViewWithSpecificOwner()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
    }

    private Session sessionForUser(String user, String extraCredential)
    {
        return testSessionBuilder()
                .setCatalog("mock")
                .setSchema("mock")
                .setIdentity(
                        Identity.forUser(user)
                                .withExtraCredentials(ImmutableMap.of(EXTRA_CREDENTIAL_KEY, extraCredential))
                                .build())
                .build();
    }
}
