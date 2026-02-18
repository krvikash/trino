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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.NodeVersion;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.SetCatalogProperties;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSession;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestSetCatalogPropertiesTask
{
    private static final String CONNECTOR_NAME = "tpch";

    private QueryRunner queryRunner;

    @BeforeEach
    public void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
    }

    @AfterEach
    public void tearDown()
    {
        try (QueryRunner ignored = queryRunner) {
            queryRunner = null;
        }
    }

    @Test
    public void testAddCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DOUBLE"))),
                """
                   "tpch.double-type-mapping" = 'DOUBLE'
                """,
                ImmutableList.of(
                        new Property(new Identifier("tpch.column-naming"), new StringLiteral("STANDARD")),
                        new Property(new Identifier("tpch.predicate-pushdown-enabled"), new StringLiteral("false"))),
                """
                   "tpch.column-naming" = 'STANDARD',
                   "tpch.double-type-mapping" = 'DOUBLE',
                   "tpch.predicate-pushdown-enabled" = 'false'
                """);
    }

    @Test
    public void testCatalogNameCaseSensitivity()
    {
        String createCatalogSql =
                """
                CREATE CATALOG %s USING %s
                WITH (
                %s)""";
        String suffix = randomNameSuffix();
        String catalog = "catalog_" + suffix;
        String catalogUpperCase = "Catalog_" + suffix;

        executeCreateCatalog(catalog, ImmutableList.of(
                new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DOUBLE"))));
        assertThat(catalogExists(catalog)).isTrue();
        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql, catalog, CONNECTOR_NAME,
                        """
                           "tpch.double-type-mapping" = 'DOUBLE'
                        """);

        executeSetCatalogProperties(catalogUpperCase, ImmutableList.of(
                new Property(new Identifier("tpch.column-naming"), new StringLiteral("STANDARD")),
                new Property(new Identifier("tpch.predicate-pushdown-enabled"), new StringLiteral("false"))));

        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql, catalog, CONNECTOR_NAME,
                        """
                           "tpch.column-naming" = 'STANDARD',
                           "tpch.double-type-mapping" = 'DOUBLE',
                           "tpch.predicate-pushdown-enabled" = 'false'
                        """);
    }

    @Test
    void testAddDuplicatedCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard"))),
                """
                   "tpch.column-naming" = 'standard'
                """,
                ImmutableList.of(
                        new Property(new NodeLocation(1, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(1, 30), "double")),
                        new Property(new NodeLocation(2, 1), new Identifier("tpch.double-type-mapping"), new StringLiteral(new NodeLocation(2, 30), "decimal"))),
                """
                   "tpch.column-naming" = 'standard',
                   "tpch.double-type-mapping" = 'decimal'
                """);
    }

    @Test
    public void testOverrideCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new Identifier("tpch.column-naming"), new StringLiteral("STANDARD")),
                        new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DOUBLE"))),
                """
                   "tpch.column-naming" = 'STANDARD',
                   "tpch.double-type-mapping" = 'DOUBLE'
                """,
                ImmutableList.of(
                        new Property(new Identifier("tpch.column-naming"), new StringLiteral("SIMPLIFIED")),
                        new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DECIMAL"))),
                """
                   "tpch.column-naming" = 'SIMPLIFIED',
                   "tpch.double-type-mapping" = 'DECIMAL'
                """);
    }

    @Test
    public void testRemoveCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        new Property(new Identifier("tpch.column-naming"), new StringLiteral("STANDARD")),
                        new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DOUBLE")),
                        new Property(new Identifier("tpch.predicate-pushdown-enabled"), new StringLiteral("false"))),
                """
                   "tpch.column-naming" = 'STANDARD',
                   "tpch.double-type-mapping" = 'DOUBLE',
                   "tpch.predicate-pushdown-enabled" = 'false'
                """,
                ImmutableList.of(
                        new Property(new Identifier("tpch.predicate-pushdown-enabled")),
                        new Property(new Identifier("tpch.column-naming"))),
                """
                   "tpch.double-type-mapping" = 'DOUBLE'
                """);
    }

    @Test
    public void testSetComplexCatalogProperties()
    {
        testSetProperties(
                ImmutableList.of(
                        // to remove:
                        new Property(new Identifier("tpch.double-type-mapping"), new StringLiteral("DOUBLE")),
                        // unchanged:
                        new Property(new Identifier("tpch.max-rows-per-page"), new StringLiteral("128")),
                        // to update:
                        new Property(new Identifier("tpch.predicate-pushdown-enabled"), new StringLiteral("false"))),
                """
                   "tpch.double-type-mapping" = 'DOUBLE',
                   "tpch.max-rows-per-page" = '128',
                   "tpch.predicate-pushdown-enabled" = 'false'
                """,
                ImmutableList.of(
                        // added:
                        new Property(new Identifier("tpch.column-naming"), new StringLiteral("STANDARD")),
                        // to remove:
                        new Property(new Identifier("tpch.double-type-mapping")),
                        // added:
                        new Property(new Identifier("tpch.partitioning-enabled"), new StringLiteral("true")),
                        // to update:
                        new Property(new Identifier("tpch.predicate-pushdown-enabled"), new StringLiteral("true")),
                        // added:
                        new Property(new Identifier("tpch.splits-per-node"), new StringLiteral("16"))),
                """
                   "tpch.column-naming" = 'STANDARD',
                   "tpch.max-rows-per-page" = '128',
                   "tpch.partitioning-enabled" = 'true',
                   "tpch.predicate-pushdown-enabled" = 'true',
                   "tpch.splits-per-node" = '16'
                """);
    }

    @Test
    void testAddOrReplaceCatalogFailure()
    {
        MockCatalogStore catalogStore = new MockCatalogStore();
        String catalog = "catalog_" + randomNameSuffix();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new MockCatalogStoreModule(catalogStore))
                        .addProperty("catalog.store", "mock"))) {
            queryRunner.installPlugin(new TpchPlugin());

            executeCreateCatalog(
                    queryRunner,
                    catalog,
                    ImmutableList.of(new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "standard"))));

            catalogStore.failAddOrReplaceCatalog();

            assertThatThrownBy(
                    () -> executeSetCatalogProperties(
                            queryRunner,
                            catalog,
                            ImmutableList.of(new Property(new NodeLocation(1, 1), new Identifier("tpch.column-naming"), new StringLiteral(new NodeLocation(1, 30), "simplified")))))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Add or replace catalog failed");

            assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                    .isEqualTo(
                            """
                            CREATE CATALOG %s USING tpch
                            WITH (
                               "tpch.column-naming" = 'standard'
                            )\
                            """.formatted(catalog));
        }
    }

    private void testSetProperties(List<Property> initialProperties, String showInitialProperties, List<Property> updatedProperties, String showExpectedProperties)
    {
        String createCatalogSql =
                """
                CREATE CATALOG %s USING %s
                WITH (
                %s)""";
        String catalog = "catalog_" + randomNameSuffix();

        executeCreateCatalog(catalog, initialProperties);
        assertThat(catalogExists(catalog)).isTrue();
        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(catalog, CONNECTOR_NAME, showInitialProperties));

        executeSetCatalogProperties(catalog, updatedProperties);
        assertThat(catalogExists(catalog)).isTrue();
        assertThat((String) queryRunner.execute("SHOW CREATE CATALOG " + catalog).getOnlyValue())
                .isEqualTo(createCatalogSql.formatted(catalog, CONNECTOR_NAME, showExpectedProperties));
    }

    private void executeCreateCatalog(String catalogA, List<Property> catalogProperties)
    {
        executeCreateCatalog(queryRunner, catalogA, catalogProperties);
    }

    private void executeCreateCatalog(QueryRunner queryRunner, String catalogA, List<Property> catalogProperties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        CreateCatalogTask task = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        CreateCatalog statement = new CreateCatalog(new NodeLocation(1, 1), new Identifier(catalogA), false, new Identifier(CONNECTOR_NAME), catalogProperties, Optional.empty(), Optional.empty());
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private void executeSetCatalogProperties(String catalogName, List<Property> properties)
    {
        executeSetCatalogProperties(queryRunner, catalogName, properties);
    }

    private void executeSetCatalogProperties(QueryRunner queryRunner, String catalogName, List<Property> properties)
    {
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(Key.get(new TypeLiteral<>() {}));
        SetCatalogPropertiesTask task = (SetCatalogPropertiesTask) tasks.get(SetCatalogProperties.class);
        SetCatalogProperties statement = new SetCatalogProperties(new Identifier(catalogName), properties);
        ListenableFuture<Void> future = task.execute(statement, createNewQuery(queryRunner), emptyList(), WarningCollector.NOOP);
        getFutureValue(future);
    }

    private boolean catalogExists(String catalogB)
    {
        return queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery(queryRunner).getSession(), catalogB);
    }

    private QueryStateMachine createNewQuery(QueryRunner queryRunner)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                testSession(queryRunner.getDefaultSession()),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getPlannerContext().getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, Duration.ofMillis(1)),
                Optional.empty(),
                true,
                Optional.empty(),
                new NodeVersion("test"));
    }
}
