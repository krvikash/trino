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
package io.trino.testing.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestMaterializedView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public abstract class AbstractMvSubstitutionTest
        extends AbstractTestQueryFramework
{
    private static final String MV_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";
    protected static final Map<String, String> SUBSTITUTION_ENABLED = ImmutableMap.of("substitution_enabled", "true");
    protected static final Map<String, String> SUBSTITUTION_DISABLED = ImmutableMap.of("substitution_enabled", "false");

    protected void dropSourceTable(CatalogSchemaTableName sourceTable)
    {
        sourceSqlExecutor().execute("DROP TABLE IF EXISTS %s".formatted(sourceTableReference(sourceTable)));
    }

    protected String sourceTableReference(CatalogSchemaTableName sourceTable)
    {
        return sourceTable.toString();
    }

    protected Session sessionWithSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "true")
                .build();
    }

    protected Session sessionWithoutSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "false")
                .build();
    }

    protected void assertSameResults(Session session, String sql)
    {
        MaterializedResult substituted = computeActual(session, sql);
        MaterializedResult baseline = computeActual(sql);
        assertThat(substituted.getMaterializedRows())
                .as("Substituted query should return same results as base table query")
                .containsExactlyInAnyOrderElementsOf(baseline.getMaterializedRows());
    }

    protected void assertSameResults(Session session, String actual, String expected)
    {
        MaterializedResult actualResults = computeActual(session, actual);
        MaterializedResult expectedResults = computeActual(session, expected);
        assertThat(actualResults.getMaterializedRows())
                .as("Query '%s' should return same results as '%s'".formatted(actual, expected))
                .containsExactlyInAnyOrderElementsOf(expectedResults.getMaterializedRows());
    }

    protected TestMaterializedView createFreshMv(String namePrefix, @Language("SQL") String sql)
    {
        return createFreshMv(namePrefix, sql, ImmutableMap.of());
    }

    protected TestMaterializedView createFreshMv(String namePrefix, @Language("SQL") String sql, Map<String, String> properties)
    {
        return createFreshMv(namePrefix, sql, properties, Optional.empty());
    }

    protected TestMaterializedView createMvWithoutRefresh(String namePrefix, @Language("SQL") String sql, Map<String, String> properties)
    {
        return new TestMaterializedView(getQueryRunner()::execute, mvName(namePrefix), sql, properties);
    }

    protected TestMaterializedView createSubstitutionMv(String namePrefix, @Language("SQL") String sql)
    {
        return createFreshMv(namePrefix, sql, SUBSTITUTION_ENABLED, Optional.empty());
    }

    protected TestMaterializedView createSubstitutionMv(String namePrefix, @Language("SQL") String sql, Duration gracePeriod)
    {
        return createFreshMv(namePrefix, sql, SUBSTITUTION_ENABLED, Optional.of(gracePeriod));
    }

    protected TestMaterializedView createFreshMv(String namePrefix, @Language("SQL") String sql, Map<String, String> properties, Optional<Duration> gracePeriod)
    {
        TestMaterializedView materializedView = new TestMaterializedView(getQueryRunner()::execute, mvName(namePrefix), sql, properties, gracePeriod);
        try {
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + materializedView.mvName());
        }
        catch (Exception e) {
            // drop the materialized view if refresh fails
            try (TestMaterializedView ignored = materializedView) {
                throw e;
            }
        }
        return materializedView;
    }

    protected abstract CatalogSchemaName mvSchema();

    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("source", "schema");
    }

    protected String sourceSchemaName()
    {
        return sourceSchema().getSchemaName();
    }

    protected CatalogSchemaTableName sourceTable(String tableName)
    {
        return new CatalogSchemaTableName(sourceSchema().getCatalogName(), sourceSchema().getSchemaName(), tableName);
    }

    protected SqlExecutor sourceSqlExecutor()
    {
        return getQueryRunner()::execute;
    }

    protected QueryRunner.MaterializedResultWithPlan assertSubstituted(String query, CatalogSchemaTableName baseTableName, CatalogSchemaTableName mvName, CatalogSchemaTableName... expectedNotSubstitutedTables)
    {
        return assertSubstituted(true, query, baseTableName, mvName, expectedNotSubstitutedTables);
    }

    protected QueryRunner.MaterializedResultWithPlan assertSubstituted(boolean validateResults, String query, CatalogSchemaTableName baseTableName, CatalogSchemaTableName mvName, CatalogSchemaTableName... expectedNotSubstitutedTables)
    {
        return assertSubstituted(validateResults, query, baseTableName, Set.of(mvName), expectedNotSubstitutedTables);
    }

    protected QueryRunner.MaterializedResultWithPlan assertSubstituted(
            String query,
            CatalogSchemaTableName baseTableName,
            Set<CatalogSchemaTableName> materializedViews,
            CatalogSchemaTableName... expectedNotSubstitutedTables)
    {
        return assertSubstituted(true, query, baseTableName, materializedViews, expectedNotSubstitutedTables);
    }

    protected QueryRunner.MaterializedResultWithPlan assertSubstituted(
            boolean validateResults,
            String query,
            CatalogSchemaTableName baseTableName,
            Set<CatalogSchemaTableName> materializedViews,
            CatalogSchemaTableName... expectedNotSubstitutedTables)
    {
        Session session = sessionWithSubstitution();
        QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<CatalogSchemaTableName> scannedTableNames = getScannedTableNames(result);

        assertThat(scannedTableNames)
                .as("Expected at least one table scan in plan")
                .isNotEmpty();
        assertThat(scannedTableNames)
                .as("Expected no scan on base tables '%s' — should be substituted with MV storage table", baseTableName)
                .doesNotContain(unquote(baseTableName));
        if (expectedNotSubstitutedTables.length > 0) {
            assertThat(scannedTableNames)
                    .as("Expected scan on base tables '%s' — should not be substituted with MV storage table", Arrays.toString(expectedNotSubstitutedTables))
                    .containsAll(unquote(expectedNotSubstitutedTables));
        }

        List<String> storageTableNames = materializedViews.stream()
                .map(mvName -> getMaterializedViewDefinition(session, mvName)
                        .getStorageTable()
                        .orElseThrow()
                        .getSchemaTableName()
                        .getTableName())
                .collect(toImmutableList());
        assertThat(scannedTableNames)
                .extracting(table -> table.getSchemaTableName().getTableName())
                .as("Expected at least one scan on MV storage table")
                .containsAnyElementsOf(storageTableNames);

        if (validateResults) {
            MaterializedResult baseline = computeActual(sessionWithoutSubstitution(), query);
            assertThat(result.result().getMaterializedRows())
                    .as("Substituted query should return same results as base table query")
                    .containsExactlyInAnyOrderElementsOf(baseline.getMaterializedRows());
        }
        return result;
    }

    protected MaterializedViewDefinition getMaterializedViewDefinition(Session session, CatalogSchemaTableName mvName)
    {
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), metadata, getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(
                            mvName.getCatalogName(),
                            mvName.getSchemaTableName().getSchemaName(),
                            mvName.getSchemaTableName().getTableName());
                    return metadata.getMaterializedView(transactionSession, name).orElseThrow();
                });
    }

    protected QueryRunner.MaterializedResultWithPlan assertNotSubstituted(Session session, String query, CatalogSchemaTableName baseTable)
    {
        QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<CatalogSchemaTableName> scannedTableNames = getScannedTableNames(result);
        assertThat(scannedTableNames)
                .as("Expected scan on base table '%s' — no substitution should happen", baseTable)
                .contains(unquote(baseTable));

        return result;
    }

    private static List<CatalogSchemaTableName> unquote(CatalogSchemaTableName... baseTables)
    {
        return Arrays.stream(baseTables)
                .map(AbstractMvSubstitutionTest::unquote)
                .collect(toImmutableList());
    }

    private static CatalogSchemaTableName unquote(CatalogSchemaTableName baseTable)
    {
        return new CatalogSchemaTableName(
                baseTable.getCatalogName().replaceAll("\"", ""),
                baseTable.getSchemaTableName().getSchemaName().replaceAll("\"", ""),
                baseTable.getSchemaTableName().getTableName().replaceAll("\"", ""));
    }

    private List<CatalogSchemaTableName> getScannedTableNames(QueryRunner.MaterializedResultWithPlan result)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getInputs().stream()
                .map(input -> new CatalogSchemaTableName(input.catalogName(), input.schema(), input.table()))
                .collect(toImmutableList());
    }

    protected CatalogSchemaTableName mvName(String mvName)
    {
        return new CatalogSchemaTableName(mvSchema().getCatalogName(), mvSchema().getSchemaName(), mvName + randomNameSuffix());
    }
}
