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

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static io.trino.sql.query.QueryAssertions.QueryAssert.collectGpuPlanNodes;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.writeString;
import static org.assertj.core.api.Assertions.assertThat;

public final class GpuQueriesTests
{
    private GpuQueriesTests() {}

    static void assertGpuQueryResultsAndOperators(QueryRunner runner, @Language("SQL") String sql, String expectedGpuPlanCoverage)
    {
        Session session = runner.getDefaultSession();
        Session withoutGpu = withoutGpu(session);
        MaterializedResultWithPlan result = runner.executeWithPlan(session, sql);
        QueryStats queryStats = runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats();

        assertThat(result.result().getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(runner.execute(withoutGpu, sql));

        PlanNode queryPlan = result.queryPlan().orElseThrow(() -> new AssertionError("No plan")).getRoot();
        assertThat(printGpuPlan(queryPlan, queryStats))
                .isEqualTo(expectedGpuPlanCoverage);
    }

    static void updateGpuOperators(QueryRunner runner, @Language("SQL") String sql, Path filePath)
            throws Exception
    {
        createDirectories(filePath.getParent());

        Session session = runner.getDefaultSession();
        MaterializedResultWithPlan result = runner.executeWithPlan(session, sql);
        QueryStats queryStats = runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats();

        PlanNode queryPlan = result.queryPlan().orElseThrow(() -> new AssertionError("No plan")).getRoot();
        String gpuPlan = printGpuPlan(queryPlan, queryStats);
        writeString(filePath, gpuPlan);
    }

    private static String printGpuPlan(PlanNode planNode, QueryStats queryStats)
    {
        Set<PlanNodeId> onGpu = collectGpuPlanNodes(queryStats);
        StringBuilder output = new StringBuilder();
        printGpuPlan(planNode, onGpu::contains, output, 0);
        return output.toString();
    }

    private static void printGpuPlan(PlanNode planNode, Predicate<PlanNodeId> onGpu, StringBuilder output, int indentLevel)
    {
        String indent = "  ".repeat(indentLevel);
        output.append(indent).append("- ").append(planNode.getClass().getSimpleName());
        switch (planNode) {
            case ExchangeNode exchange -> output.append(" ").append(exchange.getScope());
            case TableScanNode tableScan -> {
                String tableName = switch (tableScan.getTable().connectorHandle()) {
                    case HiveTableHandle hiveTableHandle -> hiveTableHandle.getTableName();
                    case IcebergTableHandle icebergTableHandle -> icebergTableHandle.getTableName();
                    case ConnectorTableHandle other -> throw new UnsupportedOperationException("Unsupported connector handle type: %s [%s]".formatted(other.getClass(), other));
                };
                output.append(" ").append(tableName);
            }
            default -> {}
        }
        if (onGpu.test(planNode.getId())) {
            output.append(" (GPU)");
        }
        output.append("\n");
        for (PlanNode source : planNode.getSources()) {
            printGpuPlan(source, onGpu, output, indentLevel + 1);
        }
    }

    private static Session withoutGpu(Session baseSession)
    {
        return Session.builder(baseSession)
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
    }

    private static Path getModuleSourcePath()
    {
        Path workingDir = Path.of(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        if (isDirectory(workingDir.resolve(".git"))) {
            // Top-level of the repo
            return workingDir.resolve("testing/trino-tests");
        }
        if (workingDir.getFileName().toString().equals("trino-tests")) {
            return workingDir;
        }
        throw new IllegalStateException("This class must be executed from trino-tests or Trino source directory");
    }

    static final class UpdateExpectedPlans
    {
        private UpdateExpectedPlans() {}

        private static final Logger log = Logger.get(UpdateExpectedPlans.class);

        static void main()
        {
            Logging.initialize();

            try {
                // in alphabetical order
                generateGpuPlans(new TestDistributedHiveGpuTpcdsQueries());
                generateGpuPlans(new TestDistributedHiveGpuTpchQueries());
                generateGpuPlans(new TestDistributedIcebergGpuTpcdsQueries());
                generateGpuPlans(new TestDistributedIcebergGpuTpchQueries());
                generateGpuPlans(new TestHiveGpuTpcdsQueries());
                generateGpuPlans(new TestHiveGpuTpchQueries());
                generateGpuPlans(new TestIcebergGpuTpcdsQueries());
                generateGpuPlans(new TestIcebergGpuTpchQueries());
            }
            catch (Throwable t) {
                log.error(t);
                // Prevent QueryRunner's background threads from preventing the JVM shutdown
                System.exit(1);
            }
        }

        private static void generateGpuPlans(GpuPlanTest test)
                throws Exception
        {
            log.info("Generating GPU plans for %s", test.getClass().getSimpleName());
            Method init = AbstractTestQueryFramework.class.getDeclaredMethod("init");
            init.setAccessible(true);
            Method close = AbstractTestQueryFramework.class.getDeclaredMethod("close");
            close.setAccessible(true);

            try {
                init.invoke(test);
                QueryRunner runner = test.accessQueryRunner();
                for (String query : test.queries().toArray(String[]::new)) {
                    Path filePath = getModuleSourcePath().resolve("src/test/resources/" + test.gpuPlanResource(query));
                    log.info("Writing GPU plan for query %s to %s", query, filePath);
                    updateGpuOperators(runner, test.readQuery(query), filePath);
                }
            }
            finally {
                close.invoke(test);
            }
        }
    }

    abstract static class GpuPlanTest
            extends AbstractTestQueryFramework
    {
        // io.trino.testing.AbstractTestQueryFramework.getQueryRunner is protected
        private QueryRunner accessQueryRunner()
        {
            return getQueryRunner();
        }

        abstract Stream<String> queries();

        abstract String readQuery(String query)
                throws IOException;

        abstract String gpuPlanResource(String query);
    }
}
