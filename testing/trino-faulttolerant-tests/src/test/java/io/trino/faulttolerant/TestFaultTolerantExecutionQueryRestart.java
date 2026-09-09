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
package io.trino.faulttolerant;

import com.google.inject.Key;
import io.trino.Session;
import io.trino.execution.scheduler.faulttolerant.FaultTolerantQueryRestartStats;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_QUERY_RETRY_ENABLED;
import static io.trino.SystemSessionProperties.QUERY_RETRY_ATTEMPTS;
import static io.trino.SystemSessionProperties.RETRY_INITIAL_DELAY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestFaultTolerantExecutionQueryRestart
        extends AbstractTestQueryFramework
{
    private final ExchangeFailureInjector failureInjector = new ExchangeFailureInjector();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setWorkerCount(1)
                .withoutExchange()
                .build();

        queryRunner.installPlugin(new FailureInjectingExchangePlugin(failureInjector));
        queryRunner.loadExchangeManager(
                FailureInjectingExchangeManagerFactory.NAME,
                Map.of(
                        "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
        return queryRunner;
    }

    @AfterEach
    public void resetFailureInjection()
    {
        failureInjector.stopInjectingFailures();
    }

    private long queryRestarts()
    {
        return getDistributedQueryRunner().getCoordinator().getInstance(Key.get(FaultTolerantQueryRestartStats.class))
                .getQueryRestartsDueToExchangeDataLoss()
                .getTotalCount();
    }

    private Session retrySession()
    {
        return testSessionBuilder(getSession())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_QUERY_RETRY_ENABLED, "true")
                .setSystemProperty(QUERY_RETRY_ATTEMPTS, "3")
                .build();
    }

    @Test
    public void testQuerySucceedsWithoutInjectedFailure()
    {
        assertQuery(
                retrySession(),
                "SELECT count(*) FROM tpch.tiny.orders",
                "SELECT 15000");
    }

    @Test
    public void testRetryWithAggregation()
    {
        failureInjector.injectFailure();
        assertQuery(
                retrySession(),
                "SELECT orderstatus, count(*) FROM tpch.tiny.orders GROUP BY orderstatus ORDER BY orderstatus",
                "VALUES ('F', 7304), ('O', 7333), ('P', 363)");
    }

    @Test
    public void testQuerySucceedsAfterSingleExchangeFailure()
    {
        failureInjector.injectFailure();
        assertQuery(
                retrySession(),
                "SELECT count(*) FROM tpch.tiny.orders",
                "SELECT 15000");
        assertThat(failureInjector.failuresRemaining()).isEqualTo(0);
    }

    @Test
    public void testRestartsBackToBackWithoutDelay()
    {
        // With no delay the next attempt starts while the failed one is still aborting its stages and tasks. Stage ids
        // are the same in every attempt, so this checks that nothing the old attempt tears down reaches the new one.
        int restarts = 3;
        failureInjector.injectFailures(restarts);
        long restartsBeforeRunningQuery = queryRestarts();
        Session session = testSessionBuilder(retrySession())
                .setSystemProperty(RETRY_INITIAL_DELAY, "0s")
                .build();

        // a single consumer task per attempt, so each injected failure costs exactly one restart
        assertQuery(session, "SELECT count(*) FROM tpch.tiny.orders", "SELECT 15000");

        assertThat(failureInjector.failuresRemaining()).isEqualTo(0);
        assertThat(queryRestarts() - restartsBeforeRunningQuery).isEqualTo(restarts);
    }

    @Test
    public void testQueryFailsWhenAllAttemptsExhausted()
    {
        failureInjector.injectFailures(1000);
        long restartsBeforeRunningQuery = queryRestarts();
        Integer allowedQueryRestarts = 1;

        Session session = testSessionBuilder(getSession())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_QUERY_RETRY_ENABLED, "true")
                .setSystemProperty(QUERY_RETRY_ATTEMPTS, allowedQueryRestarts.toString())
                .build();

        assertThatThrownBy(() -> computeActual(session, "SELECT count(*) FROM tpch.tiny.orders"))
                .hasMessageContaining("Injected exchange data loss");

        long restartsAfterRunningTheQuery = queryRestarts();
        assertThat(restartsAfterRunningTheQuery - restartsBeforeRunningQuery).isEqualTo(allowedQueryRestarts.intValue());
    }

    @Test
    public void testQueryFailsImmediatelyWhenRetryDisabled()
    {
        failureInjector.injectFailures(1000);
        long restartsBeforeRunningQuery = queryRestarts();
        int noRestartsAllowed = 0;

        Session session = testSessionBuilder(getSession())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_QUERY_RETRY_ENABLED, "false")
                .build();

        assertThatThrownBy(() -> computeActual(session, "SELECT count(*) FROM tpch.tiny.orders"))
                .hasMessageContaining("Injected exchange data loss");

        long restartsAfterRunningTheQuery = queryRestarts();
        assertThat(restartsAfterRunningTheQuery - restartsBeforeRunningQuery).isEqualTo(noRestartsAllowed);
    }

    @Test
    public void testInsertIsRestarted()
    {
        // nothing is committed before the final stage finishes, so a restarted write commits exactly once and the
        // connector drops whatever the discarded attempt wrote under the same query id
        assertUpdate("CREATE TABLE iceberg.tpch.restart_insert_target (nationkey bigint, name varchar)");
        try {
            long restartsBeforeRunningQuery = queryRestarts();
            failureInjector.injectFailure();
            assertUpdate(retrySession(), "INSERT INTO iceberg.tpch.restart_insert_target SELECT nationkey, name FROM tpch.tiny.nation", 25);
            assertThat(failureInjector.failuresRemaining()).isEqualTo(0);
            assertThat(queryRestarts() - restartsBeforeRunningQuery).isEqualTo(1);
            assertQuery("SELECT count(*), count(DISTINCT nationkey) FROM iceberg.tpch.restart_insert_target", "SELECT 25, 25");
        }
        finally {
            assertUpdate("DROP TABLE iceberg.tpch.restart_insert_target");
        }
    }

    @Test
    public void testCreateTableAsSelectIsRestarted()
    {
        try {
            long restartsBeforeRunningQuery = queryRestarts();
            failureInjector.injectFailure();
            assertUpdate(retrySession(), "CREATE TABLE iceberg.tpch.restart_ctas_target AS SELECT nationkey, name FROM tpch.tiny.nation", 25);
            assertThat(failureInjector.failuresRemaining()).isEqualTo(0);
            assertThat(queryRestarts() - restartsBeforeRunningQuery).isEqualTo(1);
            assertQuery("SELECT count(*), count(DISTINCT nationkey) FROM iceberg.tpch.restart_ctas_target", "SELECT 25, 25");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS iceberg.tpch.restart_ctas_target");
        }
    }
}
