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
package io.trino.plugin.hive.metastore.unity;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY;
import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY;
import static io.trino.plugin.hive.metastore.unity.DatabricksRetryUtils.DATABRICKS_THRIFT_TIMEOUT_RETRY_POLICY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDatabricksRetryUtils
{
    // Abbreviated form of the failure reported in ENG-27331, raised by the Databricks JDBC driver's Thrift
    // transport while closing the session after the statement already completed on the server.
    private static final String THRIFT_TRANSPORT_TIMEOUT_MESSAGE =
            "HTTP request failed by code: 500, status line: HTTP/1.1 500 Internal Server Error. " +
                    "Thrift Header : Futures timed out after [60000 milliseconds]";
    private static final String GENERIC_HTTP_500_MESSAGE =
            "HTTP request failed by code: 500, status line: HTTP/1.1 500 Internal Server Error.";

    @Test
    void testThriftTransportTimeoutIsRetried()
    {
        AtomicInteger attempts = new AtomicInteger();
        Failsafe.with(DATABRICKS_THRIFT_TIMEOUT_RETRY_POLICY)
                .run(() -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException(new SQLException(THRIFT_TRANSPORT_TIMEOUT_MESSAGE));
                    }
                });
        assertThat(attempts).hasValue(2);
    }

    @Test
    void testThriftTransportTimeoutIsRetriedWhenNestedInCausalChain()
    {
        AtomicInteger attempts = new AtomicInteger();
        Failsafe.with(DATABRICKS_THRIFT_TIMEOUT_RETRY_POLICY)
                .run(() -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("Failed to create TPCH tables", new RuntimeException(new SQLException(THRIFT_TRANSPORT_TIMEOUT_MESSAGE)));
                    }
                });
        assertThat(attempts).hasValue(2);
    }

    @Test
    void testGenericHttp500IsNotRetried()
    {
        // A blanket HTTP 500 retry was deliberately reverted, because a statement failing with 500 may have
        // already succeeded remotely; only the Thrift timeout signature is retried.
        assertNotRetried(DATABRICKS_THRIFT_TIMEOUT_RETRY_POLICY, GENERIC_HTTP_500_MESSAGE);
    }

    @Test
    void testUnrelatedFuturesTimeoutIsNotRetried()
    {
        assertNotRetried(DATABRICKS_THRIFT_TIMEOUT_RETRY_POLICY, "Futures timed out after [60000 milliseconds]");
    }

    @Test
    void testThriftTransportTimeoutIsNotCoveredByExistingPolicies()
    {
        assertNotRetried(DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY, THRIFT_TRANSPORT_TIMEOUT_MESSAGE);
        assertNotRetried(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY, THRIFT_TRANSPORT_TIMEOUT_MESSAGE);
    }

    private static void assertNotRetried(RetryPolicy<Object> retryPolicy, String message)
    {
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> Failsafe.with(retryPolicy)
                .run(() -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException(new SQLException(message));
                }))
                .rootCause()
                .isInstanceOf(SQLException.class)
                .hasMessage(message);
        assertThat(attempts).hasValue(1);
    }
}
