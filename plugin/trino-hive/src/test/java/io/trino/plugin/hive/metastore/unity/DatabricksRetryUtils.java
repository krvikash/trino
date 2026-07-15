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

import com.databricks.sdk.core.error.platform.InternalError;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.base.Throwables.getStackTraceAsString;

public final class DatabricksRetryUtils
{
    private static final Logger LOG = Logger.get(DatabricksRetryUtils.class);

    private static final Pattern DATABRICKS_COMMUNICATION_FAILURE_MATCH = Pattern.compile(
            "\\Q[Databricks][\\E(DatabricksJDBCDriver|JDBCDriver)\\Q](500593) Communication link failure. Failed to connect to server. Reason: " +
                    "TemporarilyUnavailableRetry timeout of 900 seconds has been hit.\\E.*");
    private static final String DATABRICKS_CLUSTER_PENDING_MATCH = "The current cluster state is Pending";
    private static final String DATABRICKS_CLUSTER_TERMINATED_MATCH = "The current cluster state is Terminated";

    public static final RetryPolicy<Object> DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(DatabricksRetryUtils::isDatabricksCommunicationFailure)
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(30)
            .onRetry(event -> LOG.warn(event.getLastException(), "Query failed on attempt %d, will retry (communication failure).", event.getAttemptCount()))
            .build();
    public static final RetryPolicy<Object> DATABRICKS_CLUSTER_UNAVAILABLE_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(DatabricksRetryUtils::isClusterUnavailable)
            .withDelay(Duration.ofSeconds(30))
            .withMaxRetries(80)
            .onRetry(event -> LOG.warn(event.getLastException(), "Query failed on attempt %d, will retry (cluster unavailable).", event.getAttemptCount()))
            .build();
    public static final RetryPolicy<Object> UNITY_CATALOG_TRANSIENT_ERROR_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(DatabricksRetryUtils::isUnityCatalogTransientError)
            .withBackoff(1, 30, ChronoUnit.SECONDS)
            .withMaxRetries(3)
            .onRetry(event -> LOG.warn(event.getLastException(), "Query failed on attempt %d, will retry (Unity Catalog transient error).", event.getAttemptCount()))
            .build();

    private DatabricksRetryUtils() {}

    private static boolean isDatabricksCommunicationFailure(Throwable throwable)
    {
        if (isClusterUnavailable(throwable)) {
            return false;
        }
        Throwable rootCause = getRootCause(throwable);
        return rootCause instanceof SQLException &&
                rootCause.getMessage() != null &&
                DATABRICKS_COMMUNICATION_FAILURE_MATCH.matcher(rootCause.getMessage()).find();
    }

    private static boolean isClusterUnavailable(Throwable throwable)
    {
        String stackTrace = getStackTraceAsString(throwable);
        return stackTrace.contains(DATABRICKS_CLUSTER_PENDING_MATCH) || stackTrace.contains(DATABRICKS_CLUSTER_TERMINATED_MATCH)
                // 502 is safe to retry at any point: all DDL uses IF NOT EXISTS or CREATE OR REPLACE
                || stackTrace.contains("HTTP request failed by code: 502");
    }

    private static boolean isUnityCatalogTransientError(Throwable throwable)
    {
        String stackTrace = getStackTraceAsString(throwable);
        return stackTrace.contains("TEMPORARILY_UNAVAILABLE")
                || getCausalChain(throwable).stream().anyMatch(InternalError.class::isInstance);
    }
}
