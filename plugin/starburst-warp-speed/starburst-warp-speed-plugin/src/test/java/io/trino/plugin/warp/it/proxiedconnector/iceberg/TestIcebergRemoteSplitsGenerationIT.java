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
package io.trino.plugin.warp.it.proxiedconnector.iceberg;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.WarpAbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT.getCustomMetrics;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergRemoteSplitsGenerationIT
        extends WarpAbstractTestQueryFramework
{
    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "remote_splits_generation";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                2,
                Map.of(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "iceberg.remote-splits-generation.enabled", "true",
                        "iceberg.remote-splits-generation.manifests-per-thread", "0",
                        PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG,
                new WarpPlugin(),
                Map.of());
        try {
            InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
            new IcebergPlugin().getFunctions().forEach(functions::functions);
            queryRunner.addFunctions(functions.build());
            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(CATALOG, SCHEMA));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testRemoteSplitsGenerationIsUsedThroughWarp()
    {
        try (TestTable table = newTrinoTable("%s.%s.test_remote_splits_generation".formatted(CATALOG, SCHEMA), "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                    getSession(),
                    "SELECT * FROM " + table.getName());

            assertThat(result.result().getOnlyColumnAsSet()).containsExactlyInAnyOrder(1, 2, 3);

            Map<String, Long> metrics = getCustomMetrics(result.queryId(), getDistributedQueryRunner());
            assertThat(metrics.get("remoteSplitsSource.taskCreateAttempts")).isGreaterThanOrEqualTo(1);
            assertThat(metrics.get("remoteSplitsSource.batchesFetched")).isGreaterThanOrEqualTo(1);
        }
    }
}
