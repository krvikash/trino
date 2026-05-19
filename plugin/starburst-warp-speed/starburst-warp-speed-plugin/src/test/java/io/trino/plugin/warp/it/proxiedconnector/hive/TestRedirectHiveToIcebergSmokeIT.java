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
package io.trino.plugin.warp.it.proxiedconnector.hive;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.QueryRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

public class TestRedirectHiveToIcebergSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    private static final String REDIRECTING_CATALOG = "hive_redirect";

    public TestRedirectHiveToIcebergSmokeIT()
    {
        super(1, "iceberg");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "iceberg.hive-catalog-name", REDIRECTING_CATALOG,
                        PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());

        queryRunner.installPlugin(new HivePlugin());
        Map<String, String> hiveCatProperties = Map.of(
                "hive.metastore", "file",
                "fs.hadoop.enabled", "true",
                "hive.iceberg-catalog-name", catalog,
                "hive.metastore.disable-location-checks", "true",
                "hive.metastore.catalog.dir", "file://" + hiveDir.toAbsolutePath());

        queryRunner.createCatalog(REDIRECTING_CATALOG, "hive", hiveCatProperties);
        return queryRunner;
    }
}
