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
package io.trino.plugin.warp.it.proxiedconnector.deltalake;

import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

public class TestDeltaLakeProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestDeltaLakeProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "varada_deltalake");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(new WarpStubsStorageEngineModule(),
                Optional.empty(), numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, DELTA_LAKE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Collections.emptyMap());
    }

    @Override
    @Test
    public void testGoAllProxyOnlyWhenHavePushDowns()
    {
        //Cannot insert to table in delta lake
    }

    @Test
    public void test_CTAS()
    {
        computeActual("CREATE TABLE t2 AS SELECT * FROM t");
        computeActual("DROP TABLE t2");
    }
}
