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
package io.trino.plugin.warp.substitution;

import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergOnIcebergMvSubstitutionTest;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.substitution.DispatcherSubstitutionMetadata;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

/**
 * Runs the Iceberg MV substitution suite against a Warp Speed catalog proxying Iceberg, exercising
 * Warp Speed's {@link DispatcherSubstitutionMetadata} delegation.
 */
public class TestWarpSpeedIcebergMvSubstitution
        extends AbstractIcebergOnIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        Path icebergDir = Files.createTempDirectory("iceberg_catalog_");

        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                2,
                Map.of(),
                Map.ofEntries(
                        Map.entry("http-server.log.enabled", "false"),
                        Map.entry(USE_HTTP_SERVER_PORT, "false"),
                        Map.entry("node.environment", "warp"),
                        Map.entry("iceberg.catalog.type", "TESTING_FILE_METASTORE"),
                        Map.entry("iceberg.file-format", "PARQUET"),
                        Map.entry("iceberg.format-version", "2"),
                        Map.entry(PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                        Map.entry(PASS_THROUGH_DISPATCHER, ICEBERG_CONNECTOR_NAME)),
                icebergDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                sourceSchema.getCatalogName(),
                new WarpPlugin(),
                Map.of("materialized-view-substitution.support.enabled", "true"));

        try {
            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("warpspeed", "schema");
    }
}
