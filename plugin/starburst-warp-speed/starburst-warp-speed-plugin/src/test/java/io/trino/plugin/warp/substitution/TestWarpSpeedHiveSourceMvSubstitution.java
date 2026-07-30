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
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.warp.dispatcher.substitution.DispatcherSubstitutionMetadata;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

/**
 * Runs the MV substitution suite against a Warp Speed catalog proxying Hive as the source, with the
 * materialization storage in a separate Iceberg catalog (Warp Speed over Hive cannot host Iceberg
 * MVs). Exercises {@link DispatcherSubstitutionMetadata} unwrapping the dispatcher handle and
 * delegating Hive source identity. The DELETE-based staleness tests are skipped because Hive ACID
 * merge is not available here (see {@link #sourceSupportsRowLevelDelete()}).
 */
public class TestWarpSpeedHiveSourceMvSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        Path hiveDir = Files.createTempDirectory("hive_catalog_");
        closeAfterClass((Closeable) () -> deleteRecursively(hiveDir, ALLOW_INSECURE));
        Path icebergDir = Files.createTempDirectory("iceberg_catalog_");
        closeAfterClass((Closeable) () -> deleteRecursively(icebergDir, ALLOW_INSECURE));

        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                3,
                Map.of(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME,
                        PASS_THROUGH_DISPATCHER, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                sourceSchema.getCatalogName(),
                new WarpPlugin(),
                Map.of("materialized-view-substitution.support.enabled", "true"));

        return queryRunner;
    }

    @Override
    protected boolean sourceSupportsRowLevelDelete()
    {
        // The proxied Hive connector here has no ACID metastore, so individual rows cannot be
        // deleted from a source table; the staleness tests that rely on it are skipped.
        return false;
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }
}
