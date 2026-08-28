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

import com.google.inject.Scopes;
import io.trino.Session;
import io.trino.plugin.deltalake.FileTestingTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;
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
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.DELTA_LAKE_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PASS_THROUGH_DISPATCHER;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;

/**
 * Runs the MV substitution suite against a Warp Speed catalog proxying Delta Lake as the source, with
 * the materialization storage in a separate Iceberg catalog. Exercises
 * {@link DispatcherSubstitutionMetadata} unwrapping the dispatcher handle and delegating Delta Lake
 * source identity. Writing a Delta Lake table on the local file system needs the testing transaction
 * log synchronizer bound below, and the non-concurrent writes it opts into.
 */
public class TestWarpSpeedDeltaSourceMvSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        Path deltaDir = Files.createTempDirectory("delta_catalog_");
        closeAfterClass((Closeable) () -> deleteRecursively(deltaDir, ALLOW_INSECURE));

        return DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.of(binder -> newMapBinder(binder, String.class, TransactionLogSynchronizer.class)
                        .addBinding("file").to(FileTestingTransactionLogSynchronizer.class).in(Scopes.SINGLETON)),
                3,
                Map.of(),
                Map.of(
                        "http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "warp",
                        PROXIED_CONNECTOR, DELTA_LAKE_CONNECTOR_NAME,
                        PASS_THROUGH_DISPATCHER, DELTA_LAKE_CONNECTOR_NAME,
                        "delta.enable-non-concurrent-writes", "true"),
                deltaDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                sourceSchema.getCatalogName(),
                new WarpPlugin(),
                Map.of("materialized-view-substitution.support.enabled", "true"));
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }
}
