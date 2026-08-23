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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

final class TestIcebergRestScanPlanningMetastoreCacheTest
        extends BaseIcebergRestCatalogMetastoreCacheTest
{
    @TempDir
    private static Path warehouseLocation;

    @Override
    protected IcebergQueryRunner.Builder configureQueryRunner()
            throws Exception
    {
        JdbcCatalog backend = closeAfterClass(ServerScanPlanningRestCatalogAdapter.buildBackendCatalog(warehouseLocation));
        TestingHttpServer restServer = ServerScanPlanningRestCatalogAdapter.startTestServer(new ServerScanPlanningRestCatalogAdapter(backend));
        closeAfterClass(restServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("fs.hadoop.enabled", "true")
                                .put("iceberg.rest-catalog.uri", restServer.getBaseUrl().toString())
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow());
    }
}
