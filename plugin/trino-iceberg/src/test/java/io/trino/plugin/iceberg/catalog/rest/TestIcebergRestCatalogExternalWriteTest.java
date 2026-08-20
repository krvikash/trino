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
import io.trino.plugin.iceberg.BaseIcebergExternalWriteTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;

final class TestIcebergRestCatalogExternalWriteTest
        extends BaseIcebergExternalWriteTest
{
    private TestingHttpServer testServer;
    private Path warehouseLocation;

    @Override
    protected Map<String, String> getIcebergProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "rest")
                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .put("iceberg.rest-catalog.metastore-cache.ttl", "30m")
                .put("fs.hadoop.enabled", "true")
                .buildOrThrow();
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA %s WITH (location = '%s/%s')".formatted(schemaName, warehouseLocation.toAbsolutePath(), schemaName);
    }

    @Override
    protected DistributedQueryRunner createCluster()
            throws Exception
    {
        if (testServer == null) {
            warehouseLocation = Files.createTempDirectory(null);
            JdbcCatalog backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

            DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                    .delegate(backend)
                    .build();

            testServer = delegatingCatalog.testServer();
            testServer.start();
            closeAfterClass(testServer::stop);
        }

        return IcebergQueryRunner.builder()
                .setIcebergProperties(getIcebergProperties())
                .setBaseDataDir(Optional.of(warehouseLocation))
                .disableSchemaInitializer()
                .build();
    }
}
