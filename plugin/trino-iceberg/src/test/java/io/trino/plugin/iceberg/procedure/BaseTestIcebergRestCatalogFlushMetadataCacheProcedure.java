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
package io.trino.plugin.iceberg.procedure;

import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;

abstract class BaseTestIcebergRestCatalogFlushMetadataCacheProcedure
        extends BaseTestIcebergFlushMetadataCacheProcedure
{
    @TempDir
    private static Path warehouseLocation;
    private final boolean caseInsensitiveNameMatching;
    private JdbcCatalog backend;

    protected BaseTestIcebergRestCatalogFlushMetadataCacheProcedure(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.metastore-cache.ttl", "10m")
                .addIcebergProperty("iceberg.rest-catalog.case-insensitive-name-matching", Boolean.toString(caseInsensitiveNameMatching))
                .disableSchemaInitializer()
                .build();
    }

    @Override
    protected String getSchemaLocation()
    {
        return warehouseLocation.toAbsolutePath().toString();
    }

    @Override
    protected void renameTableOutsideTrino(String schemaName, String sourceTableName, String targetTableName)
    {
        backend.renameTable(
                TableIdentifier.of(Namespace.of(schemaName), sourceTableName),
                TableIdentifier.of(Namespace.of(schemaName), targetTableName));
    }
}
