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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergCachingHiveCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private HiveMetastore metastore;

    public TestIcebergCachingHiveCatalogConnectorSmokeTest()
    {
        super(PARQUET);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION, ORDERS, REGION)
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.file-format", format.name(),
                        "iceberg.register-table-procedure.enabled", "true",
                        "iceberg.writer-sort-buffer-size", "1MB",
                        "iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max",
                        "hive.metastore-cache-ttl", "10m"))
                .build();
        metastore = getHiveMetastore(queryRunner);
        return queryRunner;
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        metastore.dropTable(getSession().getSchema().orElseThrow(), tableName, false);
        assertThat(metastore.getTable(getSession().getSchema().orElseThrow(), tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return metastore
                .getTable(getSession().getSchema().orElseThrow(), tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return "local:///%s".formatted(getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }
}
