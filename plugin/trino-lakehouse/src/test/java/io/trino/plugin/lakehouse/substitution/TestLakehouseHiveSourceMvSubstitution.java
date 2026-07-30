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
package io.trino.plugin.lakehouse.substitution;

import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.lakehouse.LakehouseQueryRunner;
import io.trino.plugin.lakehouse.TableType;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.io.Closeable;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs the MV substitution suite against a Lakehouse catalog whose default table type is Hive and
 * whose materialization storage is Iceberg, exercising {@link LakehouseSubstitutionMetadata} routing
 * of Hive handles to the Hive delegate. The DELETE-based staleness tests are skipped because the
 * catalog does not support Hive ACID merge on a source table (see {@link #sourceSupportsRowLevelDelete()}).
 */
@Execution(SAME_THREAD)
final class TestLakehouseHiveSourceMvSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        Path metastoreDir = createTempDirectory("lakehouse_hive_mv_substitution");
        closeAfterClass((Closeable) () -> deleteRecursively(metastoreDir, ALLOW_INSECURE));

        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addLakehouseProperty("lakehouse.table-type", TableType.HIVE.name())
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", metastoreDir.toFile().toURI().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .build();

        queryRunner.execute("CREATE SCHEMA %s".formatted(mvSchema()));
        return queryRunner;
    }

    // The Lakehouse catalog is its own Iceberg-backed MV storage (no separate iceberg catalog),
    // provisions a tpch catalog, and its tpch source schema is created above.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("lakehouse", "tpch");
    }

    @Override
    protected boolean addIcebergConnector()
    {
        return false;
    }

    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return true;
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName("lakehouse", "mv");
    }

    @Override
    protected boolean sourceSupportsRowLevelDelete()
    {
        // Lakehouse does not support Hive ACID merge, so it cannot DELETE individual rows from a
        // Hive source table; the staleness tests that rely on it are skipped.
        return false;
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }
}
