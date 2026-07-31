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
import io.trino.plugin.iceberg.substitution.AbstractIcebergOnIcebergMvSubstitutionTest;
import io.trino.plugin.lakehouse.LakehouseQueryRunner;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs the Iceberg MV substitution suite against a Lakehouse catalog (default table type Iceberg),
 * exercising Lakehouse's {@link LakehouseSubstitutionMetadata} delegation to the Iceberg connector.
 */
@Execution(SAME_THREAD)
final class TestLakehouseIcebergMvSubstitution
        extends AbstractIcebergOnIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        File metastoreDir = createTempDirectory("lakehouse_mv_substitution").toFile();
        metastoreDir.deleteOnExit();

        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .build();
        try {
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + mvSchema());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName("lakehouse", "mv");
    }

    // The Lakehouse catalog (default Iceberg table type) is its own MV storage and installs a tpch
    // catalog; the tpch source schema is created above.
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
}
