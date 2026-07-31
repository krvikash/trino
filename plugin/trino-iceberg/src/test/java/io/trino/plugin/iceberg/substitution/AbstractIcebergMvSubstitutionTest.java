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
package io.trino.plugin.iceberg.substitution;

import io.trino.Session;
import io.trino.plugin.hive.substitution.AbstractMvSubstitutionTest;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;

public abstract class AbstractIcebergMvSubstitutionTest
        extends AbstractMvSubstitutionTest
{
    @Override
    protected String partitionedByPropertyName()
    {
        return "partitioning";
    }

    protected abstract QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception;

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CatalogSchemaName sourceSchema = sourceSchema();
        QueryRunner queryRunner = createSourceQueryRunner(testSessionBuilder()
                        .setCatalog(sourceSchema.getCatalogName())
                        .setSchema(sourceSchema.getSchemaName())
                        .build(),
                sourceSchema);
        try {
            if (addIcebergConnector()) {
                Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
                queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
                queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                        "iceberg.hive-catalog-name", "hive",
                        // v3 + variant so MV storage can hold a Trino json column materialized from a
                        // JSON source column (exercised by testScanWithSubFieldProjectionOverMv).
                        "iceberg.format-version", "3",
                        "iceberg.legacy-variant-type-mapping", "JSON"));
                queryRunner.execute("CREATE SCHEMA %s.tpch".formatted(ICEBERG_CATALOG));
            }
            if (addTpchConnector()) {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
            }

            if (createSourceSchema()) {
                queryRunner.execute("CREATE SCHEMA " + sourceSchema);
            }
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    protected boolean addIcebergConnector()
    {
        return true;
    }

    protected boolean addTpchConnector()
    {
        return true;
    }

    protected boolean createSourceSchema()
    {
        return true;
    }
}
