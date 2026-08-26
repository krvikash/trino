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

import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * The test lives in the iceberg module (not trino-tpcds) because trino-iceberg/hive test-depend on
 * trino-tpcds — adding them as test dependencies of trino-tpcds would form a Maven reactor cycle.
 */
public class TestIcebergMvOnTpcdsSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    private static final String SCHEMA = "\"sf0.01\"";

    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("tpcds")
                        .setSchema(SCHEMA)
                        .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.createCatalog("tpcds", "tpcds");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName("tpcds", SCHEMA, "customer");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName("tpcds", SCHEMA, "item");
    }

    @Override
    protected String materializedColumn()
    {
        return "c_customer_sk";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "c_current_cdemo_sk";
    }
}
