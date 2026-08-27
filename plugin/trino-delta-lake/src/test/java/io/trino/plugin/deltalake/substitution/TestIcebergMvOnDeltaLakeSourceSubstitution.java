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
package io.trino.plugin.deltalake.substitution;

import io.trino.Session;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;

public class TestIcebergMvOnDeltaLakeSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        // The Delta Lake catalog (default session catalog "delta", schema "tpch") is the source; the
        // Iceberg catalog added by the base class holds the MV storage tables. The query runner
        // already provides the tpch catalog and the source schema.
        return DeltaLakeQueryRunner.builder(sourceSchema.getSchemaName())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName(DELTA_CATALOG, TPCH_SCHEMA);
    }

    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }

    @Test
    public void testMetadataColumnProjection()
    {
        CatalogSchemaTableName mvName = mvName("mv_path_");
        try {
            createSubstitutionMv(mvName, "SELECT \"$path\" FROM " + ordersTable);

            assertSubstituted("SELECT \"$path\" FROM " + ordersTable, ordersTable, mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }
}
