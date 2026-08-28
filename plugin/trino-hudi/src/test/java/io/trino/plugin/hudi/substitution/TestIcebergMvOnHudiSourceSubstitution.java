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
package io.trino.plugin.hudi.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hudi.HudiQueryRunner;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;

/**
 * Runs the read-only MV substitution suite with Hudi as the source. Trino cannot create, populate or
 * drop Hudi tables, so the suite only builds materialized views over the tables the query runner
 * loads through the Hudi Java client.
 */
public class TestIcebergMvOnHudiSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addConnectorProperty("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                .setDataLoader(new TpchHudiTablesInitializer(ImmutableList.of(TpchTable.ORDERS, TpchTable.CUSTOMER)))
                .build();
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName("hudi", "tests", "orders");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName("hudi", "tests", "customer");
    }

    @Override
    protected String materializedColumn()
    {
        return "orderkey";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "orderstatus";
    }
}
