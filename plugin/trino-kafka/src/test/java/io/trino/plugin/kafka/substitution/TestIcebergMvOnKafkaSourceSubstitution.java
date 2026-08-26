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
package io.trino.plugin.kafka.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.plugin.kafka.KafkaQueryRunner;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;

public class TestIcebergMvOnKafkaSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.create());
        kafka.start();
        return KafkaQueryRunner.builder(kafka)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .setTables(ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION))
                .build();
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName("kafka", "tpch", "orders");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName("kafka", "tpch", "nation");
    }

    @Override
    protected String materializedColumn()
    {
        return "orderkey";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "custkey";
    }
}
