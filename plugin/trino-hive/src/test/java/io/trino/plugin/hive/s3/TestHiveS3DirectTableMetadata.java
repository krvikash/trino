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
package io.trino.plugin.hive.s3;

import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHiveS3DirectTableMetadata
        extends AbstractTestQueryFramework
{
    private Hive3FlociDataLake hiveFlociDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake("test-hive-direct-meta-" + randomNameSuffix()));
        this.hiveFlociDataLake.start();

        return S3HiveQueryRunner.builder(hiveFlociDataLake)
                .setInitialTables(TpchTable.getTables())
                .build();
    }

    @Test
    void testShouldFailOnNativeGetMetadata()
    {
        // in case of null table_type, get table metadata call fails on HMS side
        // TODO if this test start failing, it means that incorrect behaviour is fixed on HMS side
        //  and we can remove - hive.metastore.thrift.metastore-supports-table-meta toggle
        hiveFlociDataLake.getHiveHadoop().runOnMetastore("UPDATE TBLS SET tbl_type = NULL");
        assertThatThrownBy(() -> computeActual("SHOW TABLES FROM hive.tpch"))
                .hasStackTraceContaining("Error listing tables for catalog hive");
    }
}
