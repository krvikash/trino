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

import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;

final class TestHiveS3DirectTableMetadataOldPath
        extends AbstractTestQueryFramework
{
    private Hive3MinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake("test-hive-direct-meta-" + randomNameSuffix()));
        this.hiveMinioDataLake.start();

        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .setInitialTables(TpchTable.getTables())
                // turn on old path of getting table metadata without direct call
                .setThriftMetastoreConfig(new ThriftMetastoreConfig().setMetastoreSupportsTableMeta(false))
                .build();
    }

    @Test
    void testShouldNotFailOnOldPathGetMetadata()
    {
        // in case of null table_type, direct get table metadata call fails on HMS side
        // but old path with hive.metastore.thrift.metastore-supports-table-meta - false, should work
        hiveMinioDataLake.getHiveHadoop().runOnMetastore("UPDATE TBLS SET tbl_type = NULL");
        computeActual("SHOW TABLES FROM hive.tpch");
    }
}
