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
package io.trino.plugin.iceberg;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.max-format-version", "3")
                .build();

        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);

        return queryRunner;
    }

    @Test
    void testUnsupportedTableEncryption()
    {
        String tableName = "test_encryption" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int)");
        try {
            BaseTable icebergTable = loadTable(tableName);
            icebergTable.updateProperties().set(ENCRYPTION_TABLE_KEY, "test_key").commit();

            assertQueryFails("SELECT * FROM " + tableName, "Table encryption is not supported for: .*");
        }
        finally {
            metastore.dropTable("tpch", tableName, true);
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
