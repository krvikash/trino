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
package io.trino.plugin.hive;

import io.trino.FeaturesConfig;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGpuConnectorSmokeTest
        extends TestHiveConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        checkState(
                !new FeaturesConfig().isGpuAccelerationEnabled(),
                "Otherwise %s would be the GPU test and this class redundant",
                TestHiveConnectorTest.class);

        return HiveQueryRunner.builder()
                .addExtraProperty("gpu-acceleration.enabled", "true")
                .addExtraProperty("gpu-acceleration.table-scan-enabled", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        """
                        CREATE TABLE hive.tpch.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           format = 'PARQUET'
                        )""");
    }
}
