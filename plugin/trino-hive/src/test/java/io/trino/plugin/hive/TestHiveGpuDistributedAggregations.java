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
import io.trino.testing.BaseGpuDistributedAggregationsTest;
import io.trino.testing.QueryRunner;

import static com.google.common.base.Preconditions.checkState;

public class TestHiveGpuDistributedAggregations
        extends BaseGpuDistributedAggregationsTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        checkState(
                !new FeaturesConfig().isGpuExecution(),
                "Otherwise %s would be the GPU test and this class redundant",
                TestHiveConnectorTest.class);

        return HiveQueryRunner.builder()
                .configureGpuDistributedExecution()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .build();
    }
}
