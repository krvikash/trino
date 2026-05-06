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
package io.trino.tests;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestOrderByQueries;
import io.trino.testing.QueryRunner;

public class TestGpuOrderByQueries
        extends AbstractTestOrderByQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addExtraProperty("gpu-execution", "true")
                .addExtraProperty("task.gpu-execution.enabled", "true")
                .addWorkerProperty("gpu.memory.pool-size", "4GB")
                // GPU is disabled on coordinator unless include-coordinator is set. Disable include-coordinator to force coordinator into more production-like setup.
                // This is needed to expose potential problems where operators on workers and coordinator do not match.
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .build();
    }
}
