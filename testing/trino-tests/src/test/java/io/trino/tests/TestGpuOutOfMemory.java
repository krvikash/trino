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

import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.Rmm;
import io.airlift.units.DataSize;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated // The test consumes the entire memory pool to trigger an OOM, so it must run in isolation.
final class TestGpuOutOfMemory
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .configureGpuLocalExecution()
                .build();
    }

    @Test
    void testNativeGpuOutOfMemory()
    {
        try (DeviceMemoryBuffer _ = Rmm.alloc(Rmm.getPoolSize() - DataSize.of(10, MEGABYTE).toBytes())) {
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "SELECT orderkey, count(*), sum(extendedprice), avg(discount) FROM tpch.sf1.lineitem GROUP BY orderkey"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageMatching("Failed to allocate .*\\. GPU memory pool: .* total, .* reserved, .* allocated by RMM\\. This query \\(.*\\): .* reserved; top consumers: .*\\.");
        }
    }
}
