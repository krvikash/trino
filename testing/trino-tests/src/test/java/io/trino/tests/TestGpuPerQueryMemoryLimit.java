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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestGpuPerQueryMemoryLimit
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .configureGpuLocalExecution()
                .addExtraProperty("query.max-gpu-memory-per-node", "8MB")
                .build();
    }

    @Test
    void testPerQueryMemoryLimit()
    {
        assertThatThrownBy(() -> getQueryRunner().execute(
                "SELECT count(*), sum(totalprice) FROM tpch.sf1.orders GROUP BY custkey"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageMatching("Query exceeded per-node GPU memory limit of 8MB \\[Allocated: .*, Delta: .*, Top Consumers: \\{.*}]");
    }
}
