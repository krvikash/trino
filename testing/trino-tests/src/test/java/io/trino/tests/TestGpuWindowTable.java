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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.tests.GpuWindowTable.TIES;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuWindowTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder().build();
    }

    @Test
    void testRowsAreStoredInRandomOrder()
    {
        TIES.create(getQueryRunner());
        List<Long> rowIds = computeActual("SELECT row_id FROM " + TIES.withOrderKey(BIGINT)).getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
        assertThat(rowIds).isNotEqualTo(rowIds.stream().sorted().toList());
        // Partitions are interleaved from the start
        assertThat(rowIds.subList(0, 100).stream().map(rowId -> rowId / 1_000_000).distinct().count()).isGreaterThan(1);
    }
}
