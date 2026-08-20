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
package io.trino.plugin.iceberg.profiler.rules;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergInputInfo;
import io.trino.spi.profiler.ConnectorQueryProfilerFinding;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergOverPartitionedTable
{
    @Test
    public void testFlagsTableWithManyPartitionsAndTinyFiles()
    {
        IcebergInputInfo info = new IcebergInputInfo(
                2,
                OptionalLong.empty(),
                ImmutableList.of("a", "b", "c"),
                "PARQUET",
                Optional.of("100"),
                Optional.empty(),
                Optional.of("100"),
                Optional.of("0"),
                Optional.empty(),
                Optional.empty());

        List<ConnectorQueryProfilerFinding> findings = new IcebergOverPartitionedTable().analyze(info);

        assertThat(findings).hasSize(1);
        assertThat(findings.getFirst().description()).contains("over-partitioned");
    }

    @Test
    public void testDoesNotFlagTableWithFewPartitionFields()
    {
        IcebergInputInfo info = new IcebergInputInfo(
                2,
                OptionalLong.empty(),
                ImmutableList.of("a"),
                "PARQUET",
                Optional.of("100"),
                Optional.empty(),
                Optional.of("100"),
                Optional.of("0"),
                Optional.empty(),
                Optional.empty());

        List<ConnectorQueryProfilerFinding> findings = new IcebergOverPartitionedTable().analyze(info);

        assertThat(findings).isEmpty();
    }
}
