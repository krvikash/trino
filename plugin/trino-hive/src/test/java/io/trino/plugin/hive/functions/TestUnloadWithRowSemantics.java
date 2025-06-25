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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveStorageFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestUnloadWithRowSemantics
        extends BaseUnloadFunctionTest
{
    @Override
    protected Map<String, String> getAdditionalConnectorProperties()
    {
        return ImmutableMap.of("hive.unload.use-row-semantics", "true");
    }

    @Override
    protected int minFilesCreated()
    {
        return getNodeCount() * Runtime.getRuntime().availableProcessors();
    }

    @Test
    @Override
    void testUnloadPartitionLargeResult()
    {
        assertThatThrownBy(super::testUnloadPartitionLargeResult)
                .hasMessageContaining("Invalid argument INPUT. Partitioning specified for table argument with row semantics");
    }

    @Test
    @Override
    void testUnloadNonLowercasePartitionKey()
    {
        assertThatThrownBy(super::testUnloadNonLowercasePartitionKey)
                .hasMessageContaining("Invalid argument INPUT. Partitioning specified for table argument with row semantics");
    }

    @Test
    @Override
    void testUnloadPartition()
    {
        assertThatThrownBy(super::testUnloadPartition)
                .hasMessageContaining("Invalid argument INPUT. Partitioning specified for table argument with row semantics");
    }

    @Test
    @Override
    void testUnloadMultiplePartitionKeys()
    {
        assertThatThrownBy(super::testUnloadMultiplePartitionKeys)
                .hasMessageContaining("Invalid argument INPUT. Partitioning specified for table argument with row semantics");
    }

    @Test
    @Override
    void testUnloadInvalidPartitionArgument()
    {
        assertThatThrownBy(super::testUnloadInvalidPartitionArgument)
                .hasMessageContaining("Invalid argument INPUT. Partitioning specified for table argument with row semantics");
    }

    @Override
    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = { "REGEX", "ESRI" })
    void testUnloadWithSortOrder(HiveStorageFormat format)
    {
        assertThatThrownBy(() -> super.testUnloadWithSortOrder(format))
                .hasMessageContaining("Invalid argument INPUT. Ordering specified for table argument with row semantics");
    }
}
