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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.starburst.materialization.ir.IrFixtures.outputOverScan;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

class TestIrVersions
{
    @Test
    void testOperationTreeStampsBareNames()
    {
        assertThat(IrVersions.collect(outputOverScan(new Symbol(BIGINT, "a"))))
                .isEqualTo(ImmutableMap.of("Output", 1, "TableScan", 1));
    }

    @Test
    void testOperationSupportedIsBareNames()
    {
        assertThat(Operation.SUPPORTED).isEqualTo(ImmutableMap.of("Output", 1, "TableScan", 1));
        assertThat(IrVersions.SUPPORTED).containsAllEntriesOf(Operation.SUPPORTED);
    }
}
