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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static io.starburst.materialization.ir.IrFixtures.tableScan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOutput
{
    private static final Symbol NAME = new Symbol(VARCHAR, "name");
    private static final TableScan SCAN = tableScan(NAME);

    @Test
    void testAcceptsRepeatedOutput()
    {
        Output output = new Output(ImmutableList.of("a", "b"), ImmutableList.of(NAME, NAME), SCAN);

        assertThat(output.outputs()).containsExactly(NAME, NAME);
    }

    @Test
    void testRejectsOutputNotProducedBySource()
    {
        assertThatThrownBy(() -> new Output(ImmutableList.of("missing"), ImmutableList.of(new Symbol(BIGINT, "missing")), SCAN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Output: output is not an output of the source");
    }
}
