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

import org.junit.jupiter.api.Test;

import static io.starburst.materialization.ir.IrFixtures.tableScan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestTableScan
{
    private static final Symbol NAME = new Symbol(VARCHAR, "name");
    private static final Symbol VALUE = new Symbol(BIGINT, "value");
    private static final Symbol FLAG = new Symbol(BOOLEAN, "flag");
    private static final TableScan SCAN = tableScan(NAME, VALUE, FLAG);

    @Test
    void testOutputsAreAssignmentsInOrder()
    {
        assertThat(SCAN.outputs()).containsExactly(NAME, VALUE, FLAG);
    }
}
