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
package io.trino.sql.planner.exploratory.physical.dummy;

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.OperationId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ONE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

class TestDummyTrinoXOCompanionDialect
{
    private static final OperationId OPERATION_ID = new OperationId("operation", List.of(), List.of());

    @Test
    void testLeafCostsOneOperation()
    {
        assertThat(cost(List.of())).isEqualTo(ONE);
    }

    @Test
    void testCostAddsOneOperationToChildren()
    {
        assertThat(cost(List.of(new DummyCost(2), ZERO, new DummyCost(3)))).isEqualTo(new DummyCost(6));
    }

    @Test
    void testCostIsStrictlyWorseThanEveryChild()
    {
        List<DummyCost> childCosts = List.of(ZERO, ONE, new DummyCost(5));
        DummyCost cost = cost(childCosts);

        for (DummyCost childCost : childCosts) {
            assertThat(DummyCostDomain.INSTANCE.decide(cost, childCost)).isEqualTo(WORSE);
        }
    }

    @Test
    void testInfiniteChildMakesCostInfinite()
    {
        assertThat(cost(List.of(ONE, INFINITE))).isEqualTo(INFINITE);
    }

    private static DummyCost cost(List<DummyCost> childCosts)
    {
        return DummyTrinoXOCompanionDialect.INSTANCE.getCost(
                OPERATION_ID,
                Attributes.empty(),
                Attributes.empty(),
                Collections.nCopies(childCosts.size(), Attributes.empty()),
                Collections.nCopies(childCosts.size(), DummyProperty.UNKNOWN),
                childCosts,
                Optional.empty());
    }
}
