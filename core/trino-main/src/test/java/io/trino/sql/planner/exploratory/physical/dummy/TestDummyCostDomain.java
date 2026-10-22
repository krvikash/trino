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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.exploratory.physical.CostComparison.BETTER;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static io.trino.sql.planner.exploratory.physical.CostDomainAssertions.assertDecisionOrderLaws;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ONE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDummyCostDomain
{
    private static final DummyCostDomain COSTS = DummyCostDomain.INSTANCE;

    @Test
    void testSentinels()
    {
        assertThat(COSTS.zero()).isEqualTo(new DummyCost(0));
        assertThat(COSTS.parameter()).isEqualTo(ZERO);
        assertThat(COSTS.infinite().isInfinite()).isTrue();
        assertThat(ZERO.isInfinite()).isFalse();
    }

    @Test
    void testToString()
    {
        assertThat(ZERO).hasToString("0");
        assertThat(new DummyCost(42)).hasToString("42");
        assertThat(INFINITE).hasToString("INFINITE");
    }

    @Test
    void testRejectsNegativeCost()
    {
        assertThatThrownBy(() -> new DummyCost(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("operations is negative: -1");
    }

    @Test
    void testAddition()
    {
        assertThat(COSTS.add(new DummyCost(2), new DummyCost(3))).isEqualTo(new DummyCost(5));
        assertThat(COSTS.add(new DummyCost(2), INFINITE)).isEqualTo(INFINITE);
        assertThat(COSTS.add(INFINITE, ZERO)).isEqualTo(INFINITE);
        assertThat(COSTS.addAll(List.of())).isEqualTo(ZERO);
        assertThat(COSTS.addAll(List.of(ONE, new DummyCost(2), new DummyCost(3)))).isEqualTo(new DummyCost(6));
        assertThat(COSTS.addAll(List.of(ONE, INFINITE))).isEqualTo(INFINITE);
    }

    @Test
    void testFiniteOverflowFails()
    {
        assertThat(COSTS.add(new DummyCost(Long.MAX_VALUE - 2), ONE)).isEqualTo(new DummyCost(Long.MAX_VALUE - 1));
        assertThatThrownBy(() -> COSTS.add(new DummyCost(Long.MAX_VALUE - 1), ONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("finite cost overflow: 9223372036854775806 + 1");
    }

    @Test
    void testTotalOrder()
    {
        assertThat(COSTS.compare(ZERO, ONE)).isEqualTo(BETTER);
        assertThat(COSTS.compare(new DummyCost(2), ONE)).isEqualTo(WORSE);
        assertThat(COSTS.compare(ONE, ONE)).isEqualTo(EQUAL);
        assertThat(COSTS.compare(ONE, INFINITE)).isEqualTo(BETTER);
        assertThat(COSTS.decide(ONE, INFINITE)).isEqualTo(BETTER);
        assertThat(COSTS.sameTier(ONE, ONE)).isTrue();
        assertThat(COSTS.sameTier(ONE, new DummyCost(2))).isFalse();
    }

    @Test
    void testDecisionOrderLaws()
    {
        assertDecisionOrderLaws(COSTS, List.of(ZERO, ONE, new DummyCost(2), new DummyCost(7), INFINITE));
    }
}
