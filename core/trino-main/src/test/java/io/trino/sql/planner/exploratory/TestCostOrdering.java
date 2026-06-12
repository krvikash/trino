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
package io.trino.sql.planner.exploratory;

import io.trino.sql.planner.exploratory.physical.CostComparison;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.exploratory.CostOrdering.stableLinearExtension;
import static io.trino.sql.planner.exploratory.physical.CostComparison.BETTER;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static io.trino.sql.planner.exploratory.physical.CostComparison.INCOMPARABLE;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCostOrdering
{
    @Test
    void testEmpty()
    {
        assertThat(stableLinearExtension(List.of(), (_, _) -> EQUAL)).isEmpty();
    }

    @Test
    void testTotalOrder()
    {
        assertThat(stableLinearExtension(List.of(3, 1, 2), TestCostOrdering::compareIntegers))
                .containsExactly(1, 2, 3);
    }

    @Test
    void testStableForEqualElements()
    {
        assertThat(stableLinearExtension(List.of("C", "A", "B"), (_, _) -> EQUAL))
                .containsExactly("C", "A", "B");
    }

    @Test
    void testStableForIncomparableElements()
    {
        assertThat(stableLinearExtension(List.of("C", "A", "B"), (_, _) -> INCOMPARABLE))
                .containsExactly("C", "A", "B");
    }

    @Test
    void testPartialOrder()
    {
        assertThat(stableLinearExtension(
                List.of(Element.D, Element.B, Element.C, Element.A),
                TestCostOrdering::comparePartialOrder))
                .containsExactly(Element.B, Element.C, Element.A, Element.D);
    }

    @Test
    void testRejectCycle()
    {
        assertThatThrownBy(() -> stableLinearExtension(
                List.of(Element.A, Element.B, Element.C),
                TestCostOrdering::compareCyclically))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cost comparison contains a cycle");
    }

    private static CostComparison compareIntegers(int left, int right)
    {
        if (left < right) {
            return BETTER;
        }
        if (left > right) {
            return WORSE;
        }
        return EQUAL;
    }

    private static CostComparison comparePartialOrder(Element left, Element right)
    {
        if (left == right) {
            return EQUAL;
        }
        if ((left == Element.A || left == Element.C) && right == Element.D) {
            return BETTER;
        }
        if (left == Element.D && (right == Element.A || right == Element.C)) {
            return WORSE;
        }
        return INCOMPARABLE;
    }

    private static CostComparison compareCyclically(Element left, Element right)
    {
        if (left == right) {
            return EQUAL;
        }
        if ((left == Element.A && right == Element.B) ||
                (left == Element.B && right == Element.C) ||
                (left == Element.C && right == Element.A)) {
            return BETTER;
        }
        return WORSE;
    }

    private enum Element
    {
        A,
        B,
        C,
        D,
    }
}
