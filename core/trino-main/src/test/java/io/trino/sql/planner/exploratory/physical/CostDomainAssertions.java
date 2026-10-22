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
package io.trino.sql.planner.exploratory.physical;

import java.util.List;

import static io.trino.sql.planner.exploratory.physical.CostComparison.BETTER;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static io.trino.sql.planner.exploratory.physical.CostComparison.INCOMPARABLE;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the {@link CostDomain#decide(Cost, Cost)} laws over every pair and
 * triple of the given sample costs.
 */
public final class CostDomainAssertions
{
    private CostDomainAssertions() {}

    public static <C extends Cost> void assertDecisionOrderLaws(CostDomain<C> costs, List<C> samples)
    {
        for (C left : samples) {
            for (C right : samples) {
                CostComparison decision = costs.decide(left, right);
                String pair = "decide(%s, %s)".formatted(left, right);

                assertThat(decision).as("totality of " + pair).isNotEqualTo(INCOMPARABLE);
                assertThat(costs.decide(right, left)).as("reversal of " + pair).isEqualTo(reversed(decision));

                for (C third : samples) {
                    if (decision != WORSE && costs.decide(right, third) != WORSE) {
                        assertThat(costs.decide(left, third)).as("transitivity of %s, %s, %s", left, right, third).isNotEqualTo(WORSE);
                    }
                }

                CostComparison comparison = costs.compare(left, right);
                if (comparison == BETTER || comparison == WORSE) {
                    assertThat(decision).as("extension of compare for " + pair).isEqualTo(comparison);
                }
                assertThat(decision == EQUAL).as("equality consistent with compare for " + pair).isEqualTo(comparison == EQUAL);

                for (C third : samples) {
                    if (decision != WORSE) {
                        assertThat(costs.decide(costs.add(left, third), costs.add(right, third)))
                                .as("compatibility of %s with addition of %s", pair, third)
                                .isNotEqualTo(WORSE);
                    }
                }
            }
        }

        for (C cost : samples) {
            if (costs.compare(cost, costs.infinite()) != EQUAL) {
                assertThat(costs.decide(cost, costs.infinite())).as("infinity last for " + cost).isEqualTo(BETTER);
            }
        }
    }

    private static CostComparison reversed(CostComparison comparison)
    {
        return switch (comparison) {
            case BETTER -> WORSE;
            case WORSE -> BETTER;
            case EQUAL -> EQUAL;
            case INCOMPARABLE -> INCOMPARABLE;
        };
    }
}
