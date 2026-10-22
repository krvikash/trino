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

import io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement;
import io.trino.sql.planner.exploratory.physical.CostComparison;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.ForwardingCostDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCost;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCostDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyProperty;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternative;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeLeafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidate;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.emptyWinners;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.leafOperation;
import static io.trino.sql.planner.exploratory.physical.CostComparison.INCOMPARABLE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ONE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestWinnersTier
{
    @Test
    public void testAddFirstWinner()
    {
        var candidate = candidate(alternative(leafOperation(), ONE));
        var winners = emptyWinners();

        winners.add(candidate);

        assertThat(winners).containsExactly(candidate);
        assertThat(winners.winnerCost()).contains(ONE);
    }

    @Test
    public void testAddExistingWinner()
    {
        var originalCandidate = candidate(alternative(leafOperation(), ONE));
        var winners = emptyWinners();
        winners.add(originalCandidate);

        for (DummyCost cost : List.of(ZERO, ONE, INFINITE)) {
            var duplicateCandidate = candidate(alternative(originalCandidate.alternativeKey().operation(), cost));

            assertThatThrownBy(() -> winners.add(duplicateCandidate))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("winner already exists");
            assertThat(winners).containsExactly(originalCandidate);
            assertThat(winners.winnerCost()).contains(ONE);
        }
    }

    @Test
    public void testAddBetterWinner()
    {
        var worseCandidate = candidate(alternative(leafOperation(), ONE));
        var betterCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinners();

        // add a worse winner, then replace the tier with a better one
        winners.add(worseCandidate);
        winners.add(betterCandidate);

        assertThat(winners).containsExactly(betterCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testIgnoreWorseWinner()
    {
        var betterCandidate = candidate(alternative(leafOperation(), ZERO));
        var worseCandidate = candidate(alternative(alternativeLeafOperation(), ONE));
        var winners = emptyWinners();

        // add a better winner, then ignore a worse one
        winners.add(betterCandidate);
        winners.add(worseCandidate);

        assertThat(winners).containsExactly(betterCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testAddSameTierWinners()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ZERO));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinners();

        // keep multiple winners with the same winner cost
        winners.add(firstCandidate);
        winners.add(secondCandidate);

        assertThat(winners).containsExactly(firstCandidate, secondCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testAddWorseSameTierWinner()
    {
        var betterCandidate = candidate(alternative(leafOperation(), ZERO));
        var worseCandidate = candidate(alternative(alternativeLeafOperation(), ONE));
        var winners = emptyWinnersWithCosts(new TestingCostDomain(true, SameTierRelation.FINITE));

        winners.add(betterCandidate);
        winners.add(worseCandidate);

        assertThat(winners).containsExactly(betterCandidate, worseCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testIgnoreIncomparableWinner()
    {
        var originalCandidate = candidate(alternative(leafOperation(), ONE));
        var incomparableCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinnersWithCosts(new TestingCostDomain(false, SameTierRelation.EXACT));

        winners.add(originalCandidate);
        winners.add(incomparableCandidate);

        assertThat(winners).containsExactly(originalCandidate);
        assertThat(winners.winnerCost()).contains(ONE);
    }

    @Test
    public void testAddIncomparableSameTierWinner()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ONE));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinnersWithCosts(new TestingCostDomain(false, SameTierRelation.FINITE));

        winners.add(firstCandidate);
        winners.add(secondCandidate);

        assertThat(winners).containsExactly(firstCandidate, secondCandidate);
        assertThat(winners.winnerCost()).contains(ONE);
    }

    @Test
    public void testIncreasingCostOrderWithinApproximateTier()
    {
        var worseCandidate = candidate(alternative(leafOperation(), ONE));
        var betterCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinnersWithCosts(new TestingCostDomain(true, SameTierRelation.FINITE));

        winners.add(worseCandidate);
        winners.add(betterCandidate);

        assertThat(winners.inAdmissionOrder()).containsExactly(worseCandidate, betterCandidate);
        assertThat(winners.inIncreasingCostOrder()).containsExactly(betterCandidate, worseCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testUpdateWinner()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ZERO));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var updatedFirstCandidate = candidate(alternative(firstCandidate.alternativeKey().operation(), ONE));
        var winners = emptyWinners();

        // worsen one winner and keep the other best-tier winner
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.update(updatedFirstCandidate);

        assertThat(winners).containsExactly(secondCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testUpdateWinnerToBetterCost()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ONE));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ONE));
        var updatedFirstCandidate = candidate(alternative(firstCandidate.alternativeKey().operation(), ZERO));
        var winners = emptyWinners();

        // improve one winner so it becomes the only best-tier winner
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.update(updatedFirstCandidate);

        assertThat(winners).containsExactly(updatedFirstCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testUpdateWinnerWithNonTransitiveSameTier()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ONE));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var thirdCandidate = candidate(alternative(MemoOperationBuilder.from(leafOperation())
                .withDialect("third_dialect")
                .build(), INFINITE));
        var updatedFirstCandidate = candidate(alternative(firstCandidate.alternativeKey().operation(), ZERO));
        var winners = emptyWinnersWithCosts(new TestingCostDomain(false, SameTierRelation.NON_TRANSITIVE));

        // ZERO ~ ONE and ONE ~ INFINITE, but ZERO !~ INFINITE.
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.add(thirdCandidate);
        assertThat(winners).containsExactly(firstCandidate, secondCandidate, thirdCandidate);

        // Replaying in production order makes ZERO the representative and excludes INFINITE.
        winners.update(updatedFirstCandidate);

        assertThat(winners).containsExactly(updatedFirstCandidate, secondCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    @Test
    public void testUpdateMissingWinner()
    {
        var candidate = candidate(alternative(leafOperation(), ZERO));
        var winners = emptyWinners();

        assertThatThrownBy(() -> winners.update(candidate))
                .hasMessageContaining("winner not found");
    }

    @Test
    public void testIncreasingCostOrderWithinTier()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ZERO));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var winners = emptyWinners();

        // winners with the same cost are returned in stable order
        winners.add(firstCandidate);
        winners.add(secondCandidate);

        assertThat(winners.inIncreasingCostOrder()).containsExactly(firstCandidate, secondCandidate);
    }

    @Test
    public void testUpdateWinnerToInfiniteCost()
    {
        var firstCandidate = candidate(alternative(leafOperation(), ZERO));
        var secondCandidate = candidate(alternative(alternativeLeafOperation(), ZERO));
        var updatedFirstCandidate = candidate(alternative(firstCandidate.alternativeKey().operation(), INFINITE));
        var winners = emptyWinners();

        // an infinite winner is dropped when another finite best-tier winner remains
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.update(updatedFirstCandidate);

        assertThat(winners).containsExactly(secondCandidate);
        assertThat(winners.winnerCost()).contains(ZERO);
    }

    private static WinnersTier<DummyCost, TestRequirement, DummyProperty> emptyWinnersWithCosts(CostDomain<DummyCost> costs)
    {
        return WinnersTier.empty(costs);
    }

    private enum SameTierRelation
    {
        EXACT,
        FINITE,
        NON_TRANSITIVE,
    }

    private record TestingCostDomain(boolean comparableFiniteCosts, SameTierRelation sameTierRelation)
            implements ForwardingCostDomain<DummyCost>
    {
        @Override
        public CostDomain<DummyCost> delegate()
        {
            return DummyCostDomain.INSTANCE;
        }

        @Override
        public CostComparison compare(DummyCost left, DummyCost right)
        {
            if (!comparableFiniteCosts && !left.equals(right) && !left.isInfinite() && !right.isInfinite()) {
                return INCOMPARABLE;
            }
            return delegate().compare(left, right);
        }

        @Override
        public boolean sameTier(DummyCost left, DummyCost right)
        {
            if (left.equals(right)) {
                return true;
            }
            return switch (sameTierRelation) {
                case EXACT -> false;
                case FINITE -> !left.isInfinite() && !right.isInfinite();
                case NON_TRANSITIVE -> left.equals(ONE) || right.equals(ONE);
            };
        }
    }
}
