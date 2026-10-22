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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.newir.Dialect;
import io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement;
import io.trino.sql.planner.exploratory.physical.CostComparison;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.ForwardingCostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCost;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCostDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyProperty;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.PHYSICAL_MODEL;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.OTHER;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.UNDEFINED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternative;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.alternativeLeafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidate;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.candidateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.emptyWinners;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.leafOperation;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.state;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.stateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.winners;
import static io.trino.sql.planner.exploratory.physical.CostComparison.BETTER;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static io.trino.sql.planner.exploratory.physical.CostComparison.INCOMPARABLE;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ONE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestGroupOptimizationState
{
    private static final DummyCost COST_2 = new DummyCost(2);
    private static final DummyCost COST_3 = new DummyCost(3);

    @Test
    public void testInitialize()
    {
        var stateKey = stateKey(1, UNDEFINED);

        var state = state(stateKey);

        assertThat(state.groupId()).isEqualTo(1);
        assertThat(state.requirement()).isEqualTo(UNDEFINED);
        assertThat(state.key()).isEqualTo(stateKey);
        assertThat(state.winners()).isEmpty();
        assertThat(state.winnerCost()).isEmpty();
        assertThat(state.bestKnownCost()).isEmpty();
        assertThat(state.shouldNotifyParents()).isFalse();
    }

    @Test
    public void testWithMethods()
    {
        var state = state(stateKey(1, UNDEFINED));
        MemoOperation operation = leafOperation();
        var candidate = candidate(alternative(operation));
        var winners = emptyWinners();
        winners.add(candidate);

        // update all fields through with-methods
        var updated = state
                .withGroupId(2)
                .replaceWinners(winners);

        // transformed state reflects all requested changes
        assertThat(updated.groupId()).isEqualTo(2);
        assertThat(updated.requirement()).isEqualTo(UNDEFINED);
        assertThat(updated.key()).isEqualTo(stateKey(2, UNDEFINED));
        assertThat(updated.winners()).containsExactly(candidate);
        assertThat(updated.winnerCost()).contains(ONE);
        assertThat(updated.bestKnownCost()).contains(ONE);

        // winners are defensively copied into the state
        winners.add(candidate(alternative(alternativeLeafOperation())));
        assertThat(updated.winners()).containsExactly(candidate);

        // the original state stays unchanged
        assertThat(state.groupId()).isEqualTo(1);
        assertThat(state.winners()).isEmpty();
        assertThat(state.bestKnownCost()).isEmpty();
    }

    @Test
    public void testMergeStates()
    {
        MemoOperation survivingOperation = leafOperation();
        MemoOperation removedOperation = alternativeLeafOperation();
        var survivingCandidate = candidate(alternative(survivingOperation));
        var removedCandidate = candidate(alternative(removedOperation));

        var survivingState = state(stateKey(1, UNDEFINED))
                .replaceWinners(winners(survivingCandidate));
        var removedState = state(stateKey(2, UNDEFINED))
                .replaceWinners(winners(removedCandidate));

        // merge two states with the same requirement
        var mergedState = GroupOptimizationState.mergeStates(survivingState, removedState);

        // merged state keeps the surviving identity and combines winners
        assertThat(mergedState.groupId()).isEqualTo(1);
        assertThat(mergedState.requirement()).isEqualTo(UNDEFINED);
        assertThat(mergedState.winners()).containsExactlyInAnyOrder(survivingCandidate, removedCandidate);
        assertThat(mergedState.winnerCost()).contains(ONE);
        assertThat(mergedState.bestKnownCost()).contains(ONE);

        // source states stay unchanged
        assertThat(survivingState.winners()).containsExactly(survivingCandidate);
        assertThat(removedState.winners()).containsExactly(removedCandidate);
    }

    @Test
    public void testMergeStatesPreservesAdmissionOrder()
    {
        var survivingCandidate = candidateWithCost(leafOperation(), ONE);
        var firstRemovedCandidate = candidateWithCost(alternativeLeafOperation(), COST_2);
        var secondRemovedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("second_removed")
                        .build(),
                ZERO);
        var thirdRemovedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("third_removed")
                        .build(),
                COST_3);

        WinnersTier<DummyCost, TestRequirement, DummyProperty> survivingWinners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        survivingWinners.add(survivingCandidate);
        WinnersTier<DummyCost, TestRequirement, DummyProperty> removedWinners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        removedWinners.add(firstRemovedCandidate);
        removedWinners.add(secondRemovedCandidate);
        removedWinners.add(thirdRemovedCandidate);
        assertThat(removedWinners.inAdmissionOrder()).containsExactly(firstRemovedCandidate, secondRemovedCandidate, thirdRemovedCandidate);
        assertThat(removedWinners.inIncreasingCostOrder()).containsExactly(secondRemovedCandidate, firstRemovedCandidate, thirdRemovedCandidate);

        var survivingState = GroupOptimizationState.<DummyCost, TestRequirement, DummyProperty>initialize(
                        stateKey(1, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(survivingWinners);
        var removedState = GroupOptimizationState.<DummyCost, TestRequirement, DummyProperty>initialize(
                        stateKey(2, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(removedWinners);

        var mergedState = GroupOptimizationState.mergeStates(survivingState, removedState);

        // The first removed candidate is not admitted relative to the surviving representative.
        // Replaying the second candidate first would admit the first one afterward.
        assertThat(mergedState.winners().inAdmissionOrder())
                .containsExactly(survivingCandidate, secondRemovedCandidate, thirdRemovedCandidate);
        assertThat(mergedState.winnerCost()).contains(ZERO);
        assertThat(mergedState.bestKnownCost()).contains(ZERO);
    }

    @Test
    public void testMergeStatesWithMismatchedRequirements()
    {
        var survivingState = state(stateKey(1, UNDEFINED));
        var removedState = state(stateKey(2, OTHER));

        assertThatThrownBy(() -> GroupOptimizationState.mergeStates(survivingState, removedState))
                .hasMessageContaining("requirements do not match");
    }

    @Test
    public void testRemoveCandidates()
    {
        MemoOperation firstOperation = leafOperation();
        MemoOperation secondOperation = alternativeLeafOperation();
        var firstCandidate = candidate(alternative(firstOperation));
        var secondCandidate = candidate(alternative(secondOperation));

        var winners = emptyWinners();
        winners.add(firstCandidate);
        winners.add(secondCandidate);

        var state = state(stateKey(1, UNDEFINED))
                .replaceWinners(winners);

        // remove only the candidate whose key matches this state's requirement
        var updated = state.removeCandidates(ImmutableSet.of(
                candidateKey(firstCandidate.alternativeKey(), UNDEFINED),
                candidateKey(secondCandidate.alternativeKey(), OTHER)));

        // only the matching candidate is removed
        assertThat(updated.winners()).containsExactly(secondCandidate);
        assertThat(updated.winnerCost()).contains(ONE);
        assertThat(updated.bestKnownCost()).contains(ONE);

        // the original state stays unchanged
        assertThat(state.winners()).containsExactlyInAnyOrder(firstCandidate, secondCandidate);
    }

    @Test
    public void testRemoveCandidatesPreservesAdmissionOrder()
    {
        var firstCandidate = candidateWithCost(leafOperation(), ONE);
        var secondCandidate = candidateWithCost(alternativeLeafOperation(), COST_3);
        var thirdCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("third")
                        .build(),
                ZERO);
        var fourthCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("fourth")
                        .build(),
                COST_2);
        WinnersTier<DummyCost, TestRequirement, DummyProperty> winners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.add(thirdCandidate);
        winners.add(fourthCandidate);
        assertThat(winners.inAdmissionOrder()).containsExactly(firstCandidate, secondCandidate, thirdCandidate, fourthCandidate);
        assertThat(winners.inIncreasingCostOrder()).containsExactly(secondCandidate, thirdCandidate, firstCandidate, fourthCandidate);

        var state = GroupOptimizationState.<DummyCost, TestRequirement, DummyProperty>initialize(
                        stateKey(1, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(winners);

        var updated = state.removeCandidates(ImmutableSet.of());

        assertThat(updated).isSameAs(state);
        assertThat(updated.winners().inAdmissionOrder())
                .containsExactly(firstCandidate, secondCandidate, thirdCandidate, fourthCandidate);
        assertThat(updated.winnerCost()).contains(ZERO);
        assertThat(updated.bestKnownCost()).contains(ZERO);

        assertThat(state.removeCandidates(ImmutableSet.of(candidateKey(firstCandidate.alternativeKey(), OTHER))))
                .isSameAs(state);

        var unrelatedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("unrelated")
                        .build(),
                ZERO);
        assertThat(state.removeCandidates(ImmutableSet.of(candidateKey(unrelatedCandidate.alternativeKey(), UNDEFINED))))
                .isSameAs(state);
    }

    @Test
    public void testAddCandidate()
    {
        var firstCandidate = candidate(alternative(leafOperation()));
        var secondCandidate = candidate(alternative(alternativeLeafOperation()));

        // start with one winner and add another candidate
        var state = state(stateKey(1, UNDEFINED))
                .addCandidate(firstCandidate);
        var updated = state.addCandidate(secondCandidate);

        // the returned state contains both candidates
        assertThat(state.winners()).containsExactly(firstCandidate);
        assertThat(updated.winners()).containsExactlyInAnyOrder(firstCandidate, secondCandidate);
        assertThat(updated.winnerCost()).contains(ONE);
        assertThat(updated.bestKnownCost()).contains(ONE);
    }

    @Test
    public void testBestKnownCostPresenceNotifiesParents()
    {
        var candidate = candidate(alternative(leafOperation()));
        var state = state(stateKey(1, UNDEFINED))
                .addCandidate(candidate);

        assertThat(state.bestKnownCost()).contains(ONE);
        assertThat(state.shouldNotifyParents()).isTrue();

        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.removeCandidates(ImmutableSet.of(candidateKey(candidate.alternativeKey(), UNDEFINED)));
        assertThat(state.winnerCost()).isEmpty();
        assertThat(state.bestKnownCost()).contains(ONE);
        assertThat(state.shouldNotifyParents()).isFalse();
    }

    @Test
    public void testParentNotificationTierDoesNotDrift()
    {
        var state = stateWithWinnerCost(COST_3);

        assertThat(state.shouldNotifyParents()).isTrue();
        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.replaceWinners(winnersWithCost(COST_2));

        // Costs 3 and 2 are in the same tier, so parent notification is suppressed.
        assertThat(state.bestKnownCost()).contains(COST_2);
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.replaceWinners(winnersWithCost(ONE));

        // Costs 2 and 1 are in the same tier, but costs 3 and 1 are not.
        // The notification anchor remains 3, so parents must now be notified.
        assertThat(TestingCostDomain.INSTANCE.sameTier(ONE, COST_2)).isTrue();
        assertThat(TestingCostDomain.INSTANCE.sameTier(COST_3, ONE)).isFalse();
        assertThat(state.bestKnownCost()).contains(ONE);
        assertThat(state.shouldNotifyParents()).isTrue();

        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();
    }

    @Test
    public void testMergeStatesPreservesSurvivingParentNotificationCost()
    {
        var survivingState = stateWithWinnerCost(ZERO)
                .withParentsNotified();

        var removedWinners = WinnersTier.<DummyCost, TestRequirement, DummyProperty>empty(TestingCostDomain.INSTANCE);
        removedWinners.add(candidateWithCost(alternativeLeafOperation(), COST_2));
        var removedState = GroupOptimizationState.<DummyCost, TestRequirement, DummyProperty>initialize(
                        stateKey(2, UNDEFINED),
                        TestingCostDomain.INSTANCE)
                .replaceWinners(removedWinners)
                .withParentsNotified();

        var mergedState = GroupOptimizationState.mergeStates(survivingState, removedState);

        assertThat(mergedState.winnerCost()).contains(ZERO);
        assertThat(mergedState.shouldNotifyParents()).isFalse();
    }

    private static GroupOptimizationState<DummyCost, TestRequirement, DummyProperty> stateWithWinnerCost(DummyCost cost)
    {
        return GroupOptimizationState.<DummyCost, TestRequirement, DummyProperty>initialize(stateKey(1, UNDEFINED), TestingCostDomain.INSTANCE)
                .replaceWinners(winnersWithCost(cost));
    }

    private static WinnersTier<DummyCost, TestRequirement, DummyProperty> winnersWithCost(DummyCost cost)
    {
        WinnersTier<DummyCost, TestRequirement, DummyProperty> winners = WinnersTier.empty(TestingCostDomain.INSTANCE);
        winners.add(candidateWithCost(leafOperation(), cost));
        return winners;
    }

    private static Candidate<DummyCost, TestRequirement, DummyProperty> candidateWithCost(MemoOperation operation, DummyCost cost)
    {
        OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternative = OperationImplementationAlternative.alternative(
                operation,
                List.of(),
                List.of(),
                List.of(),
                DummyProperty.UNKNOWN,
                cost,
                TestingPhysicalModel.INSTANCE.requirements(),
                TestingPhysicalModel.INSTANCE.properties());
        Candidate<DummyCost, TestRequirement, DummyProperty> candidate = new Candidate<>(
                alternative,
                EnforcerPipeline.empty(
                        operation.resultType(),
                        operation.attributes(),
                        alternative.providedProperty(),
                        TESTING_DIALECT_REGISTRY,
                        TestingPhysicalModel.INSTANCE),
                TestingPhysicalModel.INSTANCE.costs());
        return candidate;
    }

    private enum TestingCostDomain
            implements ForwardingCostDomain<DummyCost>
    {
        INSTANCE;

        private static final long TIER_TOLERANCE = 1;

        @Override
        public CostDomain<DummyCost> delegate()
        {
            return DummyCostDomain.INSTANCE;
        }

        @Override
        public boolean sameTier(DummyCost left, DummyCost right)
        {
            if (left.equals(right)) {
                return true;
            }
            return !left.isInfinite() && !right.isInfinite() && Math.abs(left.operations() - right.operations()) <= TIER_TOLERANCE;
        }
    }

    /**
     * A lawful partial cost order and a non-transitive tier relation chosen to
     * make admission order observably different from increasing cost order.
     */
    private enum ReplayCostDomain
            implements ForwardingCostDomain<DummyCost>
    {
        INSTANCE;

        @Override
        public CostDomain<DummyCost> delegate()
        {
            return DummyCostDomain.INSTANCE;
        }

        @Override
        public CostComparison compare(DummyCost left, DummyCost right)
        {
            if (left.equals(right)) {
                return EQUAL;
            }
            if (left.equals(INFINITE)) {
                return WORSE;
            }
            if (right.equals(INFINITE)) {
                return BETTER;
            }
            if (left.equals(ZERO) && (right.equals(ONE) || right.equals(COST_2))) {
                return BETTER;
            }
            if (right.equals(ZERO) && (left.equals(ONE) || left.equals(COST_2))) {
                return WORSE;
            }
            return INCOMPARABLE;
        }

        @Override
        public boolean sameTier(DummyCost left, DummyCost right)
        {
            return left.equals(right) ||
                    (left.equals(ZERO) && !right.equals(INFINITE)) ||
                    (right.equals(ZERO) && !left.equals(INFINITE)) ||
                    (left.equals(ONE) && right.equals(COST_3)) ||
                    (left.equals(COST_3) && right.equals(ONE));
        }
    }

    private enum TestingPhysicalModel
            implements PhysicalModel<DummyCost, TestRequirement, DummyProperty>
    {
        INSTANCE;

        @Override
        public CostDomain<DummyCost> costs()
        {
            return TestingCostDomain.INSTANCE;
        }

        @Override
        public RequirementDomain<TestRequirement> requirements()
        {
            return PHYSICAL_MODEL.requirements();
        }

        @Override
        public PropertyDomain<TestRequirement, DummyProperty> properties()
        {
            return PHYSICAL_MODEL.properties();
        }

        @Override
        public XOCompanionDialect<DummyCost, TestRequirement, DummyProperty> companionDialect(Dialect dialect)
        {
            throw new UnsupportedOperationException("companion dialect is not needed in group optimization state tests");
        }
    }
}
