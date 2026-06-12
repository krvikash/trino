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
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.CostComparison;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;
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
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestGroupOptimizationState
{
    private static final TestingCost COST_0 = new TestingCost(0);
    private static final TestingCost COST_1 = new TestingCost(1);
    private static final TestingCost COST_2 = new TestingCost(2);
    private static final TestingCost COST_3 = new TestingCost(3);
    private static final TestingCost INFINITE_COST = new TestingCost(Double.POSITIVE_INFINITY);

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
        assertThat(updated.winnerCost()).contains(UNKNOWN);

        // winners are defensively copied into the state
        winners.add(candidate(alternative(alternativeLeafOperation())));
        assertThat(updated.winners()).containsExactly(candidate);

        // the original state stays unchanged
        assertThat(state.groupId()).isEqualTo(1);
        assertThat(state.winners()).isEmpty();
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
        assertThat(mergedState.winnerCost()).contains(UNKNOWN);

        // source states stay unchanged
        assertThat(survivingState.winners()).containsExactly(survivingCandidate);
        assertThat(removedState.winners()).containsExactly(removedCandidate);
    }

    @Test
    public void testMergeStatesPreservesAdmissionOrder()
    {
        var survivingCandidate = candidateWithCost(leafOperation(), COST_1);
        var firstRemovedCandidate = candidateWithCost(alternativeLeafOperation(), COST_2);
        var secondRemovedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("second_removed")
                        .build(),
                COST_0);
        var thirdRemovedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("third_removed")
                        .build(),
                COST_3);

        WinnersTier<TestingCost, TestRequirement, DummyProperty> survivingWinners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        survivingWinners.add(survivingCandidate);
        WinnersTier<TestingCost, TestRequirement, DummyProperty> removedWinners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        removedWinners.add(firstRemovedCandidate);
        removedWinners.add(secondRemovedCandidate);
        removedWinners.add(thirdRemovedCandidate);
        assertThat(removedWinners.inAdmissionOrder()).containsExactly(firstRemovedCandidate, secondRemovedCandidate, thirdRemovedCandidate);
        assertThat(removedWinners.inIncreasingCostOrder()).containsExactly(secondRemovedCandidate, firstRemovedCandidate, thirdRemovedCandidate);

        var survivingState = GroupOptimizationState.<TestingCost, TestRequirement, DummyProperty>initialize(
                        stateKey(1, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(survivingWinners);
        var removedState = GroupOptimizationState.<TestingCost, TestRequirement, DummyProperty>initialize(
                        stateKey(2, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(removedWinners);

        var mergedState = GroupOptimizationState.mergeStates(survivingState, removedState);

        // The first removed candidate is not admitted relative to the surviving representative.
        // Replaying the second candidate first would admit the first one afterward.
        assertThat(mergedState.winners().inAdmissionOrder())
                .containsExactly(survivingCandidate, secondRemovedCandidate, thirdRemovedCandidate);
        assertThat(mergedState.winnerCost()).contains(COST_0);
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
        assertThat(updated.winnerCost()).contains(UNKNOWN);

        // the original state stays unchanged
        assertThat(state.winners()).containsExactlyInAnyOrder(firstCandidate, secondCandidate);
    }

    @Test
    public void testRemoveCandidatesPreservesAdmissionOrder()
    {
        var firstCandidate = candidateWithCost(leafOperation(), COST_1);
        var secondCandidate = candidateWithCost(alternativeLeafOperation(), COST_3);
        var thirdCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("third")
                        .build(),
                COST_0);
        var fourthCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("fourth")
                        .build(),
                COST_2);
        WinnersTier<TestingCost, TestRequirement, DummyProperty> winners = WinnersTier.empty(ReplayCostDomain.INSTANCE);
        winners.add(firstCandidate);
        winners.add(secondCandidate);
        winners.add(thirdCandidate);
        winners.add(fourthCandidate);
        assertThat(winners.inAdmissionOrder()).containsExactly(firstCandidate, secondCandidate, thirdCandidate, fourthCandidate);
        assertThat(winners.inIncreasingCostOrder()).containsExactly(secondCandidate, thirdCandidate, firstCandidate, fourthCandidate);

        var state = GroupOptimizationState.<TestingCost, TestRequirement, DummyProperty>initialize(
                        stateKey(1, UNDEFINED),
                        ReplayCostDomain.INSTANCE)
                .replaceWinners(winners);

        var updated = state.removeCandidates(ImmutableSet.of());

        assertThat(updated).isSameAs(state);
        assertThat(updated.winners().inAdmissionOrder())
                .containsExactly(firstCandidate, secondCandidate, thirdCandidate, fourthCandidate);
        assertThat(updated.winnerCost()).contains(COST_0);

        assertThat(state.removeCandidates(ImmutableSet.of(candidateKey(firstCandidate.alternativeKey(), OTHER))))
                .isSameAs(state);

        var unrelatedCandidate = candidateWithCost(
                MemoOperationBuilder.from(leafOperation())
                        .withDialect("unrelated")
                        .build(),
                COST_0);
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
        assertThat(updated.winnerCost()).contains(UNKNOWN);
    }

    @Test
    public void testWinnerCostPresenceChangesNotifyParents()
    {
        var candidate = candidate(alternative(leafOperation()));
        var state = state(stateKey(1, UNDEFINED))
                .addCandidate(candidate);

        assertThat(state.shouldNotifyParents()).isTrue();

        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.removeCandidates(ImmutableSet.of(candidateKey(candidate.alternativeKey(), UNDEFINED)));
        assertThat(state.winnerCost()).isEmpty();
        assertThat(state.shouldNotifyParents()).isTrue();

        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();
    }

    @Test
    public void testParentNotificationTierDoesNotDrift()
    {
        var state = stateWithWinnerCost(COST_0);

        assertThat(state.shouldNotifyParents()).isTrue();
        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.replaceWinners(winnersWithCost(COST_1));

        // Costs 0 and 1 are in the same tier, so parent notification is suppressed.
        assertThat(state.shouldNotifyParents()).isFalse();

        state = state.replaceWinners(winnersWithCost(COST_2));

        // Costs 1 and 2 are in the same tier, but costs 0 and 2 are not.
        // The notification anchor remains 0, so parents must now be notified.
        assertThat(TestingCostDomain.INSTANCE.sameTier(COST_1, COST_2)).isTrue();
        assertThat(TestingCostDomain.INSTANCE.sameTier(COST_0, COST_2)).isFalse();
        assertThat(state.shouldNotifyParents()).isTrue();

        state = state.withParentsNotified();
        assertThat(state.shouldNotifyParents()).isFalse();
    }

    @Test
    public void testMergeStatesPreservesSurvivingParentNotificationCost()
    {
        var survivingState = stateWithWinnerCost(COST_0)
                .withParentsNotified();

        var removedWinners = WinnersTier.<TestingCost, TestRequirement, DummyProperty>empty(TestingCostDomain.INSTANCE);
        removedWinners.add(candidateWithCost(alternativeLeafOperation(), COST_2));
        var removedState = GroupOptimizationState.<TestingCost, TestRequirement, DummyProperty>initialize(
                        stateKey(2, UNDEFINED),
                        TestingCostDomain.INSTANCE)
                .replaceWinners(removedWinners)
                .withParentsNotified();

        var mergedState = GroupOptimizationState.mergeStates(survivingState, removedState);

        assertThat(mergedState.winnerCost()).contains(COST_0);
        assertThat(mergedState.shouldNotifyParents()).isFalse();
    }

    private static GroupOptimizationState<TestingCost, TestRequirement, DummyProperty> stateWithWinnerCost(TestingCost cost)
    {
        return GroupOptimizationState.<TestingCost, TestRequirement, DummyProperty>initialize(stateKey(1, UNDEFINED), TestingCostDomain.INSTANCE)
                .replaceWinners(winnersWithCost(cost));
    }

    private static WinnersTier<TestingCost, TestRequirement, DummyProperty> winnersWithCost(TestingCost cost)
    {
        WinnersTier<TestingCost, TestRequirement, DummyProperty> winners = WinnersTier.empty(TestingCostDomain.INSTANCE);
        winners.add(candidateWithCost(leafOperation(), cost));
        return winners;
    }

    private static Candidate<TestingCost, TestRequirement, DummyProperty> candidateWithCost(MemoOperation operation, TestingCost cost)
    {
        OperationImplementationAlternative<TestingCost, TestRequirement, DummyProperty> alternative = OperationImplementationAlternative.alternative(
                operation,
                List.of(),
                List.of(),
                List.of(),
                DummyProperty.UNKNOWN,
                cost,
                TestingPhysicalModel.INSTANCE.requirements(),
                TestingPhysicalModel.INSTANCE.properties());
        Candidate<TestingCost, TestRequirement, DummyProperty> candidate = new Candidate<>(
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

    private record TestingCost(double value)
            implements Cost {}

    private enum TestingCostDomain
            implements CostDomain<TestingCost>
    {
        INSTANCE;

        private static final double TIER_TOLERANCE = 1;

        @Override
        public TestingCost zero()
        {
            return COST_0;
        }

        @Override
        public TestingCost infinite()
        {
            return INFINITE_COST;
        }

        @Override
        public TestingCost parameter()
        {
            return COST_0;
        }

        @Override
        public TestingCost add(TestingCost left, TestingCost right)
        {
            return new TestingCost(left.value() + right.value());
        }

        @Override
        public TestingCost addAll(List<TestingCost> costs)
        {
            TestingCost result = zero();
            for (TestingCost cost : costs) {
                result = add(result, cost);
            }
            return result;
        }

        @Override
        public CostComparison compare(TestingCost left, TestingCost right)
        {
            int comparison = Double.compare(left.value(), right.value());
            if (comparison < 0) {
                return BETTER;
            }
            if (comparison > 0) {
                return WORSE;
            }
            return EQUAL;
        }

        @Override
        public boolean sameTier(TestingCost left, TestingCost right)
        {
            return left.equals(right) || Math.abs(left.value() - right.value()) <= TIER_TOLERANCE;
        }
    }

    /**
     * A lawful partial cost order and a non-transitive tier relation chosen to
     * make admission order observably different from increasing cost order.
     */
    private enum ReplayCostDomain
            implements CostDomain<TestingCost>
    {
        INSTANCE;

        @Override
        public TestingCost zero()
        {
            return COST_0;
        }

        @Override
        public TestingCost infinite()
        {
            return INFINITE_COST;
        }

        @Override
        public TestingCost parameter()
        {
            return COST_0;
        }

        @Override
        public TestingCost add(TestingCost left, TestingCost right)
        {
            return new TestingCost(left.value() + right.value());
        }

        @Override
        public TestingCost addAll(List<TestingCost> costs)
        {
            TestingCost result = zero();
            for (TestingCost cost : costs) {
                result = add(result, cost);
            }
            return result;
        }

        @Override
        public CostComparison compare(TestingCost left, TestingCost right)
        {
            if (left.equals(right)) {
                return EQUAL;
            }
            if (left.equals(INFINITE_COST)) {
                return WORSE;
            }
            if (right.equals(INFINITE_COST)) {
                return BETTER;
            }
            if (left.equals(COST_0) && (right.equals(COST_1) || right.equals(COST_2))) {
                return BETTER;
            }
            if (right.equals(COST_0) && (left.equals(COST_1) || left.equals(COST_2))) {
                return WORSE;
            }
            return INCOMPARABLE;
        }

        @Override
        public boolean sameTier(TestingCost left, TestingCost right)
        {
            return left.equals(right) ||
                    (left.equals(COST_0) && !right.equals(INFINITE_COST)) ||
                    (right.equals(COST_0) && !left.equals(INFINITE_COST)) ||
                    (left.equals(COST_1) && right.equals(COST_3)) ||
                    (left.equals(COST_3) && right.equals(COST_1));
        }
    }

    private enum TestingPhysicalModel
            implements PhysicalModel<TestingCost, TestRequirement, DummyProperty>
    {
        INSTANCE;

        @Override
        public CostDomain<TestingCost> costs()
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
        public XOCompanionDialect<TestingCost, TestRequirement, DummyProperty> companionDialect(Dialect dialect)
        {
            throw new UnsupportedOperationException("companion dialect is not needed in group optimization state tests");
        }
    }
}
