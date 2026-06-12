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

import io.trino.sql.planner.exploratory.Candidate.CandidateKey;
import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Optimization state of a {@code (Group, Requirement)} request.
 * <p>
 * Field semantics:
 * <ul>
 *     <li>{@code groupId} - group being optimized</li>
 *     <li>{@code requirement} - incoming requirement for this optimization
 *     request</li>
 *     <li>{@code winners} - active tier of winner candidates for this state.
 *     The first produced candidate establishes the tier. A later candidate
 *     becomes its representative only when it is strictly better; otherwise,
 *     it joins the tier only when it is in the same approximate tier as the
 *     representative. Tier members may have different exact costs and may be
 *     mutually incomparable. During extraction, candidates are tried in a
 *     stable linear extension of the cost order until a non-recursive winner
 *     is found. The XO model relies on the active tier containing at least one
 *     non-recursive candidate. See {@link WinnersTier} for detailed membership
 *     and ordering semantics.</li>
 *     <li>{@code lastParentNotificationCost} - winner cost at which this state
 *     most recently emitted a parent-notification event. This does not imply
 *     that every parent has already processed the notification.</li>
 * </ul>
 */
public final class GroupOptimizationState<C extends Cost, R extends Requirement, P extends Property>
{
    private final int groupId;
    private final R requirement; // canonicalized
    private final WinnersTier<C, R, P> winners;
    private final Optional<C> lastParentNotificationCost;

    private GroupOptimizationState(int groupId, R requirement, WinnersTier<C, R, P> winners, Optional<C> lastParentNotificationCost)
    {
        this.groupId = groupId;
        // requirement is canonical (callers come via initialize(...)/replaceWinners(...)/withGroupId(...), all of which preserve canonicalization)
        this.requirement = requireNonNull(requirement, "requirement is null");
        this.winners = new WinnersTier<>(requireNonNull(winners, "winners is null"));
        this.lastParentNotificationCost = requireNonNull(lastParentNotificationCost, "lastParentNotificationCost is null");
    }

    public static <C extends Cost, R extends Requirement, P extends Property> GroupOptimizationState<C, R, P> initialize(
            StateKey<R> stateKey,
            CostDomain<C> costs)
    {
        return new GroupOptimizationState<>(stateKey.groupId(), stateKey.requirement(), WinnersTier.empty(costs), Optional.empty());
    }

    public int groupId()
    {
        return groupId;
    }

    public R requirement()
    {
        return requirement;
    }

    public WinnersTier<C, R, P> winners()
    {
        return new WinnersTier<>(winners);
    }

    public Optional<C> winnerCost()
    {
        return winners.winnerCost();
    }

    /**
     * Notify when winner-cost presence changes or the current cost leaves the
     * tier recorded at the last notification. Same-tier changes are not
     * propagated because the {@link CostDomain#sameTier(Cost, Cost)} contract
     * makes them immaterial to dependent costing and plan selection.
     */
    public boolean shouldNotifyParents()
    {
        Optional<C> currentWinnerCost = winnerCost();
        if (currentWinnerCost.isEmpty() || lastParentNotificationCost.isEmpty()) {
            return !currentWinnerCost.equals(lastParentNotificationCost);
        }
        return !winners.costs().sameTier(currentWinnerCost.orElseThrow(), lastParentNotificationCost.orElseThrow());
    }

    public GroupOptimizationState<C, R, P> withParentsNotified()
    {
        return new GroupOptimizationState<>(groupId, requirement, winners, winnerCost());
    }

    public GroupOptimizationState<C, R, P> withGroupId(int groupId)
    {
        return new GroupOptimizationState<>(groupId, requirement, winners, lastParentNotificationCost);
    }

    public GroupOptimizationState<C, R, P> replaceWinners(WinnersTier<C, R, P> winners)
    {
        return new GroupOptimizationState<>(groupId, requirement, winners, lastParentNotificationCost);
    }

    public static <C extends Cost, R extends Requirement, P extends Property> GroupOptimizationState<C, R, P> mergeStates(
            GroupOptimizationState<C, R, P> survivingState,
            GroupOptimizationState<C, R, P> removedState)
    {
        checkArgument(
                survivingState.requirement().equals(removedState.requirement()),
                "requirements do not match: %s vs %s",
                survivingState.requirement(),
                removedState.requirement());

        WinnersTier<C, R, P> mergedWinners = survivingState.winners();
        removedState.winners().inAdmissionOrder().forEach(mergedWinners::add);

        // Parents of the removed state must be reprocessed
        return survivingState.replaceWinners(mergedWinners);
    }

    public GroupOptimizationState<C, R, P> removeCandidates(Set<CandidateKey<R>> candidatesToRemove)
    {
        Set<AlternativeKey<R>> alternativesToRemove = candidatesToRemove.stream()
                .filter(candidateKey -> candidateKey.requirement().equals(requirement))
                .map(CandidateKey::alternativeKey)
                .collect(toImmutableSet());

        if (alternativesToRemove.isEmpty()) {
            return this;
        }

        List<Candidate<C, R, P>> currentWinners = winners.inAdmissionOrder();
        if (currentWinners.stream().noneMatch(candidate -> alternativesToRemove.contains(candidate.alternativeKey()))) {
            return this;
        }

        WinnersTier<C, R, P> newWinners = WinnersTier.empty(winners.costs());
        for (Candidate<C, R, P> candidate : currentWinners) {
            if (!alternativesToRemove.contains(candidate.alternativeKey())) {
                newWinners.add(candidate);
            }
        }
        return replaceWinners(newWinners);
    }

    public GroupOptimizationState<C, R, P> addCandidate(Candidate<C, R, P> candidate)
    {
        WinnersTier<C, R, P> newWinners = winners();
        newWinners.add(candidate);
        return replaceWinners(newWinners);
    }

    public StateKey<R> key()
    {
        return new StateKey<>(groupId, requirement);
    }

    // not a record so that we can enforce canonicalization on construction
    public static final class StateKey<R extends Requirement>
    {
        private final int groupId;
        private final R requirement; // canonicalized

        public static <R extends Requirement> StateKey<R> stateKey(
                int groupId,
                R requirement,
                RequirementDomain<R> requirements)
        {
            requireNonNull(requirements, "requirements is null");
            return new StateKey<>(groupId, requirements.canonicalize(requirement));
        }

        private StateKey(int groupId, R requirement)
        {
            this.groupId = groupId;
            // requirement is canonical; use stateKey(...) for external callers
            this.requirement = requireNonNull(requirement, "requirement is null");
        }

        public int groupId()
        {
            return groupId;
        }

        public R requirement()
        {
            return requirement;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof StateKey<?> otherStateKey)) {
                return false;
            }
            return groupId == otherStateKey.groupId &&
                    requirement.equals(otherStateKey.requirement);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(groupId, requirement);
        }

        @Override
        public String toString()
        {
            return "StateKey[groupId=" + groupId + ", requirement=" + requirement + ']';
        }
    }
}
