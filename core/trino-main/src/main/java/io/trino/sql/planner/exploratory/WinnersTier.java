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

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.CostComparison;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Tier of winner candidates with minimal cost for a group optimization state.
 * <p>
 * The tier keeps candidates whose cost is tied with the current winner cost
 * according to the physical model's {@link CostDomain}. The first candidate
 * establishes the tier. A later candidate replaces the representative cost
 * only when it is strictly better. Other candidates are retained only when
 * they are in the same approximate tier as the representative.
 * The resulting tier can depend on candidate admission order.
 * <p>
 * Cost ordering and approximate tier membership are separate relations. Tier
 * members may therefore be mutually incomparable, even though each member is
 * in the same approximate tier as the representative cost.
 * <p>
 * Candidates are served in a stable linear extension of the cost partial order:
 * every known better-before-worse relationship is honored, and original
 * production order breaks ties between currently unconstrained candidates.
 * <p>
 * Winners are keyed by {@link AlternativeKey}
 * so that a candidate can be replaced when its cost changes.
 */
public final class WinnersTier<C extends Cost, R extends Requirement, P extends Property>
        implements Iterable<Candidate<C, R, P>>
{
    private final Map<AlternativeKey<R>, Candidate<C, R, P>> winners = new LinkedHashMap<>();
    private C winnerCost;
    private final CostDomain<C> costs;

    private WinnersTier(CostDomain<C> costs)
    {
        this.costs = requireNonNull(costs, "costs is null");
    }

    public WinnersTier(WinnersTier<C, R, P> other)
    {
        winners.putAll(other.winners);
        winnerCost = other.winnerCost;
        this.costs = other.costs;
    }

    public static <C extends Cost, R extends Requirement, P extends Property> WinnersTier<C, R, P> empty(CostDomain<C> costs)
    {
        return new WinnersTier<>(costs);
    }

    CostDomain<C> costs()
    {
        return costs;
    }

    public void add(Candidate<C, R, P> winner)
    {
        checkArgument(!winners.containsKey(winner.alternativeKey()), "winner already exists: %s", winner.alternativeKey());

        if (winners.isEmpty()) {
            winners.put(winner.alternativeKey(), winner);
            winnerCost = winner.cost();
            return;
        }

        C candidateCost = winner.cost();
        CostComparison comparison = costs.compare(candidateCost, winnerCost);
        switch (comparison) {
            case BETTER -> {
                winners.put(winner.alternativeKey(), winner);
                winnerCost = candidateCost;
                winners.entrySet().removeIf(entry -> !costs.sameTier(entry.getValue().cost(), winnerCost));
            }
            case EQUAL, WORSE, INCOMPARABLE -> {
                if (costs.sameTier(candidateCost, winnerCost)) {
                    winners.put(winner.alternativeKey(), winner);
                }
            }
        }
    }

    public void update(Candidate<C, R, P> winner)
    {
        Candidate<C, R, P> previous = winners.replace(winner.alternativeKey(), winner);
        checkArgument(previous != null, "winner not found: %s", winner.alternativeKey());

        rebuildTier();
    }

    private void rebuildTier()
    {
        List<Candidate<C, R, P>> candidates = inAdmissionOrder();
        winners.clear();
        winnerCost = null;

        // Replay in admission order because sameTier is not necessarily transitive.
        candidates.forEach(this::add);
    }

    public boolean isEmpty()
    {
        return winners.isEmpty();
    }

    public int size()
    {
        return winners.size();
    }

    public Optional<C> winnerCost()
    {
        return Optional.ofNullable(winnerCost);
    }

    List<Candidate<C, R, P>> inAdmissionOrder()
    {
        return ImmutableList.copyOf(winners.values());
    }

    public List<Candidate<C, R, P>> inIncreasingCostOrder()
    {
        return CostOrdering.stableLinearExtension(
                ImmutableList.copyOf(winners.values()),
                (left, right) -> costs.compare(left.cost(), right.cost()));
    }

    @Override
    public Iterator<Candidate<C, R, P>> iterator()
    {
        return inIncreasingCostOrder().iterator();
    }
}
