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

import io.trino.sql.planner.exploratory.physical.CostComparison;
import io.trino.sql.planner.exploratory.physical.CostDomain;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.exploratory.physical.CostComparison.BETTER;
import static io.trino.sql.planner.exploratory.physical.CostComparison.EQUAL;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.INFINITE;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;

/**
 * Totally ordered, additive cost domain of the dummy physical model. Costs
 * count operations, so the knowledge order and the decision order coincide.
 */
public final class DummyCostDomain
        implements CostDomain<DummyCost>
{
    public static final DummyCostDomain INSTANCE = new DummyCostDomain();

    private DummyCostDomain() {}

    @Override
    public DummyCost zero()
    {
        return ZERO;
    }

    @Override
    public DummyCost infinite()
    {
        return INFINITE;
    }

    @Override
    public DummyCost parameter()
    {
        return ZERO;
    }

    @Override
    public DummyCost add(DummyCost left, DummyCost right)
    {
        if (left.isInfinite() || right.isInfinite()) {
            return INFINITE;
        }
        checkArgument(left.operations() < INFINITE.operations() - right.operations(), "finite cost overflow: %s + %s", left, right);
        return new DummyCost(left.operations() + right.operations());
    }

    @Override
    public DummyCost addAll(List<DummyCost> costs)
    {
        DummyCost result = zero();
        for (DummyCost cost : costs) {
            result = add(result, cost);
        }
        return result;
    }

    @Override
    public CostComparison compare(DummyCost left, DummyCost right)
    {
        int comparison = Long.compare(left.operations(), right.operations());
        if (comparison < 0) {
            return BETTER;
        }
        if (comparison > 0) {
            return WORSE;
        }
        return EQUAL;
    }

    @Override
    public CostComparison decide(DummyCost left, DummyCost right)
    {
        return compare(left, right);
    }

    @Override
    public boolean sameTier(DummyCost left, DummyCost right)
    {
        return left.equals(right);
    }
}
