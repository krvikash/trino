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

/**
 * Cost domain that forwards every method to {@link #delegate()}. Test cost
 * domains implement it and override only the methods they customize.
 */
public interface ForwardingCostDomain<C extends Cost>
        extends CostDomain<C>
{
    CostDomain<C> delegate();

    @Override
    default C zero()
    {
        return delegate().zero();
    }

    @Override
    default C infinite()
    {
        return delegate().infinite();
    }

    @Override
    default C parameter()
    {
        return delegate().parameter();
    }

    @Override
    default C add(C left, C right)
    {
        return delegate().add(left, right);
    }

    @Override
    default C addAll(List<C> costs)
    {
        return delegate().addAll(costs);
    }

    @Override
    default CostComparison compare(C left, C right)
    {
        return delegate().compare(left, right);
    }

    @Override
    default CostComparison decide(C left, C right)
    {
        return delegate().decide(left, right);
    }

    @Override
    default boolean sameTier(C left, C right)
    {
        return delegate().sameTier(left, right);
    }
}
