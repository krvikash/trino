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
package io.trino.sql.planner.optimizations;

import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class SkipForSingleNodeOptimizer
        implements PlanOptimizer
{
    private final PlanOptimizer delegate;

    public SkipForSingleNodeOptimizer(PlanOptimizer delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        if (!context.forceSingleNodeQuery()) {
            return delegate.optimize(plan, context);
        }
        return plan;
    }
}
