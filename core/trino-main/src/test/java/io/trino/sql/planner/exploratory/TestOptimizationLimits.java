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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TestOptimizationLimits
{
    @Test
    public void testDefaultsScaleWithInputAndRules()
    {
        OptimizationLimits small = OptimizationLimits.defaultFor(100, 0);
        OptimizationLimits larger = OptimizationLimits.defaultFor(1_000, 0);
        OptimizationLimits moreRules = OptimizationLimits.defaultFor(1_000, 1);

        assertThat(small).isEqualTo(new OptimizationLimits(20_000, 300_000, 20_000, 2_000));
        assertThat(larger).isEqualTo(new OptimizationLimits(100_000, 1_100_000, 110_000, 11_000));
        assertThat(moreRules).isEqualTo(new OptimizationLimits(100_000, 2_100_000, 110_000, 11_000));
    }

    @Test
    public void testDefaultsAreCappedWithoutOverflow()
    {
        OptimizationLimits limits = OptimizationLimits.defaultFor(Long.MAX_VALUE, Integer.MAX_VALUE);

        assertThat(limits).isEqualTo(new OptimizationLimits(100_000, 5_000_000, 1_000_000, 100_000));
    }

    @Test
    public void testRejectsNegativeLimit()
    {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new OptimizationLimits(-1, 0, 0, 0));
    }

    @Test
    public void testTracksConsumedMemoGrowthTokens()
    {
        OptimizationBudget budget = new OptimizationBudget(new OptimizationLimits(10, 0, 0, 0));

        assertThat(budget.consumedMemoGrowthTokens()).isZero();
        budget.consumeMemoGrowthTokens(4);
        assertThat(budget.consumedMemoGrowthTokens()).isEqualTo(4);
        budget.consumeMemoGrowthTokens(6);
        assertThat(budget.consumedMemoGrowthTokens()).isEqualTo(10);
    }
}
