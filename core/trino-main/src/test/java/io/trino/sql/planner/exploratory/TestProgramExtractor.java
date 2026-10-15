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

import static io.trino.spi.StandardErrorCode.XO_ERROR;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.UNDEFINED;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.baseMemo;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.state;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.stateKey;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.store;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

class TestProgramExtractor
{
    @Test
    public void testEmptyWinnersFailsWithXoError()
    {
        Memo memo = baseMemo();
        var optimizationStore = store(memo);
        var rootStateKey = stateKey(memo.rootGroup(), UNDEFINED);
        optimizationStore.putNewState(state(rootStateKey));

        assertTrinoExceptionThrownBy(() -> new ProgramExtractor<>(
                memo,
                UNDEFINED,
                optimizationStore,
                new OptimizationBudget(OptimizationLimits.defaultFor(memo.operationTokens(), 0)))
                .extractProgram())
                .hasErrorCode(XO_ERROR)
                .hasMessage("No winner for state %s".formatted(rootStateKey));
    }
}
