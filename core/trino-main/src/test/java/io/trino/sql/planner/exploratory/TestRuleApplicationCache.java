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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.planner.exploratory.rule.Context;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.Result;
import io.trino.sql.planner.exploratory.rule.RuleApplicationContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestRuleApplicationCache
{
    @Test
    void testDefaultEmptyContextCachesOnlyOnce()
    {
        RuleApplicationCache cache = new RuleApplicationCache();
        MemoOperation operation = testOperation();
        OperationRule rule = new NoOpRule();

        assertThat(cache.markIfNew(operation, rule, rule.getApplicationContext(operation, null))).isTrue();
        assertThat(cache.markIfNew(operation, rule, rule.getApplicationContext(operation, null))).isFalse();
    }

    @Test
    void testDifferentContextsAreTrackedSeparately()
    {
        RuleApplicationCache cache = new RuleApplicationCache();
        MemoOperation operation = testOperation();
        OperationRule rule = new NoOpRule();

        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isTrue();
        assertThat(cache.markIfNew(operation, rule, new TestContext("b"))).isTrue();
        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isFalse();
    }

    @Test
    void testDifferentRulesHaveIndependentContexts()
    {
        RuleApplicationCache cache = new RuleApplicationCache();
        MemoOperation operation = testOperation();

        assertThat(cache.markIfNew(operation, new NoOpRule(), RuleApplicationContext.EMPTY)).isTrue();
        assertThat(cache.markIfNew(operation, new OtherNoOpRule(), RuleApplicationContext.EMPTY)).isTrue();
    }

    @Test
    void testEmptyNonEmptySequence()
    {
        RuleApplicationCache cache = new RuleApplicationCache();
        MemoOperation operation = testOperation();
        OperationRule rule = new NoOpRule();

        assertThat(cache.markIfNew(operation, rule, RuleApplicationContext.EMPTY)).isTrue();
        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isTrue();
        assertThat(cache.markIfNew(operation, rule, RuleApplicationContext.EMPTY)).isFalse();
        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isFalse();
    }

    @Test
    void testNonEmptyEmptySequence()
    {
        RuleApplicationCache cache = new RuleApplicationCache();
        MemoOperation operation = testOperation();
        OperationRule rule = new NoOpRule();

        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isTrue();
        assertThat(cache.markIfNew(operation, rule, RuleApplicationContext.EMPTY)).isTrue();
        assertThat(cache.markIfNew(operation, rule, RuleApplicationContext.EMPTY)).isFalse();
        assertThat(cache.markIfNew(operation, rule, new TestContext("a"))).isFalse();
    }

    private static MemoOperation testOperation()
    {
        return MemoOperationBuilder.from(MemoTestingHelper.TEST_MEMO_OPERATION)
                .withOperationId(new OperationId("test_operation", MemoTestingHelper.TEST_MEMO_OPERATION.operationId().argumentTypes(), MemoTestingHelper.TEST_MEMO_OPERATION.operationId().regionTypes()))
                .build();
    }

    private record TestContext(String value)
            implements RuleApplicationContext {}

    private static class NoOpRule
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class OtherNoOpRule
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            throw new UnsupportedOperationException();
        }
    }
}
