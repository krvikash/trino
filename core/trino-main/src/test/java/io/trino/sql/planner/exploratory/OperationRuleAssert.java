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

import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationHandle;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.RuleApplicationContext;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Assertion over a single exploratory {@link OperationRule}, analogous to {@code RuleAssert}.
 * <p>
 * Usage:
 * <pre>{@code
 * tester.assertThat(rule)
 *         .on(builder -> { ...; return rootHandle; })
 *         .at(operationHandle)         // optional; defaults to the only operation in the root group
 *         .matches(memo(root)...);     // applies once, asserts it fired and the resulting shape
 * }</pre>
 * Also: {@link Applied#doesNotFire()} and {@link Applied#appliesUntilFixpoint()}.
 */
public final class OperationRuleAssert
{
    private static final int MAX_FIXPOINT_ITERATIONS = 10_000;

    private final OperationRule rule;
    private final DialectRegistry dialectRegistry;

    OperationRuleAssert(OperationRule rule, DialectRegistry dialectRegistry)
    {
        this.rule = requireNonNull(rule, "rule is null");
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
    }

    public Applied on(Function<TestingMemoBuilder, GroupHandle> planProvider)
    {
        requireNonNull(planProvider, "planProvider is null");
        TestingMemoBuilder builder = new TestingMemoBuilder(dialectRegistry);
        GroupHandle root = planProvider.apply(builder);
        TestingMemo testingMemo = builder.build(root);
        return new Applied(rule, testingMemo);
    }

    public static final class Applied
    {
        private final OperationRule rule;
        private final TestingMemo testingMemo;
        private Optional<OperationHandle> target = Optional.empty();

        private Applied(OperationRule rule, TestingMemo testingMemo)
        {
            this.rule = rule;
            this.testingMemo = testingMemo;
        }

        /**
         * Selects the operation to apply the rule to. Defaults to the single operation in the root group. The
         * operation need not be in the root group, which allows testing rule application that cascades upward.
         */
        public Applied at(OperationHandle operation)
        {
            requireNonNull(operation, "operation is null");
            checkArgument(
                    testingMemo.memo().getGroupId(testingMemo.operation(operation)).isPresent(),
                    "Operation %s is not present in the Memo",
                    operation.name());
            this.target = Optional.of(operation);
            return this;
        }

        public void doesNotFire()
        {
            assertThat(applyRuleOnce(new RuleApplier(testingMemo.memo()), targetOperation()))
                    .as("Expected rule %s not to fire", rule.getClass().getSimpleName())
                    .isFalse();
        }

        /**
         * Applies the rule once. Asserts the rule fired and the resulting Memo matches the expected shape.
         */
        public void matches(MemoMatcher expected)
        {
            assertThat(applyRuleOnce(new RuleApplier(testingMemo.memo()), targetOperation()))
                    .as("Expected rule %s to fire", rule.getClass().getSimpleName())
                    .isTrue();
            expected.matches(testingMemo);
        }

        /**
         * Repeatedly applies the rule to the current target until no more changes are made, respecting the
         * re-application gate ({@link RuleApplicationCache}). Returns the resulting {@link TestingMemo}.
         */
        public TestingMemo appliesUntilFixpoint()
        {
            Memo memo = testingMemo.memo();
            RuleApplier applier = new RuleApplier(memo);
            RuleApplicationCache cache = new RuleApplicationCache();
            boolean firedAtLeastOnce = false;
            for (int iteration = 0; ; iteration++) {
                checkState(iteration < MAX_FIXPOINT_ITERATIONS, "Fixpoint not reached after %s iterations", MAX_FIXPOINT_ITERATIONS);
                Optional<MemoOperation> current = currentTargetOperation();
                if (current.isEmpty()) {
                    break;
                }
                MemoOperation operation = current.get();
                RuleApplicationContext context = rule.getApplicationContext(operation, memo);
                if (!cache.markIfNew(operation, rule, context)) {
                    break;
                }
                if (!applyRuleOnce(applier, operation)) {
                    break;
                }
                firedAtLeastOnce = true;
            }
            assertThat(firedAtLeastOnce)
                    .as("Expected rule %s to fire at least once", rule.getClass().getSimpleName())
                    .isTrue();
            return testingMemo;
        }

        /**
         * Applies the rule once using the given applier and returns whether it fired (produced any Memo change).
         */
        private boolean applyRuleOnce(RuleApplier applier, MemoOperation operation)
        {
            return !applier.applyRule(operation, rule).isEmpty();
        }

        private MemoOperation targetOperation()
        {
            return currentTargetOperation()
                    .orElseThrow(() -> new IllegalStateException("Target operation is no longer present in the Memo"));
        }

        /**
         * Resolves the target operation against the current Memo. Follows the target handle if given (through
         * merges and attribute successions); otherwise uses the single operation of the current root group.
         * Returns empty if the target has been removed.
         */
        private Optional<MemoOperation> currentTargetOperation()
        {
            Memo memo = testingMemo.memo();
            if (target.isPresent()) {
                MemoOperation operation = testingMemo.operation(target.get());
                if (memo.getGroupId(operation).isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(operation);
            }
            MemoGroup rootGroup = memo.getGroup(memo.rootGroup());
            checkState(
                    rootGroup.operations().size() == 1,
                    "Root group has %s operations; use at(operationHandle) to choose the target",
                    rootGroup.operations().size());
            return Optional.of(rootGroup.operations().getFirst());
        }
    }
}
