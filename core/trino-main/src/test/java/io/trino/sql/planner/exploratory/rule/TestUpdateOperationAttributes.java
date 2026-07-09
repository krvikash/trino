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
package io.trino.sql.planner.exploratory.rule;

import io.trino.sql.newir.Attributes;
import io.trino.sql.planner.exploratory.BaseOperationRuleTest;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.OperationRuleAssert.Applied;
import io.trino.sql.planner.exploratory.TestingMemo;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonDeterministic;
import static io.trino.sql.planner.exploratory.MemoMatcher.memo;
import static io.trino.sql.planner.exploratory.MemoMatcher.operationPattern;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.childGroup;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.parameter;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.bigint;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.coalesce;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.constant;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.row;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.rowType;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.safeAndDeterministic;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestUpdateOperationAttributes
        extends BaseOperationRuleTest
{
    @Test
    void testUpdatesStaleAttributesFromChildren()
    {
        // The coalesce is stamped with empty attributes, but its constant children are safe and deterministic.
        // The rule recomputes the operation's attributes from its children and updates them.
        tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle left = builder.declareGroup("left");
                    GroupHandle right = builder.declareGroup("right");
                    GroupHandle coalesceGroup = builder.declareGroup("coalesce");
                    builder.addOperation(left, "left", constant());
                    builder.addOperation(right, "right", constant());
                    builder.addOperation(coalesceGroup, "coalesce", coalesce(
                            List.of(),
                            Attributes.empty(),
                            childGroup(left, ParameterLineage.identity(0)),
                            childGroup(right, ParameterLineage.identity(0))));
                    return coalesceGroup;
                })
                .matches(memo("coalesce")
                        .group("coalesce", operationPattern().containsAttributes(safeAndDeterministic())));
    }

    @Test
    void testDoesNotFireWhenAttributesAlreadyCurrent()
    {
        // The coalesce is already stamped with exactly the attributes its constant children imply (safe,
        // deterministic, and no side effects), so the rule recomputes the same attributes and makes no change.
        Attributes.Builder current = Attributes.builder().putAll(safeAndDeterministic());
        hasNoSideEffects(current);
        Attributes alreadyDerived = current.buildOrThrow();
        tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle left = builder.declareGroup("left");
                    GroupHandle right = builder.declareGroup("right");
                    GroupHandle coalesceGroup = builder.declareGroup("coalesce");
                    builder.addOperation(left, "left", constant());
                    builder.addOperation(right, "right", constant());
                    builder.addOperation(coalesceGroup, "coalesce", coalesce(
                            List.of(),
                            alreadyDerived,
                            childGroup(left, ParameterLineage.identity(0)),
                            childGroup(right, ParameterLineage.identity(0))));
                    return coalesceGroup;
                })
                .doesNotFire();
    }

    @Test
    void testFailsOnConflictingAttributes()
    {
        // The coalesce is stamped non-deterministic, but its constant children derive deterministic. Reconciling
        // the stored and freshly derived attributes detects the conflict and fails.
        Attributes.Builder nonDeterministicBuilder = Attributes.builder();
        nonDeterministic(nonDeterministicBuilder);
        Attributes nonDeterministicAttributes = nonDeterministicBuilder.buildOrThrow();

        Applied applied = tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle left = builder.declareGroup("left");
                    GroupHandle right = builder.declareGroup("right");
                    GroupHandle coalesceGroup = builder.declareGroup("coalesce");
                    builder.addOperation(left, "left", constant());
                    builder.addOperation(right, "right", constant());
                    builder.addOperation(coalesceGroup, "coalesce", coalesce(
                            List.of(),
                            nonDeterministicAttributes,
                            childGroup(left, ParameterLineage.identity(0)),
                            childGroup(right, ParameterLineage.identity(0))));
                    return coalesceGroup;
                });

        // applying the rule (not building the memo) is what reconciles stored vs derived and detects the conflict
        assertThatThrownBy(applied::doesNotFire)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicting");
    }

    @Test
    void testDerivesAttributesFromParameterChild()
    {
        // The operation's only child is a parameter (not a group), so its attributes are derived from the block
        // parameter's default attributes. The row is stamped empty and the rule updates it.
        tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle rowGroup = builder.declareGroup("row");
                    builder.addOperation(rowGroup, "row", row(rowType(bigint()), List.of(bigint()), parameter(0)));
                    return rowGroup;
                })
                .matches(memo("row")
                        .group("row", operationPattern().containsAttributes(safeAndDeterministic())));
    }

    @Test
    void testDoesNotMatchOperationWithoutChildren()
    {
        // The rule matches only operations with at least one child; a childless leaf (a constant) is not matched.
        tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle leaf = builder.declareGroup("leaf");
                    builder.addOperation(leaf, "leaf", constant());
                    return leaf;
                })
                .doesNotFire();
    }

    @Test
    void testReachesFixpoint()
    {
        // Applying the rule until fixpoint updates the stale attributes once, then makes no further change.
        TestingMemo result = tester().assertThat(new UpdateOperationAttributes())
                .on(builder -> {
                    GroupHandle left = builder.declareGroup("left");
                    GroupHandle right = builder.declareGroup("right");
                    GroupHandle coalesceGroup = builder.declareGroup("coalesce");
                    builder.addOperation(left, "left", constant());
                    builder.addOperation(right, "right", constant());
                    builder.addOperation(coalesceGroup, "coalesce", coalesce(
                            List.of(),
                            Attributes.empty(),
                            childGroup(left, ParameterLineage.identity(0)),
                            childGroup(right, ParameterLineage.identity(0))));
                    return coalesceGroup;
                })
                .appliesUntilFixpoint();

        memo("coalesce")
                .group("coalesce", operationPattern().containsAttributes(safeAndDeterministic()))
                .matches(result);
    }
}
