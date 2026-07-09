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

import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.BaseOperationRuleTest;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.TestingMemo;
import io.trino.sql.planner.exploratory.TestingMemoBuilder;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoMatcher.groupEdge;
import static io.trino.sql.planner.exploratory.MemoMatcher.groupPattern;
import static io.trino.sql.planner.exploratory.MemoMatcher.memo;
import static io.trino.sql.planner.exploratory.MemoMatcher.operationPattern;
import static io.trino.sql.planner.exploratory.MemoMatcher.parameterEdge;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.childGroup;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.parameter;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.bigint;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.constant;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.project;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.relationType;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.row;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.rowType;
import static io.trino.sql.planner.exploratory.TrinoMemoOperations.valuesWithoutFields;
import static org.assertj.core.api.Assertions.assertThat;

class TestPruneUnusedParameters
        extends BaseOperationRuleTest
{
    @Test
    void testPruneUnusedGroupParameter()
    {
        // The parent passes both of its group parameters to a child group that uses only the first.
        Type childRow = rowType(bigint());
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(child, "child", row(childRow, List.of(bigint(), bigint()), parameter(0)));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1), List.of()))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes(bigint(), bigint())
                                .children(groupEdge("prunedChild", new ParameterLineage(List.of(0), List.of()))))
                        .group("prunedChild", groupPattern()
                                .resultType(childRow)
                                .groupParameterTypes(bigint())
                                .operations(operationPattern().children(parameterEdge(0)))));
    }

    @Test
    void testRemapRetainedParameterIndices()
    {
        // Child parameters [0, 1, 2] with only 0 and 2 used (0 directly, 2 passed to a grandchild); pruning 1
        // remaps the retained indices, so ParameterChild(2) -> (1) and the grandchild edge lineage [2] -> [1].
        Type grandchildRow = rowType(bigint());
        Type childRow = rowType(bigint(), grandchildRow);
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle grandchild = builder.declareGroup("grandchild");
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(grandchild, "grandchild", row(grandchildRow, List.of(bigint()), parameter(0)));
                    builder.addOperation(child, "child", row(
                            childRow,
                            List.of(bigint(), bigint(), bigint()),
                            parameter(0),
                            childGroup(grandchild, new ParameterLineage(List.of(2), List.of()))));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1, 2), List.of()))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes(bigint(), bigint(), bigint())
                                .children(groupEdge("prunedChild", new ParameterLineage(List.of(0, 2), List.of()))))
                        .group("prunedChild", operationPattern()
                                .groupParameterTypes(bigint(), bigint())
                                .children(parameterEdge(0), groupEdge("grandchild", new ParameterLineage(List.of(1), List.of()))))
                        .group("grandchild", operationPattern()
                                .groupParameterTypes(bigint())
                                .children(parameterEdge(0))));
    }

    @Test
    void testUsesUnionOfParametersAcrossChildGroupAlternatives()
    {
        // The child group has two alternatives: one uses parameter 1, the other parameter 2. Parameter 0 is used
        // by neither, so the rule prunes it (the used set is the union across alternatives) and remaps every
        // alternative (1 -> 0, 2 -> 1).
        Type childRow = rowType(bigint());
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(child, "childAlternative0", row(childRow, List.of(bigint(), bigint(), bigint()), parameter(1)));
                    builder.addOperation(child, "childAlternative1", row(childRow, List.of(bigint(), bigint(), bigint()), parameter(2)));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1, 2), List.of()))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes(bigint(), bigint(), bigint())
                                .children(groupEdge("prunedChild", new ParameterLineage(List.of(1, 2), List.of()))))
                        .group("prunedChild", groupPattern()
                                .groupParameterTypes(bigint(), bigint())
                                .containsOperations(
                                        operationPattern().children(parameterEdge(0)),
                                        operationPattern().children(parameterEdge(1)))));
    }

    @Test
    void testPruneMultipleChildGroupsInOnePass()
    {
        // The parent passes unused parameters to two distinct child groups; both are pruned in a single pass.
        // childA (a constant ignoring its only parameter) and childB (a row using its first of two parameters)
        // prune to different shapes, so they do not deduplicate.
        Type childBRow = rowType(bigint());
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle childA = builder.declareGroup("childA");
                    GroupHandle childB = builder.declareGroup("childB");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(childA, "childA", constant(bigint()));
                    builder.addOperation(childB, "childB", row(childBRow, List.of(bigint(), bigint()), parameter(0)));
                    builder.addOperation(parent, "parent", row(
                            rowType(bigint(), childBRow),
                            List.of(bigint(), bigint(), bigint()),
                            childGroup(childA, new ParameterLineage(List.of(0), List.of())),
                            childGroup(childB, new ParameterLineage(List.of(1, 2), List.of()))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes(bigint(), bigint(), bigint())
                                .children(
                                        groupEdge("prunedA", new ParameterLineage(List.of(), List.of())),
                                        groupEdge("prunedB", new ParameterLineage(List.of(1), List.of()))))
                        .group("prunedA", operationPattern().groupParameterTypes())
                        .group("prunedB", operationPattern().groupParameterTypes(bigint()).children(parameterEdge(0))));
    }

    @Test
    void testPruneUnusedProperParameter()
    {
        // The parent projects a relation source; its assignments region receives the input row as a proper
        // parameter and passes it to a child group that ignores it, so that proper parameter is pruned.
        Type relation = relationType(rowType());
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle source = builder.declareGroup("source");
                    GroupHandle assignments = builder.declareGroup("assignments");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(source, "source", valuesWithoutFields());
                    builder.addOperation(assignments, "assignments", constant(rowType()));
                    builder.addOperation(parent, "parent", project(
                            relation,
                            rowType(bigint()),
                            List.of(),
                            childGroup(source, new ParameterLineage(List.of(), List.of())),
                            childGroup(assignments, new ParameterLineage(List.of(), List.of(0)))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes()
                                .children(
                                        groupEdge("source", new ParameterLineage(List.of(), List.of())),
                                        groupEdge("prunedAssignments", new ParameterLineage(List.of(), List.of()))))
                        .group("source", operationPattern().groupParameterTypes())
                        .group("prunedAssignments", operationPattern().groupParameterTypes()));
    }

    @Test
    void testPrunesProperParameterWhileRetainingGroupParameter()
    {
        // A project forwards one group parameter AND the input row (proper parameter) to its assignments group,
        // which uses only the group parameter. The proper parameter is pruned while the group parameter is
        // retained, exercising the proper-parameter offset after the retained group parameter.
        Type emptyRow = rowType();
        Type relation = relationType(emptyRow);
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle source = builder.declareGroup("source");
                    GroupHandle assignments = builder.declareGroup("assignments");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(source, "source", valuesWithoutFields());
                    builder.addOperation(assignments, "assignments", row(rowType(bigint()), List.of(bigint(), emptyRow), parameter(0)));
                    builder.addOperation(parent, "parent", project(
                            relation,
                            rowType(bigint()),
                            List.of(bigint()),
                            childGroup(source, new ParameterLineage(List.of(), List.of())),
                            childGroup(assignments, new ParameterLineage(List.of(0), List.of(0)))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .groupParameterTypes(bigint())
                                .children(
                                        groupEdge("source", new ParameterLineage(List.of(), List.of())),
                                        groupEdge("prunedAssignments", new ParameterLineage(List.of(0), List.of()))))
                        .group("source", operationPattern().groupParameterTypes())
                        .group("prunedAssignments", operationPattern()
                                .groupParameterTypes(bigint())
                                .children(parameterEdge(0))));
    }

    @Test
    void testDoNotPruneWhenChildGroupUsesAllParameters()
    {
        Type childRow = rowType(bigint(), bigint());
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(child, "child", row(childRow, List.of(bigint(), bigint()), parameter(0), parameter(1)));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1), List.of()))));
                    return parent;
                })
                .doesNotFire();
    }

    @Test
    void testDoNotPruneWhenChildGroupHasNoParameters()
    {
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(child, "child", constant());
                    builder.addOperation(parent, "parent", row(
                            rowType(bigint()),
                            List.of(),
                            childGroup(child, ParameterLineage.identity(0))));
                    return parent;
                })
                .doesNotFire();
    }

    @Test
    void testApplicationContextIsEmptyWhenOperationHasNoChildGroups()
    {
        // An operation whose only child is a parameter (no child groups) has nothing to prune, so the rule
        // reports an empty application context (the fixpoint gate relies on this).
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle leaf = builder.declareGroup("leaf");
        OperationHandle leafOperation = builder.addOperation(leaf, "leaf", row(rowType(bigint()), List.of(bigint()), parameter(0)));
        TestingMemo memo = builder.build(leaf);

        RuleApplicationContext context = new PruneUnusedParameters()
                .getApplicationContext(memo.operation(leafOperation), memo.memo());

        assertThat(context).isEqualTo(RuleApplicationContext.EMPTY);
    }

    @Test
    void testDoNotPruneWhenOperationHasNoChildGroups()
    {
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle leaf = builder.declareGroup("leaf");
                    builder.addOperation(leaf, "leaf", row(rowType(bigint()), List.of(bigint()), parameter(0)));
                    return leaf;
                })
                .doesNotFire();
    }

    @Test
    void testReplaceSourceLeavingOriginalChildGroupUnreachable()
    {
        Type childRow = rowType(bigint());
        GroupHandle[] childHolder = new GroupHandle[1];
        tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    childHolder[0] = child;
                    builder.addOperation(child, "child", row(childRow, List.of(bigint(), bigint()), parameter(0)));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1), List.of()))));
                    return parent;
                })
                .matches(memo("parent")
                        .group("parent", operationPattern()
                                .children(groupEdge("prunedChild", new ParameterLineage(List.of(0), List.of()))))
                        .group("prunedChild", operationPattern()
                                .groupParameterTypes(bigint())
                                .children(parameterEdge(0)))
                        .absentGroup(childHolder[0]));
    }

    @Test
    void testReachFixpoint()
    {
        Type childRow = rowType(bigint());
        TestingMemo result = tester().assertThat(new PruneUnusedParameters())
                .on(builder -> {
                    GroupHandle child = builder.declareGroup("child");
                    GroupHandle parent = builder.declareGroup("parent");
                    builder.addOperation(child, "child", row(childRow, List.of(bigint(), bigint()), parameter(0)));
                    builder.addOperation(parent, "parent", row(
                            rowType(childRow),
                            List.of(bigint(), bigint()),
                            childGroup(child, new ParameterLineage(List.of(0, 1), List.of()))));
                    return parent;
                })
                .appliesUntilFixpoint();

        memo("parent")
                .group("parent", operationPattern()
                        .children(groupEdge("prunedChild", new ParameterLineage(List.of(0), List.of()))))
                .group("prunedChild", operationPattern()
                        .groupParameterTypes(bigint())
                        .children(parameterEdge(0)))
                .matches(result);
    }
}
