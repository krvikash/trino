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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.ChildSpecification;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationSpecification;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoMatcher.groupEdge;
import static io.trino.sql.planner.exploratory.MemoMatcher.groupPattern;
import static io.trino.sql.planner.exploratory.MemoMatcher.memo;
import static io.trino.sql.planner.exploratory.MemoMatcher.operationPattern;
import static io.trino.sql.planner.exploratory.MemoMatcher.parameterEdge;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_DERIVED_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_DERIVED_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_DIALECT_NAME;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_TYPE;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.childGroup;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.parameter;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMemoMatcher
{
    private static final Type ANOTHER_TYPE = new Type(TEST_DIALECT_NAME, new Object());

    @Test
    void testMatchMinimalGraph()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("root"));

        memo("root")
                .group("root", operationPattern().name("root"))
                .matches(testingMemo);
    }

    @Test
    void testFailWhenReachedGroupHasNoPattern()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle child = builder.declareGroup("child");
        builder.addOperation(child, "child", leaf("child"));
        builder.addOperation(root, "root", operation("root", List.of(), childGroup(child, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        // The matcher describes the child edge but declares no pattern for the group it reaches.
        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().children(groupEdge("child")));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("No pattern defined for group child reached at id %s", testingMemo.groupId(child)));
    }

    @Test
    void testFailWhenDeclaredPatternIsNeverReached()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("root"));

        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().name("root"))
                .group("ghost", operationPattern().name("ghost"));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Group patterns declared but never reached from the root: [ghost]");
    }

    @Test
    void testTerminateOnCycle()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle a = builder.declareGroup("a");
        GroupHandle b = builder.declareGroup("b");
        builder.addOperation(a, "a", operation("a", List.of(), childGroup(b, ParameterLineage.identity(0))));
        builder.addOperation(b, "b", operation("b", List.of(), childGroup(a, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(a);

        memo("a")
                .group("a", operationPattern().name("a").children(groupEdge("b")))
                .group("b", operationPattern().name("b").children(groupEdge("a")))
                .matches(testingMemo);
    }

    @Test
    void testMatchDiamondWithConsistentBinding()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle shared = builder.declareGroup("shared");
        builder.addOperation(shared, "shared", leaf("shared"));
        builder.addOperation(root, "root", operation(
                "root",
                List.of(),
                childGroup(shared, ParameterLineage.identity(0)),
                childGroup(shared, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        memo("root")
                .group("root", operationPattern().children(groupEdge("shared"), groupEdge("shared")))
                .group("shared", operationPattern().name("shared"))
                .matches(testingMemo);
    }

    @Test
    void testFailWhenOneNameBindsToTwoGroups()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle left = builder.declareGroup("left");
        GroupHandle right = builder.declareGroup("right");
        builder.addOperation(left, "left", leaf("left"));
        builder.addOperation(right, "right", leaf("right"));
        builder.addOperation(root, "root", operation(
                "root",
                List.of(),
                childGroup(left, ParameterLineage.identity(0)),
                childGroup(right, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        // Both child edges claim the name "child", but they lead to two different groups.
        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().children(groupEdge("child"), groupEdge("child")))
                .group("child", operationPattern());

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Group child expected at id %s but also reached at id %s",
                        testingMemo.groupId(left),
                        testingMemo.groupId(right)));
    }

    @Test
    void testFailWhenTwoNamesBindToOneGroup()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle shared = builder.declareGroup("shared");
        builder.addOperation(shared, "shared", leaf("shared"));
        builder.addOperation(root, "root", operation(
                "root",
                List.of(),
                childGroup(shared, ParameterLineage.identity(0)),
                childGroup(shared, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        // The two edges lead to the same group but are described under different names.
        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().children(groupEdge("x"), groupEdge("y")))
                .group("x", operationPattern())
                .group("y", operationPattern());

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Group id %s matched by both x and y", testingMemo.groupId(shared)));
    }

    @Test
    void testPinGroupIdentityViaHandle()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "root", leaf("root"));
        TestingMemo testingMemo = builder.build(root);

        // Keying the group by the handle asserts identity against the handle's resolved id.
        memo(root)
                .group(root, operationPattern().name("root"))
                .matches(testingMemo);
    }

    @Test
    void testMatchContainsOperationsAndAttributeModes()
    {
        Attributes operationAttributes = attributes(
                TEST_OPERATION_ATTRIBUTE_KEY,
                TEST_OPERATION_ATTRIBUTE_OBJECT,
                TEST_DERIVED_ATTRIBUTE_KEY,
                TEST_DERIVED_ATTRIBUTE_OBJECT);

        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "primary", leaf("primary", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));
        builder.addOperation(root, "alternative", leaf("alternative", Attributes.empty(), ImmutableSet.of()));
        TestingMemo testingMemo = builder.build(root);

        // The group holds two alternatives; containsOperations matches a subset, and the attribute-mode
        // assertions separate the inherent attribute from the derived one.
        memo("root")
                .group("root", groupPattern().containsOperations(
                        operationPattern()
                                .name("primary")
                                .resultType(TEST_TYPE)
                                .containsAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                                .inherentAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                                .derivedAttributes(attributes(TEST_DERIVED_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT))))
                .matches(testingMemo);
    }

    @Test
    void testMatchOperationDialectAndName()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("theOperation"));

        memo("root")
                .group("root", operationPattern().dialect(TEST_DIALECT_NAME).name("theOperation"))
                .matches(testingMemo);
    }

    @Test
    void testFailOnOperationNameMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("actualName"));

        MemoMatcher matcher = memo("root").group("root", operationPattern().name("expectedName"));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected operation name expectedName in group root but found actualName");
    }

    @Test
    void testFailOnOperationDialectMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("op"));

        MemoMatcher matcher = memo("root").group("root", operationPattern().dialect("other_dialect"));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected dialect other_dialect in group root but found test_dialect");
    }

    @Test
    void testFailOnOperationResultTypeMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("op"));

        MemoMatcher matcher = memo("root").group("root", operationPattern().resultType(ANOTHER_TYPE));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Expected result type %s in group root but found %s", ANOTHER_TYPE, TEST_TYPE));
    }

    @Test
    void testMatchOperationGroupParameterTypes()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        memo("root")
                .group("root", operationPattern().groupParameterTypes(TEST_TYPE))
                .matches(testingMemo);
    }

    @Test
    void testFailOnOperationGroupParameterTypesMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        MemoMatcher matcher = memo("root").group("root", operationPattern().groupParameterTypes(ANOTHER_TYPE));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected group parameter types %s in group root but found %s",
                        List.of(ANOTHER_TYPE),
                        List.of(TEST_TYPE)));
    }

    @Test
    void testMatchFullOperationAttributes()
    {
        Attributes operationAttributes = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        memo("root")
                .group("root", operationPattern().attributes(operationAttributes))
                .matches(testingMemo);
    }

    @Test
    void testFailOnFullOperationAttributesMismatch()
    {
        Attributes operationAttributes = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        // Expecting a different value for the same key.
        Attributes expected = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT);
        MemoMatcher matcher = memo("root").group("root", operationPattern().attributes(expected));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Expected attributes %s in group root but found %s", expected, operationAttributes));
    }

    @Test
    void testFailWhenContainsAttributesKeyIsMissing()
    {
        Attributes operationAttributes = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        // Requiring a key the operation does not carry.
        MemoMatcher matcher = memo("root").group("root", operationPattern()
                .containsAttributes(attributes(TEST_DERIVED_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT)));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected attribute %s=%s in group root but found null",
                        TEST_DERIVED_ATTRIBUTE_KEY,
                        TEST_DERIVED_ATTRIBUTE_OBJECT));
    }

    @Test
    void testFailOnInherentAttributesMismatch()
    {
        Attributes operationAttributes = attributes(
                TEST_OPERATION_ATTRIBUTE_KEY,
                TEST_OPERATION_ATTRIBUTE_OBJECT,
                TEST_DERIVED_ATTRIBUTE_KEY,
                TEST_DERIVED_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        // The derived attribute is not part of the inherent subset.
        MemoMatcher matcher = memo("root").group("root", operationPattern()
                .inherentAttributes(attributes(TEST_DERIVED_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT)));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected inherent attributes %s in group root but found %s",
                        attributes(TEST_DERIVED_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT),
                        attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT)));
    }

    @Test
    void testFailOnDerivedAttributesMismatch()
    {
        Attributes operationAttributes = attributes(
                TEST_OPERATION_ATTRIBUTE_KEY,
                TEST_OPERATION_ATTRIBUTE_OBJECT,
                TEST_DERIVED_ATTRIBUTE_KEY,
                TEST_DERIVED_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        // The inherent attribute is not part of the derived subset.
        MemoMatcher matcher = memo("root").group("root", operationPattern()
                .derivedAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT)));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected derived attributes %s in group root but found %s",
                        attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT),
                        attributes(TEST_DERIVED_ATTRIBUTE_KEY, TEST_DERIVED_ATTRIBUTE_OBJECT)));
    }

    @Test
    void testIgnoreUnspecifiedOperationFields()
    {
        Attributes operationAttributes = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        TestingMemo testingMemo = singleGroup("root", leaf("op", operationAttributes, ImmutableSet.of(TEST_OPERATION_ATTRIBUTE_KEY)));

        // Only the name is asserted; dialect, attributes, children etc. are left unchecked.
        memo("root")
                .group("root", operationPattern().name("op"))
                .matches(testingMemo);
    }

    @Test
    void testMatchParameterEdgeIndex()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        memo("root")
                .group("root", operationPattern().children(parameterEdge(0)))
                .matches(testingMemo);
    }

    @Test
    void testFailOnParameterEdgeIndexMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        MemoMatcher matcher = memo("root").group("root", operationPattern().children(parameterEdge(1)));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected parameter index 1 in group root but found 0");
    }

    @Test
    void testFailOnChildCountMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        MemoMatcher matcher = memo("root").group("root", operationPattern().children(parameterEdge(0), parameterEdge(0)));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Operation in group root expected 2 children but found 1");
    }

    @Test
    void testFailWhenParameterEdgeMatchesGroupChild()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle child = builder.declareGroup("child");
        builder.addOperation(child, "child", leaf("child"));
        builder.addOperation(root, "root", operation("root", List.of(), childGroup(child, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        MemoMatcher matcher = memo("root").group("root", operationPattern().children(parameterEdge(0)));

        // The message ends with the actual child's rendering, which carries a resolved id; assert the phrase up to it.
        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected parameter child in group root but found GroupChild");
    }

    @Test
    void testFailWhenGroupEdgeMatchesParameterChild()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        MemoMatcher matcher = memo("root").group("root", operationPattern().children(groupEdge("x")));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected group child in group root but found group_param_0");
    }

    @Test
    void testMatchGroupEdgeLineage()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle child = builder.declareGroup("child");
        builder.addOperation(child, "child", leaf("child"));
        builder.addOperation(root, "root", operation("root", List.of(TEST_TYPE), childGroup(child, new ParameterLineage(List.of(0), List.of()))));
        TestingMemo testingMemo = builder.build(root);

        memo("root")
                .group("root", operationPattern().children(groupEdge("child", new ParameterLineage(List.of(0), List.of()))))
                .group("child", operationPattern().name("child"))
                .matches(testingMemo);
    }

    @Test
    void testFailOnGroupEdgeLineageMismatch()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle child = builder.declareGroup("child");
        builder.addOperation(child, "child", leaf("child"));
        builder.addOperation(root, "root", operation("root", List.of(TEST_TYPE), childGroup(child, new ParameterLineage(List.of(0), List.of()))));
        TestingMemo testingMemo = builder.build(root);

        // Expecting an empty lineage where the actual edge passes group parameter 0.
        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().children(groupEdge("child", new ParameterLineage(List.of(), List.of()))));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected parameter lineage [],[] in group root but found [group_param_0],[]");
    }

    @Test
    void testFailOnGroupResultTypeMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("op"));

        MemoMatcher matcher = memo("root").group("root", groupPattern().resultType(ANOTHER_TYPE));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Expected result type %s in group root but found %s", ANOTHER_TYPE, TEST_TYPE));
    }

    @Test
    void testFailOnGroupParameterTypesMismatch()
    {
        TestingMemo testingMemo = singleGroup("root", operation("op", List.of(TEST_TYPE), parameter(0)));

        MemoMatcher matcher = memo("root").group("root", groupPattern().groupParameterTypes(ANOTHER_TYPE));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected group parameter types %s in group root but found %s",
                        List.of(ANOTHER_TYPE),
                        List.of(TEST_TYPE)));
    }

    @Test
    void testFailOnGroupAttributesMismatch()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "op", leaf("op"));
        TestingMemo testingMemo = builder.build(root);

        // The leaf carries no attributes, so the group's derived attributes cannot equal this non-empty expectation.
        Attributes expected = attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        MemoMatcher matcher = memo("root").group("root", groupPattern().attributes(expected));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected group attributes %s in group root but found %s",
                        expected,
                        testingMemo.group(root).attributes()));
    }

    @Test
    void testFailOnExactOperationCountMismatch()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "op", leaf("op"));
        TestingMemo testingMemo = builder.build(root);

        MemoMatcher matcher = memo("root").group("root", groupPattern().operations(operationPattern(), operationPattern()));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Group root (id %s) expected 2 operations but found 1", testingMemo.groupId(root)));
    }

    @Test
    void testFailWhenContainsOperationsCannotBeMatchedToDistinctOperations()
    {
        TestingMemo testingMemo = singleGroup("root", leaf("op"));

        // Two contained patterns require two distinct operations, but the group holds only one.
        MemoMatcher matcher = memo("root").group("root", groupPattern().containsOperations(
                operationPattern().name("op"),
                operationPattern().name("op")));

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Group root has no unmatched operation matching contained operation #1");
    }

    @Test
    void testMatchAbsentGroupWhenUnreachable()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle orphan = builder.declareGroup("orphan");
        builder.addOperation(root, "root", leaf("root"));
        builder.addOperation(orphan, "orphan", leaf("orphan"));
        TestingMemo testingMemo = builder.build(root);

        // The orphan group is never referenced from the root, so it is absent from the plan.
        memo("root")
                .group("root", operationPattern().name("root"))
                .absentGroup(orphan)
                .matches(testingMemo);
    }

    @Test
    void testFailWhenAbsentGroupIsReachable()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "root", leaf("root"));
        TestingMemo testingMemo = builder.build(root);

        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().name("root"))
                .absentGroup(root);

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format("Expected group root (id %s) to be absent, but it is reachable from the root", testingMemo.groupId(root)));
    }

    @Test
    void testMatchSameGroupWhenMerged()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle a = builder.declareGroup("a");
        GroupHandle b = builder.declareGroup("b");
        builder.addOperation(root, "root", leaf("root"));
        builder.addOperation(a, "a", leaf("a"));
        builder.addOperation(b, "b", leaf("b"));
        TestingMemo testingMemo = builder.build(root);

        // Merge b into a so both handles resolve to one group.
        testingMemo.memo().mergeGroups(testingMemo.groupId(a), testingMemo.groupId(b), new MemoChangesCollector());

        memo("root")
                .group("root", operationPattern().name("root"))
                .sameGroup(a, b)
                .matches(testingMemo);
    }

    @Test
    void testFailWhenSameGroupIsNotMerged()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle a = builder.declareGroup("a");
        GroupHandle b = builder.declareGroup("b");
        builder.addOperation(root, "root", leaf("root"));
        builder.addOperation(a, "a", leaf("a"));
        builder.addOperation(b, "b", leaf("b"));
        TestingMemo testingMemo = builder.build(root);

        MemoMatcher matcher = memo("root")
                .group("root", operationPattern().name("root"))
                .sameGroup(a, b);

        assertThatThrownBy(() -> matcher.matches(testingMemo))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(format(
                        "Expected groups a and b to be merged into one, but they resolve to ids %s and %s",
                        testingMemo.groupId(a),
                        testingMemo.groupId(b)));
    }

    private static TestingMemo singleGroup(String name, OperationSpecification operation)
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle group = builder.declareGroup(name);
        builder.addOperation(group, name, operation);
        return builder.build(group);
    }

    /**
     * A childless fake operation with no group parameters.
     */
    private static OperationSpecification leaf(String name)
    {
        return leaf(name, Attributes.empty(), ImmutableSet.of());
    }

    private static OperationSpecification leaf(String name, Attributes attributes, Set<AttributeKey> inherentOperationAttributeKeys)
    {
        return OperationSpecification.operation(TEST_DIALECT_NAME, new OperationId(name, ImmutableList.of(), ImmutableList.of()), TEST_TYPE)
                .attributes(attributes)
                .inherentOperationAttributeKeys(inherentOperationAttributeKeys);
    }

    /**
     * A fake operation whose argument children are the given child specs. One {@code TEST_TYPE} argument type is
     * declared per child; group parameters referenced by any {@code parameter(index)} child must be
     * {@code TEST_TYPE} for the operation to be well-formed.
     */
    private static OperationSpecification operation(String name, List<Type> groupParameterTypes, ChildSpecification... children)
    {
        List<Type> argumentTypes = Collections.nCopies(children.length, TEST_TYPE);
        return OperationSpecification.operation(TEST_DIALECT_NAME, new OperationId(name, ImmutableList.copyOf(argumentTypes), ImmutableList.of()), TEST_TYPE)
                .groupParameterTypes(groupParameterTypes)
                .children(children)
                .inherentOperationAttributeKeys(ImmutableSet.of());
    }
}
