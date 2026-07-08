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
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.ChildSpecification;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.GroupHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationHandle;
import io.trino.sql.planner.exploratory.TestingMemoBuilder.OperationSpecification;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_DIALECT_NAME;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_TYPE;
import static io.trino.sql.planner.exploratory.TestingMemoBuilder.childGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTestingMemoBuilder
{
    private static final Type ANOTHER_TYPE = new Type(TEST_DIALECT_NAME, new Object());

    @Test
    void testBuildSingleGroupMemo()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        OperationHandle rootOperation = builder.addOperation(root, "rootOperation", leaf("rootLeaf"));
        TestingMemo testingMemo = builder.build(root);

        assertThat(testingMemo.root()).isEqualTo(root);
        assertThat(testingMemo.memo().size()).isEqualTo(1);
        assertThat(testingMemo.memo().rootGroup()).isEqualTo(testingMemo.groupId(root));
        assertThat(testingMemo.group(root).operations()).containsExactly(testingMemo.operation(rootOperation));
        assertThat(testingMemo.operation(rootOperation).operationId().name()).isEqualTo("rootLeaf");
        assertThat(testingMemo.memo().getGroupId(testingMemo.operation(rootOperation))).hasValue(testingMemo.groupId(root));
    }

    @Test
    void testRejectGroupWithoutOperations()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.declareGroup("orphan");
        builder.addOperation(root, "rootOperation", leaf("rootLeaf"));

        assertThatThrownBy(() -> builder.build(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Group has no operations: orphan");
    }

    @Test
    void testRejectDuplicateGroupName()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        builder.declareGroup("group");

        assertThatThrownBy(() -> builder.declareGroup("group"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Group already declared: group");
    }

    @Test
    void testRejectOperationForGroupFromAnotherBuilder()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        TestingMemoBuilder otherBuilder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle foreignGroup = otherBuilder.declareGroup("group");

        assertThatThrownBy(() -> builder.addOperation(foreignGroup, "operation", leaf("leaf")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Group not declared on this builder: group");
    }

    @Test
    void testRejectRootFromAnotherBuilder()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "rootOperation", leaf("rootLeaf"));
        TestingMemoBuilder otherBuilder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle foreignRoot = otherBuilder.declareGroup("root");

        assertThatThrownBy(() -> builder.build(foreignRoot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Root group not declared on this builder: root");
    }

    @Test
    void testKeepMultipleOperationsPerGroupInOrder()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        OperationHandle first = builder.addOperation(root, "first", leaf("alpha"));
        OperationHandle second = builder.addOperation(root, "second", leaf("beta"));
        TestingMemo testingMemo = builder.build(root);

        assertThat(testingMemo.group(root).operations())
                .containsExactly(testingMemo.operation(first), testingMemo.operation(second));
    }

    @Test
    void testRejectIncompatibleAlternatives()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        builder.addOperation(root, "first", leaf("alpha"));
        builder.addOperation(root, "second", leafOfType("beta", ANOTHER_TYPE));

        assertThatThrownBy(() -> builder.build(root))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the added operation must have the same result type as the group");
    }

    @Test
    void testResolveChildEdgeToChildGroupId()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle child = builder.declareGroup("child");
        GroupHandle parent = builder.declareGroup("parent");
        builder.addOperation(child, "child", leaf("childLeaf"));
        OperationHandle parentOperation = builder.addOperation(parent, "parent", operation("parentOperation", List.of(), childGroup(child, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(parent);

        GroupChild childEdge = (GroupChild) testingMemo.operation(parentOperation).children().getFirst();
        assertThat(childEdge.groupId()).isEqualTo(testingMemo.groupId(child));
        assertThat(testingMemo.memo().size()).isEqualTo(2);
        assertThat(testingMemo.memo().reachablePostOrder()).contains(testingMemo.groupId(child));
    }

    @Test
    void testBuildCyclicMemo()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle a = builder.declareGroup("a");
        GroupHandle b = builder.declareGroup("b");
        OperationHandle aOperation = builder.addOperation(a, "a", operation("aOperation", List.of(), childGroup(b, ParameterLineage.identity(0))));
        OperationHandle bOperation = builder.addOperation(b, "b", operation("bOperation", List.of(), childGroup(a, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(a);

        GroupChild aEdge = (GroupChild) testingMemo.operation(aOperation).children().getFirst();
        GroupChild bEdge = (GroupChild) testingMemo.operation(bOperation).children().getFirst();
        assertThat(aEdge.groupId()).isEqualTo(testingMemo.groupId(b));
        assertThat(bEdge.groupId()).isEqualTo(testingMemo.groupId(a));
        assertThat(testingMemo.memo().size()).isEqualTo(2);
    }

    @Test
    void testResolveHandlesThroughMerge()
    {
        TestingMemoBuilder builder = new TestingMemoBuilder(TESTING_DIALECT_REGISTRY);
        GroupHandle root = builder.declareGroup("root");
        GroupHandle child1 = builder.declareGroup("child1");
        GroupHandle child2 = builder.declareGroup("child2");
        builder.addOperation(child1, "c1", leaf("c1Leaf"));
        builder.addOperation(child2, "c2", leaf("c2Leaf"));
        OperationHandle rootOperation = builder.addOperation(root, "root", operation("rootOperation", List.of(), childGroup(child1, ParameterLineage.identity(0))));
        TestingMemo testingMemo = builder.build(root);

        int survivingId = testingMemo.groupId(child2);
        // Merge child1 into child2; the root edge referencing child1 is remapped, producing a new root operation version.
        testingMemo.memo().mergeGroups(testingMemo.groupId(child2), testingMemo.groupId(child1), new MemoChangesCollector());

        assertThat(testingMemo.groupId(child1)).isEqualTo(survivingId);
        GroupChild rootEdge = (GroupChild) testingMemo.operation(rootOperation).children().getFirst();
        assertThat(rootEdge.groupId()).isEqualTo(survivingId);
    }

    private static OperationSpecification leaf(String name)
    {
        return leafOfType(name, TEST_TYPE);
    }

    private static OperationSpecification leafOfType(String name, Type resultType)
    {
        return OperationSpecification.operation(TEST_DIALECT_NAME, new OperationId(name, ImmutableList.of(), ImmutableList.of()), resultType)
                .attributes(Attributes.empty())
                .inherentOperationAttributeKeys(ImmutableSet.of());
    }

    private static OperationSpecification operation(String name, List<Type> groupParameterTypes, ChildSpecification... children)
    {
        List<Type> argumentTypes = Collections.nCopies(children.length, TEST_TYPE);
        return OperationSpecification.operation(TEST_DIALECT_NAME, new OperationId(name, ImmutableList.copyOf(argumentTypes), ImmutableList.of()), TEST_TYPE)
                .groupParameterTypes(groupParameterTypes)
                .children(children)
                .inherentOperationAttributeKeys(ImmutableSet.of());
    }
}
