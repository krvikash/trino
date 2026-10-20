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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.dialect.trino.operationmetadata.RowOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupAttributesChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupMerge;
import io.trino.sql.planner.exploratory.MemoChangesCollector.GroupPrune;
import io.trino.sql.planner.exploratory.MemoChangesCollector.MemoChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationAttributesChange;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationInsert;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationRemove;
import io.trino.sql.planner.exploratory.MemoChangesCollector.OperationSuccession;
import io.trino.sql.planner.exploratory.MemoChangesCollector.SingletonGroupCreate;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.rule.Context;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.Result;
import io.trino.sql.planner.exploratory.rule.UpdateOperationAttributes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoGroupMatcher.memoGroup;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.GroupChildMatcher.groupChild;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.ParameterChildMatcher.parameterChild;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.memoOperation;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_MEMO_OPERATION;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_TYPE;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.singleConstantRow;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroupsContains;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroupsDoesNotContain;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static io.trino.sql.planner.exploratory.rule.NewMemoOperation.operationLike;
import static io.trino.sql.planner.exploratory.rule.Patterns.operation;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMemoChangesCollector
{
    @Test
    public void testSingletonGroupCreate()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation operation = leafOperation();
        int groupId = memo.insertOperation(operation, collector);

        assertThat(collector.getChanges())
                .containsExactly(new SingletonGroupCreate(operation, groupId));
    }

    @Test
    public void testGroupMerge()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation firstOperation = leafOperation();
        int firstGroupId = memo.insertOperation(firstOperation, collector);
        MemoOperation secondOperation = leafOperationWithDialect("another_dialect");
        int secondGroupId = memo.insertOperation(secondOperation, collector);
        memo.mergeGroups(firstGroupId, secondGroupId, collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(firstOperation, firstGroupId),
                        new SingletonGroupCreate(secondOperation, secondGroupId),
                        new GroupMerge(firstGroupId, secondGroupId));
    }

    @Test
    public void testGroupMergeWithAttributesChange()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation firstOperation = leafOperation();
        int firstGroupId = memo.insertOperation(firstOperation, collector);
        MemoOperation secondOperation = MemoOperationBuilder.from(leafOperationWithDialect("another_dialect"))
                .withAttributes(attributes(
                        TEST_OPERATION_ATTRIBUTE_KEY,
                        TEST_OPERATION_ATTRIBUTE_OBJECT,
                        new AttributeKey(IR, SAFE),
                        TRUE))
                .build();
        int secondGroupId = memo.insertOperation(secondOperation, collector);
        memo.mergeGroups(firstGroupId, secondGroupId, collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(firstOperation, firstGroupId),
                        new SingletonGroupCreate(secondOperation, secondGroupId),
                        new GroupMerge(firstGroupId, secondGroupId),
                        new GroupAttributesChange(firstGroupId));
    }

    @Test
    public void testMergeGroupIntoItself()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation operation = leafOperation();
        int groupId = memo.insertOperation(operation, collector);
        memo.mergeGroups(groupId, groupId, collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(operation, groupId));
    }

    @Test
    public void testRecursiveGroupMerge()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();

        MemoOperation survivingOperation = leafOperation();
        int survivingGroupId = memo.insertOperation(survivingOperation, ignoredChanges);

        MemoOperation removedOperation = leafOperationWithDialect("another_dialect");
        int removedGroupId = memo.insertOperation(removedOperation, ignoredChanges);

        MemoOperation uniqueParentOverRemoved = singleChildOperation("unique_parent", removedGroupId);
        int uniqueParentGroupId = memo.insertOperation(uniqueParentOverRemoved, ignoredChanges);

        MemoOperation deduplicatedParentOverRemoved = singleChildOperation("deduplicated_parent", removedGroupId);
        int deduplicatedParentRemovedGroupId = memo.insertOperation(deduplicatedParentOverRemoved, ignoredChanges);

        MemoOperation deduplicatedParentOverSurviving = singleChildOperation("deduplicated_parent", survivingGroupId);
        int deduplicatedParentSurvivingGroupId = memo.insertOperation(deduplicatedParentOverSurviving, ignoredChanges);

        MemoOperation rewrittenUniqueParent = singleChildOperation("unique_parent", survivingGroupId);

        MemoChangesCollector collector = new MemoChangesCollector();
        memo.mergeGroups(survivingGroupId, removedGroupId, collector);

        assertThat(collector.getChanges()).containsExactly(
                new GroupMerge(survivingGroupId, removedGroupId),
                // update parent operation in its group -- no recursive merge
                new OperationInsert(rewrittenUniqueParent, uniqueParentGroupId),
                new OperationSuccession(uniqueParentOverRemoved, rewrittenUniqueParent, uniqueParentGroupId),
                new OperationRemove(uniqueParentOverRemoved, uniqueParentGroupId),
                // update parent operation with recursive merge
                new GroupMerge(deduplicatedParentSurvivingGroupId, deduplicatedParentRemovedGroupId),
                new OperationSuccession(deduplicatedParentOverRemoved, deduplicatedParentOverSurviving, deduplicatedParentSurvivingGroupId),
                new OperationRemove(deduplicatedParentOverRemoved, deduplicatedParentSurvivingGroupId));

        memo.validateGroupToParentsMapping();
        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        survivingGroupId, memoGroup()
                                .withOperations(
                                        memoOperation()
                                                .withName("test_operation")
                                                .build(),
                                        memoOperation()
                                                .withName("test_operation")
                                                .build())
                                .build(),
                        uniqueParentGroupId, memoGroup()
                                .withOperations(
                                        memoOperation()
                                                .withName("unique_parent")
                                                .withChildren(groupChild(survivingGroupId))
                                                .build())
                                .build(),
                        deduplicatedParentSurvivingGroupId, memoGroup()
                                .withOperations(
                                        memoOperation()
                                                .withName("deduplicated_parent")
                                                .withChildren(groupChild(survivingGroupId))
                                                .build())
                                .build()));
        assertMemoGroupsDoesNotContain(memo.groups(), removedGroupId, deduplicatedParentRemovedGroupId);
    }

    @Test
    public void testGroupPrune()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();
        MemoOperation orphanOperation = leafOperation();
        int orphanGroupId = memo.insertOperation(orphanOperation, ignoredChanges);

        assertThat(memo.pruneOrphanedGroups())
                .containsExactly(new GroupPrune(orphanGroupId, ImmutableList.of(orphanOperation)));
        assertMemoGroupsDoesNotContain(memo.groups(), orphanGroupId);
    }

    @Test
    public void testAttributesChange()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation operation = leafOperationWithDerivedAttributes(
                attributes(new AttributeKey(IR, REPEATABILITY), DETERMINISTIC));
        int groupId = memo.insertOperation(operation, collector);
        MemoOperation strengthenedOperation = leafOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, SAFE),
                TRUE));
        memo.insertOperation(strengthenedOperation, collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(operation, groupId),
                        new OperationAttributesChange(operation, strengthenedOperation, groupId),
                        new GroupAttributesChange(groupId));
    }

    @Test
    public void testOperationRemove()
    {
        Memo memo = baseMemo();
        MemoChangesCollector collector = new MemoChangesCollector();

        // create a group with two operations so that one can be removed
        MemoOperation firstOperation = leafOperation();
        int firstGroupId = memo.insertOperation(firstOperation, collector);
        MemoOperation secondOperation = leafOperationWithDialect("another_dialect");
        int secondGroupId = memo.insertOperation(secondOperation, collector);
        memo.mergeGroups(firstGroupId, secondGroupId, collector);
        memo.removeOperation(secondOperation, collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(firstOperation, firstGroupId),
                        new SingletonGroupCreate(secondOperation, secondGroupId),
                        new GroupMerge(firstGroupId, secondGroupId),
                        // after group merge, both operations are in group with id=firstGroupId
                        new OperationRemove(secondOperation, firstGroupId));

        // removing the same operation again via removeOperation() fails
        assertThatThrownBy(() -> memo.removeOperation(secondOperation, collector))
                .hasMessageContaining("Removed operation not found in Memo");
    }

    @Test
    public void testRuleApplicationDoesNotPruneOrphanedGroups()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();
        MemoOperation operation = leafOperation();
        int groupId = memo.insertOperation(operation, ignoredChanges);

        List<MemoChange> changes = new RuleApplier(memo).applyRule(operation, new UpdateOperationAttributes());

        assertThat(changes)
                .isEmpty();

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        groupId, memoGroup()
                                .withOperations(
                                        memoOperation()
                                                .withName(TEST_MEMO_OPERATION.operationId().name())
                                                .withAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                                                .build())
                                .build()));
    }

    @Test
    public void testRuleApplication()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();
        MemoOperation childOperation = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 0);
        int childGroupId = memo.insertOperation(childOperation, ignoredChanges);
        MemoOperation parentOperation = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE),
                childGroupId,
                new ParameterLineage(ImmutableList.of(0, 1), ImmutableList.of()));
        int parentGroupId = memo.insertOperation(parentOperation, ignoredChanges);

        MemoOperation expectedRewrittenChild = parametrizedOperation(ImmutableList.of(TEST_TYPE), 0);
        int expectedRewrittenChildGroupId = memo.size();
        MemoOperation expectedRewrittenParent = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE),
                expectedRewrittenChildGroupId,
                new ParameterLineage(ImmutableList.of(0), ImmutableList.of()));
        int expectedRewrittenParentGroupId = expectedRewrittenChildGroupId + 1;

        List<MemoChange> changes = new RuleApplier(memo).applyRule(
                parentOperation,
                new TestingRule(expectedRewrittenParent, expectedRewrittenChild));

        assertThat(changes).containsExactly(
                new SingletonGroupCreate(expectedRewrittenChild, expectedRewrittenChildGroupId),
                new SingletonGroupCreate(expectedRewrittenParent, expectedRewrittenParentGroupId),
                new GroupMerge(parentGroupId, expectedRewrittenParentGroupId),
                new OperationRemove(parentOperation, parentGroupId));

        assertMemoGroupsContains(
                memo.groups(),
                ImmutableMap.of(
                        expectedRewrittenChildGroupId, memoGroup()
                                .withGroupParameterTypes(TEST_TYPE)
                                .withOperations(
                                        memoOperation()
                                                .withName(RowOperationMetadata.NAME)
                                                .withGroupParameterTypes(TEST_TYPE)
                                                .withChildren(parameterChild(0))
                                                .build())
                                .build(),
                        parentGroupId, memoGroup()
                                .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                                .withOperations(
                                        memoOperation()
                                                .withName(RowOperationMetadata.NAME)
                                                .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                                                .withChildren(groupChild(expectedRewrittenChildGroupId))
                                                .build())
                                .build()));
    }

    @Test
    public void testDirectlyRecursiveRuleResult()
    {
        Memo memo = baseMemo();
        MemoOperation rootOperation = memo.getGroup(memo.rootGroup()).operations().getFirst();

        assertThatThrownBy(() -> new RuleApplier(memo).applyRule(rootOperation, new DirectlyRecursiveRule()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Rule %s produced a cyclic NewMemoOperation graph", DirectlyRecursiveRule.class.getName());
    }

    @Test
    public void testIndirectlyRecursiveRuleResult()
    {
        Memo memo = baseMemo();
        MemoOperation rootOperation = memo.getGroup(memo.rootGroup()).operations().getFirst();

        assertThatThrownBy(() -> new RuleApplier(memo).applyRule(rootOperation, new IndirectlyRecursiveRule()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Rule %s produced a cyclic NewMemoOperation graph", IndirectlyRecursiveRule.class.getName());
    }

    @Test
    public void testRecursiveRuleResultDoesNotModifyMemo()
    {
        ParentChildMemo parentChildMemo = parentChildMemo();
        Memo memo = parentChildMemo.memo();
        int memoSize = memo.size();

        // the valid sibling precedes the cyclic one, so inserting while traversing would modify Memo before detecting the cycle
        MemoOperation validSibling = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 1);
        assertThatThrownBy(() -> new RuleApplier(memo).applyRule(parentChildMemo.parentOperation(), new CyclicSiblingRule(validSibling)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Rule %s produced a cyclic NewMemoOperation graph", CyclicSiblingRule.class.getName());

        assertThat(memo.size()).isEqualTo(memoSize);
        assertThat(memo.getGroupId(validSibling)).isEmpty();
        assertThat(memo.getGroupId(parentChildMemo.childOperation())).hasValue(parentChildMemo.childGroupId());
        assertThat(memo.getGroupId(parentChildMemo.parentOperation())).hasValue(parentChildMemo.parentGroupId());
    }

    @Test
    public void testSharedNewMemoOperation()
    {
        ParentChildMemo parentChildMemo = parentChildMemo();
        Memo memo = parentChildMemo.memo();
        MemoOperation parentOperation = parentChildMemo.parentOperation();
        int parentGroupId = parentChildMemo.parentGroupId();

        MemoOperation sharedChild = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 1);
        int expectedSharedChildGroupId = memo.size();
        MemoOperation expectedParent = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE),
                expectedSharedChildGroupId,
                new ParameterLineage(ImmutableList.of(0, 1), ImmutableList.of()));
        int expectedParentGroupId = expectedSharedChildGroupId + 1;

        List<MemoChange> changes = new RuleApplier(memo).applyRule(parentOperation, new SharedOperationRule(sharedChild));

        // the shared operation is inserted once, and its group is not merged with itself
        assertThat(changes).containsExactly(
                new SingletonGroupCreate(sharedChild, expectedSharedChildGroupId),
                new SingletonGroupCreate(expectedParent, expectedParentGroupId),
                new GroupMerge(parentGroupId, expectedParentGroupId));
    }

    @Test
    @Timeout(10)
    public void testDeeplySharedNewMemoOperationDoesNotExpandExponentially()
    {
        ParentChildMemo parentChildMemo = parentChildMemo();

        new RuleApplier(parentChildMemo.memo()).applyRule(
                parentChildMemo.parentOperation(),
                new DeeplySharedOperationRule(parentChildMemo.childOperation(), 40));

        parentChildMemo.memo().validateGroupToParentsMapping();
    }

    @Test
    public void testSharedNewMemoOperationIsRefreshedAfterMergeSuccession()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();
        MemoOperation existingChild = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 0);
        int childGroupId = memo.insertOperation(existingChild, ignoredChanges);
        MemoOperation sourceOperation = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE, TEST_TYPE),
                childGroupId,
                new ParameterLineage(ImmutableList.of(0, 1), ImmutableList.of()));
        int sourceGroupId = memo.insertOperation(sourceOperation, ignoredChanges);
        MemoOperation sharedParent = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE),
                childGroupId,
                new ParameterLineage(ImmutableList.of(0, 1), ImmutableList.of()));
        int sharedParentGroupId = memo.insertOperation(sharedParent, ignoredChanges);
        MemoOperation sharedChild = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 1);

        List<MemoChange> changes = new RuleApplier(memo).applyRule(
                sourceOperation,
                new SharedOperationMergedBeforeReuseRule(sharedChild, existingChild, sharedParent));

        assertThat(changes)
                .anySatisfy(change -> assertThat(change)
                        .isInstanceOfSatisfying(OperationSuccession.class, succession -> {
                            assertThat(succession.newOperation()).isEqualTo(sharedParent);
                            assertThat(succession.groupId()).isEqualTo(sharedParentGroupId);
                        }));
        int recentSourceGroupId = memo.getRecentId(sourceGroupId);
        MemoGroup sourceGroup = memo.getGroup(recentSourceGroupId);
        assertThat(sourceGroup.getChildGroups()).doesNotContain(recentSourceGroupId);
        memo.validateGroupToParentsMapping();
    }

    private static Memo baseMemo()
    {
        return Memo.forProgram(program(valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)))), TESTING_DIALECT_REGISTRY);
    }

    private static ParentChildMemo parentChildMemo()
    {
        Memo memo = baseMemo();
        MemoChangesCollector ignoredChanges = new MemoChangesCollector();
        MemoOperation childOperation = parametrizedOperation(ImmutableList.of(TEST_TYPE, TEST_TYPE), 0);
        int childGroupId = memo.insertOperation(childOperation, ignoredChanges);
        MemoOperation parentOperation = parametrizedOperationWithChild(
                ImmutableList.of(TEST_TYPE, TEST_TYPE),
                childGroupId,
                new ParameterLineage(ImmutableList.of(0, 1), ImmutableList.of()));
        int parentGroupId = memo.insertOperation(parentOperation, ignoredChanges);
        return new ParentChildMemo(memo, childOperation, childGroupId, parentOperation, parentGroupId);
    }

    private record ParentChildMemo(
            Memo memo,
            MemoOperation childOperation,
            int childGroupId,
            MemoOperation parentOperation,
            int parentGroupId) {}

    private static MemoOperation leafOperation()
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withOperationId(new OperationId(TEST_MEMO_OPERATION.operationId().name(), ImmutableList.of(), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withChildren(ImmutableList.of())
                .withAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                .build();
    }

    private static MemoOperation leafOperationWithDerivedAttributes(Attributes derivedAttributes)
    {
        return MemoOperationBuilder.from(leafOperation())
                .withAttributes(Attributes.builder()
                        .putAll(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                        .putAll(derivedAttributes)
                        .buildOrThrow())
                .build();
    }

    private static MemoOperation leafOperationWithDialect(String dialectName)
    {
        return MemoOperationBuilder.from(leafOperation())
                .withDialect(dialectName)
                .build();
    }

    private static MemoOperation singleChildOperation(String name, int childGroupId)
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withOperationId(new OperationId(name, ImmutableList.of(TEST_MEMO_OPERATION.resultType()), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withChildren(ImmutableList.of(new MemoOperation.GroupChild(childGroupId, ParameterLineage.identity(0))))
                .build();
    }

    private static MemoOperation parametrizedOperation(List<Type> groupParameterTypes, int parameterIndex)
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("trino")
                .withOperationId(new OperationId(RowOperationMetadata.NAME, ImmutableList.of(TEST_TYPE), ImmutableList.of()))
                .withResultType(TEST_TYPE)
                .withGroupParameterTypes(groupParameterTypes)
                .withChildren(ImmutableList.of(new ParameterChild(parameterIndex)))
                .withAttributes(Attributes.empty())
                .withInherentOperationAttributeKeys(ImmutableSet.of())
                .build();
    }

    private static MemoOperation parametrizedOperationWithChild(List<Type> groupParameterTypes, int childGroupId, ParameterLineage parameterLineage)
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("trino")
                .withOperationId(new OperationId(RowOperationMetadata.NAME, ImmutableList.of(TEST_TYPE), ImmutableList.of()))
                .withResultType(TEST_TYPE)
                .withGroupParameterTypes(groupParameterTypes)
                .withChildren(ImmutableList.of(new MemoOperation.GroupChild(childGroupId, parameterLineage)))
                .withAttributes(Attributes.empty())
                .withInherentOperationAttributeKeys(ImmutableSet.of())
                .build();
    }

    private record TestingRule(MemoOperation rewrittenParent, MemoOperation rewrittenChild)
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ((GroupChild) rewrittenParent.children().getFirst()).parameterLineage();
            return Result.of(operationLike(rewrittenParent)
                    .withChild(0, operationLike(rewrittenChild), parameterLineage));
        }

        @Override
        public boolean canReplaceSource()
        {
            return true;
        }
    }

    private record DirectlyRecursiveRule()
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ParameterLineage.identity(operation.groupParameterTypes().size());
            NewMemoOperation newOperation = operationLike(operation);
            return Result.of(newOperation.withChild(0, newOperation, parameterLineage));
        }
    }

    private record IndirectlyRecursiveRule()
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ParameterLineage.identity(operation.groupParameterTypes().size());
            NewMemoOperation firstOperation = operationLike(operation);
            NewMemoOperation secondOperation = operationLike(operation);
            firstOperation.withChild(0, secondOperation, parameterLineage);
            secondOperation.withChild(0, firstOperation, parameterLineage);
            return Result.of(firstOperation);
        }
    }

    private record CyclicSiblingRule(MemoOperation validSibling)
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ((GroupChild) operation.children().getFirst()).parameterLineage();
            NewMemoOperation parent = operationLike(operation);
            return Result.of(parent.withChildGroup(0, ImmutableList.of(operationLike(validSibling), parent), parameterLineage));
        }
    }

    private record SharedOperationRule(MemoOperation sharedChild)
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ((GroupChild) operation.children().getFirst()).parameterLineage();
            NewMemoOperation sharedNewOperation = operationLike(sharedChild);
            return Result.of(operationLike(operation)
                    .withChildGroup(0, ImmutableList.of(sharedNewOperation, sharedNewOperation), parameterLineage));
        }
    }

    private record DeeplySharedOperationRule(MemoOperation leafOperation, int depth)
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ((GroupChild) operation.children().getFirst()).parameterLineage();
            NewMemoOperation sharedOperation = operationLike(leafOperation);
            for (int level = 0; level < depth; level++) {
                sharedOperation = operationLike(operation)
                        .withChildGroup(0, ImmutableList.of(sharedOperation, sharedOperation), parameterLineage);
            }
            return Result.of(sharedOperation);
        }
    }

    private record SharedOperationMergedBeforeReuseRule(MemoOperation sharedChild, MemoOperation existingChild, MemoOperation sharedParent)
            implements OperationRule
    {
        @Override
        public Pattern<MemoOperation> pattern()
        {
            return operation();
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            ParameterLineage parameterLineage = ((GroupChild) operation.children().getFirst()).parameterLineage();
            NewMemoOperation sharedNewOperation = operationLike(sharedChild);
            NewMemoOperation sharedParentOperation = operationLike(sharedParent)
                    .withChild(0, sharedNewOperation, parameterLineage);
            NewMemoOperation mergingSibling = operationLike(sharedParent)
                    .withChildGroup(0, ImmutableList.of(operationLike(existingChild), sharedNewOperation), parameterLineage);
            return Result.of(operationLike(operation)
                    .withChildGroup(0, ImmutableList.of(sharedParentOperation, mergingSibling, sharedParentOperation), parameterLineage));
        }
    }
}
