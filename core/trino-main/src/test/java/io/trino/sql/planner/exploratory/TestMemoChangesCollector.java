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
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoGroupMatcher.memoGroup;
import static io.trino.sql.planner.exploratory.MemoOperationMatcher.GroupChildMatcher.groupChild;
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
        MemoChangesCollector collector = new MemoChangesCollector();
        MemoOperation orphanOperation = leafOperation();
        int orphanGroupId = memo.insertOperation(orphanOperation, collector);
        memo.pruneOrphanedGroups(collector);

        assertThat(collector.getChanges())
                .containsExactly(
                        new SingletonGroupCreate(orphanOperation, orphanGroupId),
                        new GroupPrune(orphanGroupId, ImmutableList.of(orphanOperation)));
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
                new OperationRemove(parentOperation, parentGroupId),
                new GroupPrune(childGroupId, ImmutableList.of(childOperation)),
                new GroupPrune(parentGroupId, ImmutableList.of(expectedRewrittenParent)),
                new GroupPrune(expectedRewrittenChildGroupId, ImmutableList.of(expectedRewrittenChild)));
    }

    private static Memo baseMemo()
    {
        return Memo.forProgram(program(valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)))), TESTING_DIALECT_REGISTRY);
    }

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
}
