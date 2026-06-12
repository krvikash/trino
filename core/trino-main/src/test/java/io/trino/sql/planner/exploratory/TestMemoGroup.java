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

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;
import org.junit.jupiter.api.Test;

import static io.trino.sql.dialect.ir.IrDialect.HAS_SIDE_EFFECTS;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.REPEATABILITY;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoGroupMatcher.memoGroup;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_MEMO_OPERATION;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_OPERATION_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_OPERATION_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_TYPE;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.assertMemoGroup;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMemoGroup
{
    @Test
    public void testSingletonGroup()
    {
        MemoGroup actual = MemoGroup.singletonGroup(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        assertMemoGroup(
                actual,
                memoGroup()
                        .withResultType(TEST_TYPE)
                        .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                        .withOperations(TEST_MEMO_OPERATION)
                        .withAttributes(Attributes.empty())
                        .build());
    }

    @Test
    public void testAddOperationAlreadyInGroup()
    {
        MemoGroup actual = MemoGroup.singletonGroup(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);
        actual.addOperation(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        assertMemoGroup(
                actual,
                memoGroup()
                        .withResultType(TEST_TYPE)
                        .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                        .withOperations(TEST_MEMO_OPERATION)
                        .withAttributes(Attributes.empty())
                        .build());
    }

    @Test
    public void testAddOperationNotYetInGroup()
    {
        MemoGroup actual = MemoGroup.singletonGroup(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        MemoOperation anotherMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("another_dialect")
                .build();
        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);

        assertMemoGroup(
                actual,
                memoGroup()
                        .withResultType(TEST_TYPE)
                        .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                        .withOperations(TEST_MEMO_OPERATION, anotherMemoOperation)
                        .withAttributes(Attributes.empty())
                        .build());
    }

    @Test
    public void testMergeGroups()
    {
        MemoGroup firstGroup = MemoGroup.singletonGroup(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        MemoOperation secondMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("second_dialect")
                .build();
        firstGroup.addOperation(secondMemoOperation, TESTING_DIALECT_REGISTRY);

        MemoOperation thirdMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("third_dialect")
                .build();
        MemoGroup secondGroup = MemoGroup.singletonGroup(thirdMemoOperation, TESTING_DIALECT_REGISTRY);
        secondGroup.addOperation(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        firstGroup.mergeWith(secondGroup, TESTING_DIALECT_REGISTRY);

        assertMemoGroup(
                firstGroup,
                memoGroup()
                        .withResultType(TEST_TYPE)
                        .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                        .withOperations(TEST_MEMO_OPERATION, secondMemoOperation, thirdMemoOperation)
                        .withAttributes(Attributes.empty())
                        .build());
    }

    @Test
    public void testRemoveOperation()
    {
        MemoGroup actual = MemoGroup.singletonGroup(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        MemoOperation anotherMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("another_dialect")
                .build();

        // remove operation that is not in the group
        assertThatThrownBy(() -> actual.removeOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY))
                .hasMessage("Removed operation not found in the group");

        // remove the only operation in the group
        assertThatThrownBy(() -> actual.removeOperation(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY))
                .hasMessage("Cannot remove the only operation from the group");

        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);

        // remove operation successfully
        actual.removeOperation(TEST_MEMO_OPERATION, TESTING_DIALECT_REGISTRY);

        assertMemoGroup(
                actual,
                memoGroup()
                        .withResultType(TEST_TYPE)
                        .withGroupParameterTypes(TEST_TYPE, TEST_TYPE)
                        .withOperations(anotherMemoOperation)
                        .withAttributes(Attributes.empty())
                        .build());
    }

    // TEST GROUP ATTRIBUTES

    @Test
    public void testSingletonGroupAttributes()
    {
        // operation with repeatability, safety and side effects known
        MemoOperation memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, SAFE),
                TRUE,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        MemoGroup actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // operation with only side effects known
        memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, HAS_SIDE_EFFECTS), FALSE));
        actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, HAS_SIDE_EFFECTS), FALSE))
                        .build());
    }

    @Test
    public void testAddOperationAlreadyInGroupAttributes()
    {
        MemoOperation memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        MemoGroup actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);

        // add operation with the same attributes - no change expected
        // the new operation is deduplicated with the existing one in the group, so the group still contains only one operation
        // which has the same attributes, so the attributes of the group remain unchanged
        MemoOperation anotherMemoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // add operation with safety attribute - attributes should be updated
        // the new operation replaces the old one in the group
        anotherMemoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, SAFE),
                TRUE,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(anotherMemoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // add operation with conflicting attributes
        // the new operation replaces the old one in the group, but the attributes update fails:
        // the current state of knowledge for the group conflicts with the attributes of the new operation
        // DETERMINISTIC vs NON_DETERMINISTIC
        MemoOperation conflictingMemoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC));
        assertThatThrownBy(() -> actual.addOperation(conflictingMemoOperation, TESTING_DIALECT_REGISTRY))
                .hasMessage("The attributes contain conflicting information about repeatability");
    }

    @Test
    public void testAddOperationNotYetInGroupAttributes()
    {
        MemoOperation memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        MemoGroup actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);

        // add operation with different dialect: it will not be deduplicated in the group
        // the new operation has different attributes -- group attributes should be refreshed to reflect both operations
        MemoOperation anotherMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("another_dialect")
                .withAttributes(attributes(new AttributeKey(IR, SAFE), TRUE))
                .build();
        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation, anotherMemoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // add operation with different dialect: it will not be deduplicated in the group
        // the attributes update fails: the current state of knowledge for the group conflicts with the attributes of the new operation
        // has no side effects vs has side effects
        MemoOperation conflictingMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("yet_another_dialect")
                .withAttributes(attributes(new AttributeKey(IR, HAS_SIDE_EFFECTS), TRUE))
                .build();
        assertThatThrownBy(() -> actual.addOperation(conflictingMemoOperation, TESTING_DIALECT_REGISTRY))
                .hasMessage("The attributes contain conflicting information about side effects");

        // add operation with different dialect: it will not be deduplicated in the group
        // the attributes update fails: attributes of existing operation conflict with the attributes of the new operation
        // DETERMINISTIC vs NON_DETERMINISTIC
        MemoOperation yetAnotherConflictingMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("yet_another_dialect")
                .withAttributes(attributes(new AttributeKey(IR, REPEATABILITY), NON_DETERMINISTIC))
                .build();
        assertThatThrownBy(() -> actual.addOperation(yetAnotherConflictingMemoOperation, TESTING_DIALECT_REGISTRY))
                .hasMessage("The attributes contain conflicting information about repeatability");
    }

    @Test
    public void testMergeGroupsAttributes()
    {
        MemoOperation memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        MemoGroup actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);

        // merge group with itself - no change expected
        actual.mergeWith(actual, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // merge with another group with the same operation but different attributes - attributes should be updated
        MemoOperation anotherMemoOperation = memoOperationWithDerivedAttributes(attributes(new AttributeKey(IR, SAFE), TRUE));
        MemoGroup anotherGroup = MemoGroup.singletonGroup(anotherMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                anotherGroup,
                memoGroup()
                        .withOperations(anotherMemoOperation)
                        .withAttributes(attributes(new AttributeKey(IR, SAFE), TRUE))
                        .build());
        actual.mergeWith(anotherGroup, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(anotherMemoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // merge with other group with conflicting attributes
        MemoOperation conflictingMemoOperation = memoOperationWithDerivedAttributes(attributes(new AttributeKey(IR, HAS_SIDE_EFFECTS), TRUE));
        MemoGroup conflictingGroup = MemoGroup.singletonGroup(conflictingMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                conflictingGroup,
                memoGroup()
                        .withOperations(conflictingMemoOperation)
                        .withAttributes(attributes(new AttributeKey(IR, HAS_SIDE_EFFECTS), TRUE))
                        .build());
        assertThatThrownBy(() -> actual.mergeWith(conflictingGroup, TESTING_DIALECT_REGISTRY))
                .hasMessage("The attributes contain conflicting information about side effects");
    }

    @Test
    public void testRemoveOperationAttributes()
    {
        MemoOperation memoOperation = memoOperationWithDerivedAttributes(attributes(
                new AttributeKey(IR, REPEATABILITY),
                DETERMINISTIC,
                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                FALSE));
        MemoGroup actual = MemoGroup.singletonGroup(memoOperation, TESTING_DIALECT_REGISTRY);

        // add operation with different dialect: it will not be deduplicated in the group
        MemoOperation anotherMemoOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("another_dialect")
                .withAttributes(attributes(new AttributeKey(IR, SAFE), TRUE))
                .build();
        actual.addOperation(anotherMemoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(memoOperation, anotherMemoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());

        // remove the first operation, group attributes remain even though there is no operation to back them explicitly
        actual.removeOperation(memoOperation, TESTING_DIALECT_REGISTRY);
        assertMemoGroup(
                actual,
                memoGroup()
                        .withOperations(anotherMemoOperation)
                        .withAttributes(attributes(
                                new AttributeKey(IR, REPEATABILITY),
                                DETERMINISTIC,
                                new AttributeKey(IR, SAFE),
                                TRUE,
                                new AttributeKey(IR, HAS_SIDE_EFFECTS),
                                FALSE))
                        .build());
    }

    private MemoOperation memoOperationWithDerivedAttributes(Attributes derivedAttributes)
    {
        Attributes.Builder attributes = Attributes.builder();
        attributes.putUnchecked(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT);
        attributes.putAll(derivedAttributes);
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withAttributes(attributes.buildOrThrow())
                .build();
    }
}
