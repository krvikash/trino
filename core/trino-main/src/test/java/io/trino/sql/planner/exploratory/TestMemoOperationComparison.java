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
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_CHILD_GROUP_ID;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_DERIVED_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_DERIVED_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_DIALECT_NAME;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_MEMO_OPERATION;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_OPERATION_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_OPERATION_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoOperationBuilder.TEST_TYPE;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestMemoOperationComparison
{
    private static final Set<MemoOperation> SINGLETON_SET = ImmutableSet.of(TEST_MEMO_OPERATION);
    private static final Type ANOTHER_TYPE = new Type(TEST_DIALECT_NAME, new Object());

    @Test
    public void testReflexiveComparison()
    {
        assertThat(SINGLETON_SET.contains(MemoOperationBuilder.from(TEST_MEMO_OPERATION).build())).isTrue();
    }

    @Test
    public void testDifferentDialect()
    {
        MemoOperation differentDialectOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect("different_dialect")
                .build();
        assertThat(SINGLETON_SET.contains(differentDialectOperation)).isFalse();
    }

    @Test
    public void testDifferentOperationName()
    {
        MemoOperation differentOperationNameOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withOperationId(new OperationId(
                        "different_operation",
                        TEST_MEMO_OPERATION.operationId().argumentTypes(),
                        TEST_MEMO_OPERATION.operationId().regionTypes()))
                .build();
        assertThat(SINGLETON_SET.contains(differentOperationNameOperation)).isFalse();
    }

    @Test
    public void testInconsistentResultType()
    {
        MemoOperation differentResultTypeOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withResultType(ANOTHER_TYPE)
                .build();
        assertThatThrownBy(() -> SINGLETON_SET.contains(differentResultTypeOperation))
                .hasMessageMatching("Inconsistent result types for equivalent operations.*");
    }

    @Test
    public void testDifferentGroupParameterTypes()
    {
        MemoOperation differentGroupParameterTypesOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withGroupParameterTypes(ImmutableList.of(TEST_TYPE, TEST_TYPE, TEST_TYPE))
                .build();
        assertThat(SINGLETON_SET.contains(differentGroupParameterTypesOperation)).isFalse();
    }

    @Test
    public void testDifferentParameterChild()
    {
        // the first child is a parameter child with index 1 in the TEST_MEMO_OPERATION
        MemoOperation differentParameterChildOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withChildren(ImmutableList.of(
                        new ParameterChild(0),
                        new GroupChild(TEST_CHILD_GROUP_ID, ParameterLineage.identityRecursive(2, 0, 2))))
                .build();
        assertThat(SINGLETON_SET.contains(differentParameterChildOperation)).isFalse();
    }

    @Test
    public void testDifferentGroupChild()
    {
        // the second child is a group child in the TEST_MEMO_OPERATION with different proper parameter lineage
        MemoOperation differentGroupChildOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withChildren(ImmutableList.of(
                        new ParameterChild(1),
                        new GroupChild(TEST_CHILD_GROUP_ID, ParameterLineage.identityRecursive(2, 0, 1))))
                .build();
        assertThat(SINGLETON_SET.contains(differentGroupChildOperation)).isFalse();
    }

    @Test
    public void testDifferentOperationAttributes()
    {
        MemoOperation differentOperationAttributesOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withAttributes(attributes(
                        TEST_OPERATION_ATTRIBUTE_KEY,
                        new Object(),
                        TEST_DERIVED_ATTRIBUTE_KEY,
                        TEST_DERIVED_ATTRIBUTE_OBJECT))
                .build();
        assertThat(SINGLETON_SET.contains(differentOperationAttributesOperation)).isFalse();
    }

    @Test
    public void testDifferentDerivedAttributes()
    {
        // derived attributes are not compared
        MemoOperation differentDerivedAttributesOperation = MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withAttributes(attributes(
                        TEST_OPERATION_ATTRIBUTE_KEY,
                        TEST_OPERATION_ATTRIBUTE_OBJECT,
                        TEST_DERIVED_ATTRIBUTE_KEY,
                        new Object()))
                .build();
        assertThat(SINGLETON_SET.contains(differentDerivedAttributesOperation)).isTrue();
    }
}
