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

import com.google.common.collect.ImmutableList;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation;
import io.trino.sql.planner.exploratory.MemoOperation.Child;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NewMemoOperation
{
    private String dialect;
    private OperationId operationId;
    private Type resultType;
    // Default initial capacities; the arrays grow on demand.
    private Type[] groupParameterTypes = new Type[16];
    private Object[] children = new Object[16];
    private Attributes attributes;

    private NewMemoOperation() {}

    private NewMemoOperation(MemoOperation memoOperation)
    {
        this.dialect = memoOperation.dialect();
        this.operationId = memoOperation.operationId();
        this.resultType = memoOperation.resultType();
        this.groupParameterTypes = memoOperation.groupParameterTypes().toArray(Type[]::new);
        this.children = memoOperation.children().toArray(Object[]::new);
        this.attributes = memoOperation.attributes();
    }

    public static NewMemoOperation operation()
    {
        return new NewMemoOperation();
    }

    public static NewMemoOperation operationLike(MemoOperation memoOperation)
    {
        return new NewMemoOperation(memoOperation);
    }

    public String dialect()
    {
        return dialect;
    }

    public OperationId operationId()
    {
        return operationId;
    }

    public Type resultType()
    {
        return resultType;
    }

    public List<Type> groupParameterTypes()
    {
        return Arrays.asList(groupParameterTypes).subList(0, validateAndGetTrimmedLength(groupParameterTypes, "groupParameterTypes"));
    }

    public List<Object> children()
    {
        return Arrays.asList(children).subList(0, validateAndGetTrimmedLength(children, "children"));
    }

    public Attributes attributes()
    {
        return attributes;
    }

    public NewMemoOperation withDialect(String dialect)
    {
        this.dialect = dialect;
        return this;
    }

    public NewMemoOperation withOperationId(OperationId operationId)
    {
        this.operationId = operationId;
        return this;
    }

    public NewMemoOperation withResultType(Type resultType)
    {
        this.resultType = resultType;
        return this;
    }

    public NewMemoOperation withGroupParameterTypes(List<Type> groupParameterTypes)
    {
        this.groupParameterTypes = groupParameterTypes.toArray(Type[]::new);
        return this;
    }

    public NewMemoOperation withGroupParameterType(int parameterIndex, Type parameterType)
    {
        while (groupParameterTypes.length <= parameterIndex) {
            groupParameterTypes = Arrays.copyOf(groupParameterTypes, Math.max(groupParameterTypes.length * 2, parameterIndex + 1));
        }
        groupParameterTypes[parameterIndex] = parameterType;
        return this;
    }

    public NewMemoOperation withChildren(List<Object> children)
    {
        checkArgument(
                children.stream().allMatch(child -> child instanceof Child || child instanceof ChildGroupWithParameterLineage),
                "Unexpected child type found in children list");
        this.children = children.toArray(Object[]::new);
        return this;
    }

    public NewMemoOperation withChild(int childIndex, Child child)
    {
        ensureChildrenCapacity(childIndex);
        children[childIndex] = child;
        return this;
    }

    public NewMemoOperation withChild(int childIndex, NewMemoOperation childOperation, ParameterLineage parameterLineage)
    {
        return withChildGroup(childIndex, ImmutableList.of(childOperation), parameterLineage);
    }

    public NewMemoOperation withChildGroup(int childIndex, List<NewMemoOperation> childOperations, ParameterLineage parameterLineage)
    {
        ensureChildrenCapacity(childIndex);
        children[childIndex] = new ChildGroupWithParameterLineage(childOperations, parameterLineage);
        return this;
    }

    private void ensureChildrenCapacity(int requiredIndex)
    {
        while (children.length <= requiredIndex) {
            children = Arrays.copyOf(children, Math.max(children.length * 2, requiredIndex + 1));
        }
    }

    public NewMemoOperation withAttributes(Attributes attributes)
    {
        this.attributes = attributes;
        return this;
    }

    private int validateAndGetTrimmedLength(Object[] array, String name)
    {
        int nullSuffix = array.length;
        while (nullSuffix > 0 && array[nullSuffix - 1] == null) {
            nullSuffix--;
        }
        checkArgument(
                Arrays.stream(array)
                        .limit(nullSuffix)
                        .noneMatch(Objects::isNull),
                "Null value found in %s array",
                name);

        return nullSuffix;
    }

    public record ChildGroupWithParameterLineage(List<NewMemoOperation> childOperations, ParameterLineage parameterLineage)
    {
        public ChildGroupWithParameterLineage
        {
            requireNonNull(childOperations, "childOperations is null");
            requireNonNull(parameterLineage, "parameterLineage is null");
            checkArgument(!childOperations.isEmpty(), "no operations in child group");
        }
    }
}
