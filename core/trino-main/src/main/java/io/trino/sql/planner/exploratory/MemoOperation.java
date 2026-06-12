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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.dialect.ir.IrDialect.FunctionType;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.exploratory.AttributeUtils.composeOperationAttributes;
import static io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage.GROUP_PARAM_PREFIX;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * MemoOperation represents an {@link Operation} stored in Memo.
 * Its structure is similar to that of Operation with the following differences:
 * 1. Both Operation's arguments and regions are represented as MemoOperation's children.
 * It results in a flat structure as opposed to the recursive structure of Operation.
 * 2. All Parameters used in the operation and its children are explicitly listed as input parameters.
 * It allows to avoid nested scopes and simplify equivalence checking.
 * Parameter lineage information is stored for each child.
 */
public class MemoOperation
{
    private final String dialect;

    // Identifier of the operation within its dialect.
    // It can be used to distinguish between the original arguments and regions,
    // and to recreate the original parameters passed to child groups.
    private final OperationId operationId;

    private final Type resultType;

    private final List<Type> groupParameterTypes;

    // Children corresponding to both operation arguments and regions.
    // They refer to:
    // - parametrized MemoGroup in case when the operation's argument was an Operation Result
    // - memoGroup's input parameter in case when the operation's argument was a Block Parameter
    // - parametrized MemoGroup of the terminal operation of the Region for each operation's Region
    private final List<Child> children;

    private final Attributes attributes;

    // Keys of attributes that are considered operation attributes (as opposed to derived attributes)
    private final Set<AttributeKey> inherentOperationAttributeKeys;

    @VisibleForTesting
    public MemoOperation(
            String dialect,
            OperationId operationId,
            Type resultType,
            List<Type> groupParameterTypes,
            List<Child> children,
            Attributes attributes,
            Set<AttributeKey> inherentOperationAttributeKeys)
    {
        requireNonNull(dialect, "dialect is null");
        requireNonNull(operationId, "operationId is null");
        requireNonNull(resultType, "resultType is null");
        requireNonNull(groupParameterTypes, "groupParameterTypes is null");
        requireNonNull(children, "children is null");
        requireNonNull(attributes, "attributes is null");
        requireNonNull(inherentOperationAttributeKeys, "inherentOperationAttributeKeys is null");

        checkArgument(
                children.size() == operationId.argumentTypes().size() + operationId.regionTypes().size(),
                "Expected %s children, but got %s",
                operationId.argumentTypes().size() + operationId.regionTypes().size(),
                children.size());

        // verify parameter lineage
        // proper parameters are the parameters declared by the operation and passed to its nested regions
        int properParameterCount = operationId.regionTypes().stream()
                .map(Type::dialectType)
                .map(FunctionType.class::cast)
                .map(FunctionType::argumentTypes)
                .mapToInt(List::size)
                .sum();
        children.forEach(child -> {
            switch (child) {
                case ParameterChild(int groupParameterIndex) -> verifyParameterIndex(groupParameterIndex, groupParameterTypes.size());
                case GroupChild(_, ParameterLineage parameterLineage) -> {
                    // parameter lineage consists of group parameters and operation's proper parameters passed down to the child group
                    parameterLineage.passedGroupParameters().forEach(index -> verifyParameterIndex(index, groupParameterTypes.size()));
                    parameterLineage.passedProperParameters().forEach(index -> verifyParameterIndex(index, properParameterCount));
                }
            }
        });

        // verify result type for children corresponding to operation arguments
        // only do the verification for ParameterChild, since GroupChild's type is not available
        for (int i = 0; i < operationId.argumentTypes().size(); i++) {
            Child child = children.get(i);
            switch (child) {
                case ParameterChild(int groupParameterIndex) -> {
                    Type expectedType = operationId.argumentTypes().get(i);
                    Type actualType = groupParameterTypes.get(groupParameterIndex);
                    checkArgument(expectedType.equals(actualType), "Expected argument type %s for child %s, but got %s", expectedType, i, actualType);
                }
                // proper parameters should not be passed to non-region children
                case GroupChild(_, ParameterLineage parameterLineage) -> checkArgument(parameterLineage.passedProperParameters().isEmpty(), "Expected no proper parameters to be passed to child %s", i);
            }
        }

        // verify result type for children corresponding to operation regions
        // region types in operationId are FunctionTypes: (proper parameter types) -> return type
        // since the unused parameters might be pruned during optimization, we only verify the return type
        // and only do the verification for ParameterChild, since GroupChild's type is not available
        for (int i = operationId.argumentTypes().size(); i < operationId.argumentTypes().size() + operationId.regionTypes().size(); i++) {
            Child child = children.get(i);
            if (child instanceof ParameterChild(int groupParameterIndex)) {
                Type expectedType = operationId.getRegionType(i - operationId.argumentTypes().size()).returnType();
                Type actualType = groupParameterTypes.get(groupParameterIndex);
                checkArgument(expectedType.equals(actualType), "Expected argument type %s for child %s, but got %s", expectedType, i, actualType);
            }
        }

        this.dialect = dialect;
        this.operationId = operationId;
        this.resultType = resultType;
        this.groupParameterTypes = ImmutableList.copyOf(groupParameterTypes);
        this.children = ImmutableList.copyOf(children);
        this.attributes = attributes;
        this.inherentOperationAttributeKeys = ImmutableSet.copyOf(inherentOperationAttributeKeys);
    }

    private static void verifyParameterIndex(int index, int parameterCount)
    {
        checkArgument(index >= 0 && index < parameterCount, "Invalid parameter index %s", index);
    }

    public static MemoOperation create(
            String dialect,
            OperationId operationId,
            Type resultType,
            List<Type> groupParameterTypes,
            List<Child> children,
            Attributes attributes,
            DialectRegistry dialectRegistry)
    {
        return new MemoOperation(
                dialect,
                operationId,
                resultType,
                groupParameterTypes,
                children,
                attributes,
                dialectRegistry.dialect(dialect).getInherentOperationAttributeKeys(operationId));
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
        return groupParameterTypes;
    }

    public List<Child> children()
    {
        return children;
    }

    public Attributes attributes()
    {
        return attributes;
    }

    public Set<AttributeKey> inherentOperationAttributeKeys()
    {
        return inherentOperationAttributeKeys;
    }

    public Attributes deriveAttributes(List<Attributes> childAttributes, DialectRegistry dialectRegistry)
    {
        checkArgument(childAttributes.size() == children.size(), "Expected %s child attributes, but got %s", children.size(), childAttributes.size());

        return dialectRegistry.dialect(dialect).getAttributeDerivationForOperation(operationId).apply(attributes, childAttributes);
    }

    public MemoOperation remapChildren(Function<Integer, Integer> groupIdMapper)
    {
        List<Child> newChildren = children.stream()
                .map(child -> {
                    if (child instanceof GroupChild(int groupId, ParameterLineage parameterLineage)) {
                        return new GroupChild(groupIdMapper.apply(groupId), parameterLineage);
                    }
                    return child;
                })
                .collect(toImmutableList());

        return new MemoOperation(
                this.dialect,
                this.operationId,
                this.resultType,
                this.groupParameterTypes,
                newChildren,
                this.attributes,
                this.inherentOperationAttributeKeys);
    }

    public Operation toOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes groupAttributes, DialectRegistry dialectRegistry)
    {
        // validate arguments
        List<Type> providedArgumentTypes = arguments.stream()
                .map(Value::type)
                .collect(toImmutableList());
        checkArgument(operationId.argumentTypes().equals(providedArgumentTypes), "Expected argument types %s, but got %s", operationId.argumentTypes(), providedArgumentTypes);

        // validate regions
        List<Type> providedRegionTypes = regions.stream()
                .map(Region::getFunctionType)
                .collect(toImmutableList());
        checkArgument(operationId.regionTypes().equals(providedRegionTypes), "Expected region types %s, but got %s", operationId.regionTypes(), providedRegionTypes);

        // compose attributes with group attributes
        Attributes composedAttributes = composeOperationAttributes(attributes, inherentAttributes(), groupAttributes, dialectRegistry);

        Dialect dialect = dialectRegistry.dialect(this.dialect);
        Operation operation = dialect.createOperation(
                operationId.name(),
                resultName,
                arguments,
                regions,
                composedAttributes);

        checkArgument(operation.result().type().equals(resultType), "Expected result type %s, but got %s", resultType, operation.result().type());

        return operation;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MemoOperation that = (MemoOperation) o;

        boolean equals = Objects.equals(dialect, that.dialect) &&
                Objects.equals(operationId, that.operationId) &&
                // do not compare resultType. it should be derivable from dialect and operationId
                Objects.equals(groupParameterTypes, that.groupParameterTypes) &&
                Objects.equals(children, that.children) && // operations are inserted into memo bottom-up, so equivalence of child groups is already established
                Objects.equals(inherentAttributes(), that.inherentAttributes()); // skip derived attributes. the state of knowledge for compared operations may differ
        // do not compare inherentOperationAttributeKeys. it is derived from dialect and operationId

        if (equals && !Objects.equals(resultType, that.resultType)) {
            throw new IllegalStateException("Inconsistent result types for equivalent operations: " + resultType + " vs " + that.resultType);
        }

        return equals;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dialect, operationId, groupParameterTypes, children, inherentAttributes());
    }

    public Attributes inherentAttributes()
    {
        return attributes.filterKeys(inherentOperationAttributeKeys::contains);
    }

    // TODO https://starburstdata.atlassian.net/browse/ENG-19084 add the notion of logical vs physical.

    public sealed interface Child
            permits ParameterChild, GroupChild {}

    public record ParameterChild(int groupParameterIndex)
            implements Child
    {
        @Override
        public String toString()
        {
            return GROUP_PARAM_PREFIX + groupParameterIndex;
        }
    }

    public record GroupChild(int groupId, ParameterLineage parameterLineage)
            implements Child
    {
        public GroupChild
        {
            requireNonNull(parameterLineage, "parameterLineage is null");
        }
    }

    public record ParameterLineage(List<Integer> passedGroupParameters, List<Integer> passedProperParameters)
    {
        public static final String GROUP_PARAM_PREFIX = "group_param_";
        public static final String PROPER_PARAM_PREFIX = "proper_param_";

        public ParameterLineage
        {
            passedGroupParameters = ImmutableList.copyOf(requireNonNull(passedGroupParameters, "passedGroupParameters is null"));
            passedProperParameters = ImmutableList.copyOf(requireNonNull(passedProperParameters, "passedProperParameters is null"));
        }

        public static ParameterLineage identity(int parameterCount)
        {
            return new ParameterLineage(
                    IntStream.range(0, parameterCount).boxed().collect(toImmutableList()),
                    ImmutableList.of());
        }

        public static ParameterLineage identityRecursive(int outerParameterCount, int properParameterOffset, int properParameterCount)
        {
            return new ParameterLineage(
                    IntStream.range(0, outerParameterCount).boxed().collect(toImmutableList()),
                    IntStream.range(properParameterOffset, properParameterOffset + properParameterCount).boxed().collect(toImmutableList()));
        }

        @Override
        public String toString()
        {
            return passedGroupParameters.stream()
                    .map(index -> GROUP_PARAM_PREFIX + index)
                    .collect(joining(", ", "[", "],")) +
                    passedProperParameters.stream()
                            .map(index -> PROPER_PARAM_PREFIX + index)
                            .collect(joining(", ", "[", "]"));
        }
    }
}
