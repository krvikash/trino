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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AggregationStep;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.type.FunctionType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelectorReturningAtMostOneField;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AggregationStep.FINAL;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AggregationStep.PARTIAL;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.AggregationStep.SINGLE;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.DISTINCT;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.RESULT_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata.SORT_ORDERS;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AggregateCall
        extends TrinoOperation
{
    private final Result result;
    private final Value group;
    private final Region arguments;
    private final Region filterSelector;
    private final Region maskSelector;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public AggregateCall(
            String resultName,
            Value group,
            // AggregateCall should derive its own output type based on the function and step.
            // For SINGLE and FINAL step, the type can be derived from the ResolvedFunction.
            // For PARTIAL and INTERMEDIATE step, the type can be obtained through a metadata call: AggregationFunctionMetadata.getIntermediateTypes().
            // For now, we are passing the type at construction.
            // TODO derive output type instead of passing
            Type outputType,
            Block arguments,
            Block filterSelector,
            Block maskSelector,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            ResolvedFunction function,
            boolean distinct,
            AggregationStep step) // step is needed to verify argument count and validate output type
    {
        this(resultName, group, outputType, arguments, filterSelector, maskSelector, orderingSelector, sortOrders, function, distinct, step, ImmutableMap.of());
    }

    public AggregateCall(
            String resultName,
            Value group,
            // AggregateCall should derive its own output type based on the function and step.
            // For SINGLE and FINAL step, the type can be derived from the ResolvedFunction.
            // For PARTIAL and INTERMEDIATE step, the type can be obtained through a metadata call: AggregationFunctionMetadata.getIntermediateTypes().
            // For now, we are passing the type at construction.
            // TODO derive output type instead of passing
            Type outputType,
            Block arguments,
            Block filterSelector,
            Block maskSelector,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            ResolvedFunction function,
            boolean distinct,
            AggregationStep step, // needed to verify argument count and validate output type
            Map<AttributeKey, Object> enforcedAttributes)
// we don't pass input attributes because the argument is always a Block Parameter
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(group, "group is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(filterSelector, "filterSelector is null");
        requireNonNull(maskSelector, "maskSelector is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(function, "function is null");
        requireNonNull(step, "step is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(group.type()))) {
            throw new TrinoException(IR_ERROR, "group input of AggregateCall operation must be of relation type");
        }
        this.group = group;

        validateRowSelector(arguments, relationRowType(trinoType(group.type())), "invalid arguments for AggregateCall operation");

        // verify argument count considering step
        List<Type> argumentTypes = trinoType(arguments.getReturnedType()).getTypeParameters();
        if (step == SINGLE || step == PARTIAL) {
            if (function.signature().getArgumentTypes().size() != argumentTypes.size()) {
                throw new TrinoException(IR_ERROR, format("expected %s arguments for aggregation step %s, found: %s", function.signature().getArgumentTypes().size(), step, argumentTypes.size()));
            }
            for (int i = 0; i < argumentTypes.size(); i++) {
                Type expectedType = function.signature().getArgumentType(i);
                Type actualType = argumentTypes.get(i);
                if (!expectedType.equals(actualType)) {
                    throw new TrinoException(IR_ERROR, format("invalid argument type. expected: %s, actual: %s", expectedType, actualType));
                }
            }
        }
        else {
            // intermediate and final steps get the intermediate value and the lambda functions
            int expectedArgumentCount = 1 + (int) function.signature().getArgumentTypes().stream()
                    .filter(FunctionType.class::isInstance)
                    .count();
            if (expectedArgumentCount != argumentTypes.size()) {
                throw new TrinoException(IR_ERROR, format("expected %s arguments for aggregation step %s, found: %s", expectedArgumentCount, step, argumentTypes.size()));
            }
        }

        // TODO verify that arguments are all FieldSelection or Lambda operations -- we must pass the current Program (or its value map) to get values sources
        /*if (trinoType(arguments.getReturnedType()) instanceof RowType rowType) {
            Row argumentsRow = (Row) getOnlyElement(arguments.getTerminalOperation().arguments()).source(program);
            // recreating the check from AggregationNode.Aggregation
            // each argument must be either
            //  - reference to input row field
            //  - Lambda operation
            for (Value argument : argumentsRow.arguments()) {
                SourceNode sourceNode = argument.source(program);
                boolean validArgument = sourceNode instanceof Lambda ||
                        sourceNode instanceof FieldSelection fieldSelection && getOnlyElement(fieldSelection.arguments()).source(program).equals(arguments);
                if (!validArgument) {
                    throw new TrinoException(IR_ERROR, "invalid argument to AggregateCall operation. Expected lambda or input field reference");
                }
            }
        }*/
        this.arguments = singleBlockRegion(arguments);

        if ((step == SINGLE || step == FINAL) && !outputType.equals(function.signature().getReturnType())) {
            throw new TrinoException(IR_ERROR, "invalid output type for AggregateCall operation");
        }
        this.result = new Result(resultName, irType(outputType));

        validateRowSelectorReturningAtMostOneField(filterSelector, relationRowType(trinoType(group.type())), "invalid filter selector for AggregateCall operation");
        this.filterSelector = singleBlockRegion(filterSelector);

        validateRowSelectorReturningAtMostOneField(maskSelector, relationRowType(trinoType(group.type())), "invalid mask selector for AggregateCall operation");
        this.maskSelector = singleBlockRegion(maskSelector);

        validateRowSelector(orderingSelector, relationRowType(trinoType(group.type())), "invalid ordering selector for AggregateCall operation");
        if (!(trinoType(orderingSelector.getReturnedType()).getTypeParameters().isEmpty() || step == SINGLE)) {
            throw new TrinoException(IR_ERROR, "ORDER BY is not supported for distributed aggregation");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for AggregateCall do not match in size");
        }

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(operationAttributesBuilder, orders));
        RESOLVED_FUNCTION.putAttribute(operationAttributesBuilder, function);
        DISTINCT.putAttribute(operationAttributesBuilder, distinct);
        AGGREGATION_STEP.putAttribute(operationAttributesBuilder, step);
        RESULT_TYPE.putAttribute(operationAttributesBuilder, outputType);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(AggregateCallOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        DEFAULT_BLOCK_PARAMETER_ATTRIBUTES, // group input
                        arguments.getTerminalOperation().attributes(),
                        filterSelector.getTerminalOperation().attributes(),
                        maskSelector.getTerminalOperation().attributes(),
                        orderingSelector.getTerminalOperation().attributes())));

        // TODO check if new attributes are compatible with existing ones. In particular, internal attributes must not change
        attributes.putAll(enforcedAttributes);
        this.attributes = attributes.buildKeepingLast();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(group);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(arguments, filterSelector, maskSelector, orderingSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty aggregate call";
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == 4, "regions lists size mismatch");
        return new AggregateCall(
                result.name(),
                group,
                trinoType(result.type()),
                newRegions.get(0).getOnlyBlock(),
                newRegions.get(1).getOnlyBlock(),
                newRegions.get(2).getOnlyBlock(),
                newRegions.get(3).getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                RESOLVED_FUNCTION.getAttribute(attributes),
                DISTINCT.getAttribute(attributes),
                AGGREGATION_STEP.getAttribute(attributes));
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new AggregateCall(
                result.name(),
                newArgument,
                trinoType(result.type()),
                arguments.getOnlyBlock(),
                filterSelector.getOnlyBlock(),
                maskSelector.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                RESOLVED_FUNCTION.getAttribute(attributes),
                DISTINCT.getAttribute(attributes),
                AGGREGATION_STEP.getAttribute(attributes));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new AggregateCall(
                newName,
                group,
                trinoType(result.type()),
                arguments.getOnlyBlock(),
                filterSelector.getOnlyBlock(),
                maskSelector.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                RESOLVED_FUNCTION.getAttribute(attributes),
                DISTINCT.getAttribute(attributes),
                AGGREGATION_STEP.getAttribute(attributes));
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(AggregateCallOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block argumentsBlock()
    {
        return arguments.getOnlyBlock();
    }

    public Block filterSelector()
    {
        return filterSelector.getOnlyBlock();
    }

    public Block maskSelector()
    {
        return maskSelector.getOnlyBlock();
    }

    public Block orderingSelector()
    {
        return orderingSelector.getOnlyBlock();
    }
}
