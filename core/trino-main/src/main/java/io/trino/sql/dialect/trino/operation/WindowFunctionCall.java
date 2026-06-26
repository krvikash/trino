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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameType;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
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
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.DISTINCT;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_END_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_START_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.FRAME_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.IGNORE_NULLS;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.SORT_ORDERS;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameType.RANGE;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class WindowFunctionCall
        extends TrinoOperation
{
    private final Result result;
    private final Value window;
    private final Region arguments;
    private final Region orderingSelector;
    private final Region frameStartFieldSelector;
    private final Region sortKeyCoercedForFrameStartComparisonSelector;
    private final Region frameEndFieldSelector;
    private final Region sortKeyCoercedForFrameEndComparisonSelector;
    private final Attributes attributes;

    public WindowFunctionCall(
            String resultName,
            Value window,
            Block arguments,
            Block orderingSelector,
            Block frameStartFieldSelector,
            Block sortKeyCoercedForFrameStartComparisonSelector,
            Block frameEndFieldSelector,
            Block sortKeyCoercedForFrameEndComparisonSelector,
            ResolvedFunction function,
            Optional<SortOrderList> sortOrders,
            WindowFrameType frameType,
            WindowFrameBoundType frameStartType,
            WindowFrameBoundType frameEndType,
            boolean ignoreNulls,
            boolean distinct)
    {
        this(resultName,
                window,
                arguments,
                orderingSelector,
                frameStartFieldSelector,
                sortKeyCoercedForFrameStartComparisonSelector,
                frameEndFieldSelector,
                sortKeyCoercedForFrameEndComparisonSelector,
                function,
                sortOrders,
                frameType,
                frameStartType,
                frameEndType,
                ignoreNulls,
                distinct,
                Attributes.empty());
    }

    public WindowFunctionCall(
            String resultName,
            Value window,
            Block arguments,
            Block orderingSelector,
            Block frameStartFieldSelector,
            Block sortKeyCoercedForFrameStartComparisonSelector,
            Block frameEndFieldSelector,
            Block sortKeyCoercedForFrameEndComparisonSelector,
            ResolvedFunction function,
            Optional<SortOrderList> sortOrders,
            WindowFrameType frameType,
            WindowFrameBoundType frameStartType,
            WindowFrameBoundType frameEndType,
            boolean ignoreNulls,
            boolean distinct,
            Attributes enforcedAttributes)
// we don't pass input attributes because the argument is always a Block Parameter
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(window, "window is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(frameStartFieldSelector, "frameStartFieldSelector is null");
        requireNonNull(sortKeyCoercedForFrameStartComparisonSelector, "sortKeyCoercedForFrameStartComparisonSelector is null");
        requireNonNull(frameEndFieldSelector, "frameEndFieldSelector is null");
        requireNonNull(sortKeyCoercedForFrameEndComparisonSelector, "sortKeyCoercedForFrameEndComparisonSelector is null");
        requireNonNull(function, "function is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(frameType, "frameType is null");
        requireNonNull(frameStartType, "frameStartType is null");
        requireNonNull(frameEndType, "frameEndType is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(window.type()))) {
            throw new TrinoException(IR_ERROR, "window input of WindowFunctionCall operation must be of relation type");
        }
        this.window = window;

        validateRowSelector(arguments, relationRowType(trinoType(window.type())), "invalid arguments for WindowFunctionCall operation");
        this.arguments = singleBlockRegion(arguments);

        this.result = new Result(resultName, irType(function.signature().getReturnType()));

        validateRowSelector(orderingSelector, relationRowType(trinoType(window.type())), "invalid ordering selector for WindowFunctionCall operation");
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for WindowFunctionCall do not match in size");
        }

        validateRowSelectorReturningAtMostOneField(frameStartFieldSelector, relationRowType(trinoType(window.type())), "invalid frame start field selector for WindowFunctionCall operation");
        this.frameStartFieldSelector = singleBlockRegion(frameStartFieldSelector);

        validateRowSelectorReturningAtMostOneField(sortKeyCoercedForFrameStartComparisonSelector, relationRowType(trinoType(window.type())), "invalid sort key selector for WindowFunctionCall operation");
        this.sortKeyCoercedForFrameStartComparisonSelector = singleBlockRegion(sortKeyCoercedForFrameStartComparisonSelector);

        validateRowSelectorReturningAtMostOneField(frameEndFieldSelector, relationRowType(trinoType(window.type())), "invalid frame end field selector for WindowFunctionCall operation");
        this.frameEndFieldSelector = singleBlockRegion(frameEndFieldSelector);

        validateRowSelectorReturningAtMostOneField(sortKeyCoercedForFrameEndComparisonSelector, relationRowType(trinoType(window.type())), "invalid sort key selector for WindowFunctionCall operation");
        this.sortKeyCoercedForFrameEndComparisonSelector = singleBlockRegion(sortKeyCoercedForFrameEndComparisonSelector);

        if (frameType == RANGE) {
            if (trinoType(frameStartFieldSelector.getReturnedType()).getTypeParameters().size() == 1 &&
                    trinoType(sortKeyCoercedForFrameStartComparisonSelector.getReturnedType()).getTypeParameters().isEmpty()) {
                throw new TrinoException(IR_ERROR, "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if frameStartField is present");
            }
            if (trinoType(frameEndFieldSelector.getReturnedType()).getTypeParameters().size() == 1 &&
                    trinoType(sortKeyCoercedForFrameEndComparisonSelector.getReturnedType()).getTypeParameters().isEmpty()) {
                throw new TrinoException(IR_ERROR, "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if frameEndField is present");
            }
        }

        Attributes.Builder operationAttributesBuilder = Attributes.builder();
        RESOLVED_FUNCTION.putAttribute(operationAttributesBuilder, function);
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(operationAttributesBuilder, orders));
        FRAME_TYPE.putAttribute(operationAttributesBuilder, frameType);
        FRAME_START_TYPE.putAttribute(operationAttributesBuilder, frameStartType);
        FRAME_END_TYPE.putAttribute(operationAttributesBuilder, frameEndType);
        IGNORE_NULLS.putAttribute(operationAttributesBuilder, ignoreNulls);
        DISTINCT.putAttribute(operationAttributesBuilder, distinct);
        Attributes operationAttributes = operationAttributesBuilder.buildOrThrow();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(WindowFunctionCallOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        DEFAULT_BLOCK_PARAMETER_ATTRIBUTES, // group input
                        arguments.getTerminalOperation().attributes(),
                        orderingSelector.getTerminalOperation().attributes(),
                        frameStartFieldSelector.getTerminalOperation().attributes(),
                        sortKeyCoercedForFrameStartComparisonSelector.getTerminalOperation().attributes(),
                        frameEndFieldSelector.getTerminalOperation().attributes(),
                        sortKeyCoercedForFrameEndComparisonSelector.getTerminalOperation().attributes())));

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
        return ImmutableList.of(window);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(arguments, orderingSelector, frameStartFieldSelector, sortKeyCoercedForFrameStartComparisonSelector, frameEndFieldSelector, sortKeyCoercedForFrameEndComparisonSelector);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty window function call";
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == 6, "regions lists size mismatch");
        return new WindowFunctionCall(
                result.name(),
                window,
                newRegions.get(0).getOnlyBlock(),
                newRegions.get(1).getOnlyBlock(),
                newRegions.get(2).getOnlyBlock(),
                newRegions.get(3).getOnlyBlock(),
                newRegions.get(4).getOnlyBlock(),
                newRegions.get(5).getOnlyBlock(),
                RESOLVED_FUNCTION.getAttribute(attributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                FRAME_TYPE.getAttribute(attributes),
                FRAME_START_TYPE.getAttribute(attributes),
                FRAME_END_TYPE.getAttribute(attributes),
                IGNORE_NULLS.getAttribute(attributes),
                DISTINCT.getAttribute(attributes));
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new WindowFunctionCall(
                result.name(),
                newArgument,
                arguments.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                frameStartFieldSelector.getOnlyBlock(),
                sortKeyCoercedForFrameStartComparisonSelector.getOnlyBlock(),
                frameEndFieldSelector.getOnlyBlock(),
                sortKeyCoercedForFrameEndComparisonSelector.getOnlyBlock(),
                RESOLVED_FUNCTION.getAttribute(attributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                FRAME_TYPE.getAttribute(attributes),
                FRAME_START_TYPE.getAttribute(attributes),
                FRAME_END_TYPE.getAttribute(attributes),
                IGNORE_NULLS.getAttribute(attributes),
                DISTINCT.getAttribute(attributes));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new WindowFunctionCall(
                newName,
                window,
                arguments.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                frameStartFieldSelector.getOnlyBlock(),
                sortKeyCoercedForFrameStartComparisonSelector.getOnlyBlock(),
                frameEndFieldSelector.getOnlyBlock(),
                sortKeyCoercedForFrameEndComparisonSelector.getOnlyBlock(),
                RESOLVED_FUNCTION.getAttribute(attributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                FRAME_TYPE.getAttribute(attributes),
                FRAME_START_TYPE.getAttribute(attributes),
                FRAME_END_TYPE.getAttribute(attributes),
                IGNORE_NULLS.getAttribute(attributes),
                DISTINCT.getAttribute(attributes));
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(WindowFunctionCallOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block argumentsBlock()
    {
        return arguments.getOnlyBlock();
    }

    public Block orderingSelector()
    {
        return orderingSelector.getOnlyBlock();
    }

    public Block frameStartFieldSelector()
    {
        return frameStartFieldSelector.getOnlyBlock();
    }

    public Block sortKeyCoercedForFrameStartComparisonSelector()
    {
        return sortKeyCoercedForFrameStartComparisonSelector.getOnlyBlock();
    }

    public Block frameEndFieldSelector()
    {
        return frameEndFieldSelector.getOnlyBlock();
    }

    public Block sortKeyCoercedForFrameEndComparisonSelector()
    {
        return sortKeyCoercedForFrameEndComparisonSelector.getOnlyBlock();
    }
}
