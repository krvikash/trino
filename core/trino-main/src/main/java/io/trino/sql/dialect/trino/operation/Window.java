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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.PRE_PARTITIONED_INDEXES;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.PRE_SORTED_PREFIX;
import static io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata.SORT_ORDERS;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class Window
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region windowFunctionCalls;
    private final Region partitioningSelector;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public Window(
            String resultName,
            Value input,
            Block windowFunctionCalls,
            Block partitioningSelector,
            Block orderingSelector,
            List<Integer> prePartitionedIndexes, // indexes in partitioningSelector
            Optional<SortOrderList> sortOrders,
            int preSortedPrefix,
            Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(windowFunctionCalls, "windowFunctionCalls is null");
        requireNonNull(partitioningSelector, "partitioningSelector is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(prePartitionedIndexes, "prePartitionedIndexes is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Window operation must be of relation type");
        }
        this.input = input;

        validateRowSelector(windowFunctionCalls, trinoType(input.type()), "invalid windowFunctionCalls for Window operation");
        this.windowFunctionCalls = singleBlockRegion(windowFunctionCalls);

        validateRowSelector(partitioningSelector, relationRowType(trinoType(input.type())), "invalid partitioningSelector for Window operation");
        this.partitioningSelector = singleBlockRegion(partitioningSelector);

        int partitioningCount = trinoType(partitioningSelector.getReturnedType()).getTypeParameters().size();
        prePartitionedIndexes.stream()
                .forEach(index -> {
                    if (index < 0 || index >= partitioningCount) {
                        throw new TrinoException(IR_ERROR, "invalid pre-partitioned field for Window operation");
                    }
                });

        validateRowSelector(orderingSelector, relationRowType(trinoType(input.type())), "invalid orderingSelector for Window operation");
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for Window do not match in size");
        }

        if (preSortedPrefix > trinoType(orderingSelector.getReturnedType()).getTypeParameters().size()) {
            throw new TrinoException(IR_ERROR, "too many pre-sorted inputs for Window");
        }

        Set<Integer> partitioningIndexes = IntStream.range(0, trinoType(partitioningSelector.getReturnedType()).getTypeParameters().size())
                .boxed()
                .collect(toImmutableSet());
        if (preSortedPrefix > 0 && !partitioningIndexes.equals(ImmutableSet.copyOf(prePartitionedIndexes))) {
            throw new TrinoException(IR_ERROR, "preSortedPrefix for Window operation can only be greater than zero if all partitioning fields are pre-partitioned");
        }

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(input.type())).getTypeParameters())
                .addAll(trinoType(windowFunctionCalls.getReturnedType()).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        PRE_PARTITIONED_INDEXES.putAttribute(attributes, prePartitionedIndexes);
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(attributes, orders));
        PRE_SORTED_PREFIX.putAttribute(attributes, preSortedPrefix);

        // TODO derive attributes from source attributes
        this.attributes = attributes.buildOrThrow();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(windowFunctionCalls, partitioningSelector, orderingSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty window";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Window(
                result.name(),
                newArgument,
                windowFunctionCalls.getOnlyBlock(),
                partitioningSelector.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                PRE_PARTITIONED_INDEXES.getAttribute(attributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                PRE_SORTED_PREFIX.getAttribute(attributes),
                ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(WindowOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block windowFunctionCalls()
    {
        return windowFunctionCalls.getOnlyBlock();
    }

    public Block partitioningSelector()
    {
        return partitioningSelector.getOnlyBlock();
    }

    public Block orderingSelector()
    {
        return orderingSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }
}
