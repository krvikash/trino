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
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.PARTIAL;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.PRE_SORTED_INDEXES;
import static io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata.SORT_ORDERS;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class Limit
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public Limit(
            String resultName,
            Value input,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            long count,
            boolean partial,
            List<Integer> preSortedIndexes, // indexes in orderingSelector
            Map<AttributeKey, Object> sourceAttributes)
    {
        this(resultName, input, orderingSelector, sortOrders, count, partial, preSortedIndexes, sourceAttributes, ImmutableMap.of());
    }

    public Limit(
            String resultName,
            Value input,
            Block orderingSelector,
            Optional<SortOrderList> sortOrders,
            long count,
            boolean partial,
            List<Integer> preSortedIndexes, // indexes in orderingSelector
            Map<AttributeKey, Object> sourceAttributes,
            Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(preSortedIndexes, "preSortedIndexes is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the limit operation must be of relation type");
        }
        this.input = input;

        this.result = new Result(resultName, input.type());

        validateRowSelector(orderingSelector, relationRowType(trinoType(input.type())), "invalid ordering selector for limit operation");
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for limit do not match in size");
        }

        int orderingSize = trinoType(orderingSelector.getReturnedType()).getTypeParameters().size();
        preSortedIndexes.stream()
                .forEach(index -> {
                    if (index < 0 || index >= orderingSize) {
                        throw new TrinoException(IR_ERROR, "invalid pre-sorted field for limit operation");
                    }
                });

        if (count < 0) {
            throw new TrinoException(IR_ERROR, "invalid count for limit operation");
        }

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(operationAttributesBuilder, orders));
        COUNT.putAttribute(operationAttributesBuilder, count);
        PARTIAL.putAttribute(operationAttributesBuilder, partial);
        PRE_SORTED_INDEXES.putAttribute(operationAttributesBuilder, preSortedIndexes);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(LimitOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes, orderingSelector.getTerminalOperation().attributes())));

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
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(orderingSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty limit";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Limit(
                result.name(),
                newArgument,
                orderingSelector.getOnlyBlock(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(attributes)),
                COUNT.getAttribute(attributes),
                PARTIAL.getAttribute(attributes),
                PRE_SORTED_INDEXES.getAttribute(attributes),
                ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(LimitOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public boolean isWithTies()
    {
        return SORT_ORDERS.getAttribute(attributes()) != null;
    }

    public Block orderingSelector()
    {
        return orderingSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }
}
