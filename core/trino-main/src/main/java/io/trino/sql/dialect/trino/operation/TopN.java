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
import io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TopNStep;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateNonEmptyRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.LIMIT;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.SORT_ORDERS;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TOP_N_STEP;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class TopN
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public TopN(String resultName, Value input, Block orderingSelector, SortOrderList sortOrders, long limit, TopNStep step, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(step, "step is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the TopN operation must be of relation type");
        }
        this.input = input;

        this.result = new Result(resultName, input.type());

        validateNonEmptyRowSelector(orderingSelector, relationRowType(trinoType(input.type())), "invalid ordering selector for TopN operation");
        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.sortOrders().size()) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for TopN do not match in size");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        SORT_ORDERS.putAttribute(operationAttributesBuilder, sortOrders);
        LIMIT.putAttribute(operationAttributesBuilder, limit);
        TOP_N_STEP.putAttribute(operationAttributesBuilder, step);
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(TopNOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes, orderingSelector.getTerminalOperation().attributes())));

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
        return "pretty top n";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new TopN(
                result.name(),
                newArgument,
                orderingSelector.getOnlyBlock(),
                SORT_ORDERS.getAttribute(attributes),
                LIMIT.getAttribute(attributes),
                TOP_N_STEP.getAttribute(attributes),
                ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(TopNOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block orderingSelector()
    {
        return orderingSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }
}
