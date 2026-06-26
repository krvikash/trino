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
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.RankingType;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateNonEmptyRowSelector;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.MAX_RANKING_PER_PARTITION;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.PARTIAL;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.RANKING_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata.SORT_ORDERS;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class TopNRanking
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region partitioningSelector;
    private final Region orderingSelector;
    private final Attributes attributes;

    public TopNRanking(
            String resultName,
            Value input,
            Block partitioningSelector,
            Block orderingSelector,
            RankingType rankingType,
            int maxRankingPerPartition,
            boolean partial,
            SortOrderList sortOrders,
            Attributes sourceAttributes)
    {
        this(resultName, input, partitioningSelector, orderingSelector, rankingType, maxRankingPerPartition, partial, sortOrders, sourceAttributes, Attributes.empty());
    }

    public TopNRanking(
            String resultName,
            Value input,
            Block partitioningSelector,
            Block orderingSelector,
            RankingType rankingType,
            int maxRankingPerPartition,
            boolean partial,
            SortOrderList sortOrders,
            Attributes sourceAttributes,
            Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(partitioningSelector, "partitioningSelector is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(rankingType, "rankingType is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the TopNRanking operation must be of relation type");
        }
        this.input = input;

        validateRowSelector(partitioningSelector, relationRowType(trinoType(input.type())), "invalid partitioningSelector for TopNRanking operation");
        this.partitioningSelector = singleBlockRegion(partitioningSelector);

        validateNonEmptyRowSelector(orderingSelector, relationRowType(trinoType(input.type())), "invalid orderingSelector for TopNRanking operation");
        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.sortOrders().size()) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for TopNRanking do not match in size");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (maxRankingPerPartition <= 0) {
            throw new TrinoException(IR_ERROR, "maxRankingPerPartition for TopNRanking operation must be greater than zero");
        }

        if (partial) {
            this.result = new Result(resultName, input.type());
        }
        else {
            List<Type> outputTypes = ImmutableList.<Type>builder()
                    .addAll(relationRowType(trinoType(input.type())).getTypeParameters())
                    .add(BIGINT)
                    .build();
            this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));
        }

        Attributes.Builder operationAttributesBuilder = Attributes.builder();
        RANKING_TYPE.putAttribute(operationAttributesBuilder, rankingType);
        MAX_RANKING_PER_PARTITION.putAttribute(operationAttributesBuilder, maxRankingPerPartition);
        PARTIAL.putAttribute(operationAttributesBuilder, partial);
        SORT_ORDERS.putAttribute(operationAttributesBuilder, sortOrders);
        Attributes operationAttributes = operationAttributesBuilder.buildOrThrow();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(TopNRankingOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        sourceAttributes,
                        partitioningSelector.getTerminalOperation().attributes(),
                        orderingSelector.getTerminalOperation().attributes())));
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
        return ImmutableList.of(partitioningSelector, orderingSelector);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty top n ranking";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new TopNRanking(
                result.name(),
                newArgument,
                partitioningSelector.getOnlyBlock(),
                orderingSelector.getOnlyBlock(),
                RANKING_TYPE.getAttribute(attributes),
                MAX_RANKING_PER_PARTITION.getAttribute(attributes),
                PARTIAL.getAttribute(attributes),
                SORT_ORDERS.getAttribute(attributes),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(TopNRankingOperationMetadata.OPERATION_ATTRIBUTES);
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
        return visitor.visitTopNRanking(this, context);
    }
}
