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
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DistributionType;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JoinType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validatePredicate;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JOIN_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.SPILLABLE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Join
        extends TrinoOperation
{
    private final Result result;
    private final Value left;
    private final Value right;
    private final Region leftCriteriaSelector;
    private final Region rightCriteriaSelector;
    private final Region filter;
    private final Region leftOutputSelector;
    private final Region rightOutputSelector;
    private final Region dynamicFilterTargetSelector;
    private final Map<AttributeKey, Object> attributes;

    public Join(
            String resultName,
            Value left,
            Value right,
            Block leftCriteriaSelector,
            Block rightCriteriaSelector,
            Block filter,
            Block leftOutputSelector,
            Block rightOutputSelector,
            Block dynamicFilterTargetSelector,
            JoinType joinType,
            boolean maySkipOutputDuplicates,
            Optional<DistributionType> distributionType,
            Optional<Boolean> spillable,
            List<String> dynamicFilterIds,
            Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            Map<AttributeKey, Object> leftAttributes,
            Map<AttributeKey, Object> rightAttributes)
    {
        this(
                resultName,
                left,
                right,
                leftCriteriaSelector,
                rightCriteriaSelector,
                filter,
                leftOutputSelector,
                rightOutputSelector,
                dynamicFilterTargetSelector,
                joinType,
                maySkipOutputDuplicates,
                distributionType,
                spillable,
                dynamicFilterIds,
                reorderJoinStatsAndCost,
                leftAttributes,
                rightAttributes,
                ImmutableMap.of());
    }

    public Join(
            String resultName,
            Value left,
            Value right,
            Block leftCriteriaSelector,
            Block rightCriteriaSelector,
            Block filter,
            Block leftOutputSelector,
            Block rightOutputSelector,
            Block dynamicFilterTargetSelector,
            JoinType joinType,
            boolean maySkipOutputDuplicates,
            Optional<DistributionType> distributionType,
            Optional<Boolean> spillable,
            List<String> dynamicFilterIds,
            Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            Map<AttributeKey, Object> leftAttributes,
            Map<AttributeKey, Object> rightAttributes,
            Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(leftCriteriaSelector, "leftCriteriaSelector is null");
        requireNonNull(rightCriteriaSelector, "rightCriteriaSelector is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftOutputSelector, "leftOutputSelector is null");
        requireNonNull(rightOutputSelector, "rightOutputSelector is null");
        requireNonNull(dynamicFilterTargetSelector, "dynamicFilterTargetSelector is null");
        requireNonNull(joinType, "joinType is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");
        requireNonNull(dynamicFilterIds, "dynamicFilterIds is null");
        requireNonNull(dynamicFilterIds, "dynamicFilterIds is null");
        requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        requireNonNull(leftAttributes, "leftAttributes is null");
        requireNonNull(rightAttributes, "rightAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(left.type())) || !IS_RELATION.test(trinoType(right.type()))) {
            throw new TrinoException(IR_ERROR, "left and right sources of Join operation must be of relation type");
        }
        this.left = left;
        this.right = right;

        validateRowSelector(leftCriteriaSelector, relationRowType(trinoType(left.type())), "invalid left criteria selector for Join operation");
        this.leftCriteriaSelector = singleBlockRegion(leftCriteriaSelector);

        validateRowSelector(rightCriteriaSelector, relationRowType(trinoType(right.type())), "invalid right criteria selector for Join operation");
        this.rightCriteriaSelector = singleBlockRegion(rightCriteriaSelector);

        if (!trinoType(leftCriteriaSelector.getReturnedType()).getTypeParameters().equals(trinoType(rightCriteriaSelector.getReturnedType()).getTypeParameters())) {
            throw new TrinoException(IR_ERROR, "left and right criteria selectors for Join operation do not match");
        }

        validatePredicate(filter, relationRowType(trinoType(left.type())), relationRowType(trinoType(right.type())), "invalid filter for Join operation");
        this.filter = singleBlockRegion(filter);

        validateRowSelector(leftOutputSelector, relationRowType(trinoType(left.type())), "invalid left output selector for Join operation");
        this.leftOutputSelector = singleBlockRegion(leftOutputSelector);

        validateRowSelector(rightOutputSelector, relationRowType(trinoType(right.type())), "invalid right output selector for Join operation");
        this.rightOutputSelector = singleBlockRegion(rightOutputSelector);

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(trinoType(leftOutputSelector.getReturnedType()).getTypeParameters())
                .addAll(trinoType(rightOutputSelector.getReturnedType()).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));
        }

        validateRowSelector(dynamicFilterTargetSelector, relationRowType(trinoType(right.type())), "invalid dynamic filter target selector for Join operation");
        this.dynamicFilterTargetSelector = singleBlockRegion(dynamicFilterTargetSelector);

        if (trinoType(dynamicFilterTargetSelector.getReturnedType()).getTypeParameters().size() != dynamicFilterIds.size()) {
            throw new TrinoException(IR_ERROR, "dynamic filter target selector for Join operation does not match dynamic filter IDs");
        }

        ImmutableMap.Builder<AttributeKey, Object> operationAttributesBuilder = ImmutableMap.builder();
        JOIN_TYPE.putAttribute(operationAttributesBuilder, joinType);
        MAY_SKIP_OUTPUT_DUPLICATES.putAttribute(operationAttributesBuilder, maySkipOutputDuplicates);
        distributionType.ifPresent(value -> DISTRIBUTION_TYPE.putAttribute(operationAttributesBuilder, value));
        spillable.ifPresent(value -> SPILLABLE.putAttribute(operationAttributesBuilder, value));
        DYNAMIC_FILTER_IDS.putAttribute(operationAttributesBuilder, dynamicFilterIds);
        reorderJoinStatsAndCost.ifPresent(estimate -> STATISTICS_AND_COST_SUMMARY.putAttribute(operationAttributesBuilder, estimate));
        Map<AttributeKey, Object> operationAttributes = operationAttributesBuilder.buildOrThrow();

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(JoinOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        leftAttributes,
                        rightAttributes,
                        leftCriteriaSelector.getTerminalOperation().attributes(),
                        rightCriteriaSelector.getTerminalOperation().attributes(),
                        filter.getTerminalOperation().attributes(),
                        leftOutputSelector.getTerminalOperation().attributes(),
                        rightOutputSelector.getTerminalOperation().attributes(),
                        dynamicFilterTargetSelector.getTerminalOperation().attributes())));

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
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(leftCriteriaSelector, rightCriteriaSelector, filter, leftOutputSelector, rightOutputSelector, dynamicFilterTargetSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty join";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Join(
                result.name(),
                index == 0 ? newArgument : left,
                index == 1 ? newArgument : right,
                leftCriteriaSelector.getOnlyBlock(),
                rightCriteriaSelector.getOnlyBlock(),
                filter.getOnlyBlock(),
                leftOutputSelector.getOnlyBlock(),
                rightOutputSelector.getOnlyBlock(),
                dynamicFilterTargetSelector.getOnlyBlock(),
                JOIN_TYPE.getAttribute(attributes),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(attributes),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(attributes)),
                Optional.ofNullable(SPILLABLE.getAttribute(attributes)),
                DYNAMIC_FILTER_IDS.getAttribute(attributes),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(attributes)),
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(JoinOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Value left()
    {
        return left;
    }

    public Value right()
    {
        return right;
    }

    public Block leftCriteriaSelector()
    {
        return leftCriteriaSelector.getOnlyBlock();
    }

    public Block rightCriteriaSelector()
    {
        return rightCriteriaSelector.getOnlyBlock();
    }

    public Block filter()
    {
        return filter.getOnlyBlock();
    }

    public Block leftOutputSelector()
    {
        return leftOutputSelector.getOnlyBlock();
    }

    public Block rightOutputSelector()
    {
        return rightOutputSelector.getOnlyBlock();
    }

    public Block dynamicFilterTargetSelector()
    {
        return dynamicFilterTargetSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }
}
