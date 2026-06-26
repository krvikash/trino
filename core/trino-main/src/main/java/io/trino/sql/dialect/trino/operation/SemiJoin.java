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
import io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata.DistributionType;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelectorReturningExactlyOneField;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata.DYNAMIC_FILTER_ID;
import static io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class SemiJoin
        extends TrinoOperation
{
    private final Result result;
    private final Value source;
    private final Value filteringSource;
    private final Region sourceFieldSelector;
    private final Region filteringSourceFieldSelector;
    private final Attributes attributes;

    public SemiJoin(
            String resultName,
            Value source,
            Value filteringSource,
            Block sourceFieldSelector,
            Block filteringSourceFieldSelector,
            Optional<DistributionType> distributionType,
            Optional<String> dynamicFilterId,
            Attributes sourceAttributes,
            Attributes filteringSourceAttributes)
    {
        this(resultName,
                source,
                filteringSource,
                sourceFieldSelector,
                filteringSourceFieldSelector,
                distributionType,
                dynamicFilterId,
                sourceAttributes,
                filteringSourceAttributes,
                Attributes.empty());
    }

    public SemiJoin(
            String resultName,
            Value source,
            Value filteringSource,
            Block sourceFieldSelector,
            Block filteringSourceFieldSelector,
            Optional<DistributionType> distributionType,
            Optional<String> dynamicFilterId,
            Attributes sourceAttributes,
            Attributes filteringSourceAttributes,
            Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(source, "source is null");
        requireNonNull(filteringSource, "filteringSource is null");
        requireNonNull(sourceFieldSelector, "sourceFieldSelector is null");
        requireNonNull(filteringSourceFieldSelector, "filteringSourceFieldSelector is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(dynamicFilterId, "dynamicFilterId is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(filteringSourceAttributes, "filteringSourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(source.type())) || !IS_RELATION.test(trinoType(filteringSource.type()))) {
            throw new TrinoException(IR_ERROR, "source and filtering source of SemiJoin operation must be of relation type");
        }
        this.source = source;
        this.filteringSource = filteringSource;

        validateRowSelectorReturningExactlyOneField(sourceFieldSelector, relationRowType(trinoType(source.type())), "source field selector for SemiJoin operation must select exactly one field");
        this.sourceFieldSelector = singleBlockRegion(sourceFieldSelector);

        validateRowSelectorReturningExactlyOneField(filteringSourceFieldSelector, relationRowType(trinoType(filteringSource.type())), "filtering source field selector for SemiJoin operation must select exactly one field");
        this.filteringSourceFieldSelector = singleBlockRegion(filteringSourceFieldSelector);

        if (!trinoType(sourceFieldSelector.getReturnedType()).equals(trinoType(filteringSourceFieldSelector.getReturnedType()))) {
            throw new TrinoException(IR_ERROR, "source and filtering source field selectors for SemiJoin operation must return the same type");
        }

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(source.type())).getTypeParameters())
                .add(BOOLEAN)
                .build();
        this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));

        Attributes.Builder operationAttributesBuilder = Attributes.builder();
        distributionType.ifPresent(value -> DISTRIBUTION_TYPE.putAttribute(operationAttributesBuilder, value));
        dynamicFilterId.ifPresent(value -> DYNAMIC_FILTER_ID.putAttribute(operationAttributesBuilder, value));
        Attributes operationAttributes = operationAttributesBuilder.buildOrThrow();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(SemiJoinOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        sourceAttributes,
                        filteringSourceAttributes,
                        sourceFieldSelector.getTerminalOperation().attributes(),
                        filteringSourceFieldSelector.getTerminalOperation().attributes())));
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
        return ImmutableList.of(source, filteringSource);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(sourceFieldSelector, filteringSourceFieldSelector);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty semi join";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new SemiJoin(
                result.name(),
                index == 0 ? newArgument : source,
                index == 1 ? newArgument : filteringSource,
                sourceFieldSelector.getOnlyBlock(),
                filteringSourceFieldSelector.getOnlyBlock(),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(attributes)),
                Optional.ofNullable(DYNAMIC_FILTER_ID.getAttribute(attributes)),
                Attributes.empty(),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(SemiJoinOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Value source()
    {
        return source;
    }

    public Value filteringSource()
    {
        return filteringSource;
    }

    public Block sourceFieldSelector()
    {
        return sourceFieldSelector.getOnlyBlock();
    }

    public Block filteringSourceFieldSelector()
    {
        return filteringSourceFieldSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitSemiJoin(this, context);
    }
}
