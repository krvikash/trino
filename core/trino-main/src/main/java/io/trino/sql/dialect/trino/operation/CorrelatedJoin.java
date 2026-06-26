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
import io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata.JoinType;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validatePredicate;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRelationSelector;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata.JOIN_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class CorrelatedJoin
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    // correlation as field selector. later we should model correlation through the uses graph?
    private final Region correlation;
    private final Region subquery;
    private final Region filter;
    private final Attributes attributes;
    // TODO the PlanNode has origin subquery for debug. skipping it for now

    public CorrelatedJoin(String resultName, Value input, Block correlation, Block subquery, Block filter, JoinType joinType, Attributes sourceAttributes)
    {
        this(resultName, input, correlation, subquery, filter, joinType, sourceAttributes, Attributes.empty());
    }

    public CorrelatedJoin(
            String resultName,
            Value input,
            Block correlation,
            Block subquery,
            Block filter,
            JoinType joinType,
            Attributes sourceAttributes,
            Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(correlation, "correlation is null");
        requireNonNull(subquery, "subquery is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(joinType, "joinType is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type())) || !IS_RELATION.test(trinoType(subquery.getReturnedType()))) {
            throw new TrinoException(IR_ERROR, "input and subquery of CorrelatedJoin must be of relation type");
        }

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(input.type())).getTypeParameters())
                .addAll(relationRowType(trinoType(subquery.getReturnedType())).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));
        }

        this.input = input;

        validateRowSelector(correlation, relationRowType(trinoType(input.type())), "invalid correlation for CorrelatedJoin operation");
        this.correlation = singleBlockRegion(correlation);

        validateRelationSelector(subquery, relationRowType(trinoType(input.type())), "invalid subquery for CorrelatedJoin operation");
        this.subquery = singleBlockRegion(subquery);

        validatePredicate(filter, relationRowType(trinoType(input.type())), relationRowType(trinoType(subquery.getReturnedType())), "invalid filter for CorrelatedJoin operation");
        this.filter = singleBlockRegion(filter);

        Attributes operationAttributes = JOIN_TYPE.asAttributes(joinType);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(CorrelatedJoinOperationMetadata.deriveAttributes(
                operationAttributes,
                ImmutableList.of(
                        sourceAttributes,
                        correlation.getTerminalOperation().attributes(),
                        subquery.getTerminalOperation().attributes(),
                        filter.getTerminalOperation().attributes())));

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
        return ImmutableList.of(correlation, subquery, filter);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty correlated join";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new CorrelatedJoin(
                result.name(),
                newArgument,
                correlation.getOnlyBlock(),
                subquery.getOnlyBlock(),
                filter.getOnlyBlock(),
                JOIN_TYPE.getAttribute(attributes),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(CorrelatedJoinOperationMetadata.OPERATION_ATTRIBUTES);
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitCorrelatedJoin(this, context);
    }
}
