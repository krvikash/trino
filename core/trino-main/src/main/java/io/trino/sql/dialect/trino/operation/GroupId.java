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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata.GROUPING_SETS;
import static io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static java.util.Objects.requireNonNull;

public class GroupId
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    // this is not a pass-through output fields selector. For each selected input column, an artificial output column will be computed.
    // It will contain the values of the original columns filtered by whether the column is included in the grouping set
    private final Region groupingColumnsSelector;
    private final Region aggregationArgumentsSelector;
    private final Map<AttributeKey, Object> attributes;

    public GroupId(
            String resultName,
            Value input,
            Block groupingColumnsSelector,
            Block aggregationArgumentsSelector,
            List<List<Integer>> groupingSets, // indexes in fields returned by groupingColumnsSelector
            Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(groupingColumnsSelector, "groupingColumnsSelector is null");
        requireNonNull(aggregationArgumentsSelector, "aggregationArgumentsSelector is null");
        requireNonNull(groupingSets, "groupingSets is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the GroupId operation must be of relation type");
        }
        this.input = input;

        validateRowSelector(groupingColumnsSelector, relationRowType(trinoType(input.type())), "invalid grouping columns selector for GroupId operation");
        this.groupingColumnsSelector = singleBlockRegion(groupingColumnsSelector);

        validateRowSelector(aggregationArgumentsSelector, relationRowType(trinoType(input.type())), "invalid aggregation arguments selector for GroupId operation");
        this.aggregationArgumentsSelector = singleBlockRegion(aggregationArgumentsSelector);

        List<Type> groupingColumnTypes = trinoType(groupingColumnsSelector.getReturnedType()).getTypeParameters();
        int groupingColumnsCount = groupingColumnTypes.size();
        groupingSets.stream()
                .flatMap(List::stream)
                .forEach(index -> checkArgument(index >= 0 && index < groupingColumnsCount, "invalid grouping column for GroupId operation"));

        ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();
        // distinct grouping columns
        groupingSets.stream()
                .flatMap(List::stream)
                .collect(toImmutableSet()).stream()
                .map(groupingColumnTypes::get)
                .forEach(outputTypes::add);
        // aggregation arguments
        getSelectedFields(aggregationArgumentsSelector).stream()
                .map(relationRowType(trinoType(input.type())).getTypeParameters()::get)
                .forEach(outputTypes::add);
        // group id column
        outputTypes.add(BIGINT);

        this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes.build()))));

        Map<AttributeKey, Object> operationAttributes = GROUPING_SETS.asMap(groupingSets);

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(GroupIdOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes, groupingColumnsSelector.getTerminalOperation().attributes(), aggregationArgumentsSelector.getTerminalOperation().attributes())));

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
        return ImmutableList.of(groupingColumnsSelector, aggregationArgumentsSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty group id";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new GroupId(
                result.name(),
                newArgument,
                groupingColumnsSelector.getOnlyBlock(),
                aggregationArgumentsSelector.getOnlyBlock(),
                GROUPING_SETS.getAttribute(attributes),
                ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(GroupIdOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Block groupingColumnsSelector()
    {
        return groupingColumnsSelector.getOnlyBlock();
    }

    public Block aggregationArgumentsSelector()
    {
        return aggregationArgumentsSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupId(this, context);
    }
}
