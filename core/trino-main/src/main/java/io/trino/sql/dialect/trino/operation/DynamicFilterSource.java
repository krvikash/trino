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
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public class DynamicFilterSource
        extends TrinoOperation
{
    private static final String NAME = "dynamic_filter_source";

    private final Result result;
    private final Value input;
    private final Region dynamicFilterTargetSelector;
    private final Map<AttributeKey, Object> attributes;

    public DynamicFilterSource(String resultName, Value input, Block dynamicFilterTargetSelector, List<String> dynamicFilterIds, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(dynamicFilterTargetSelector, "dynamicFilterTargetSelector is null");
        requireNonNull(dynamicFilterIds, "dynamicFilterIds is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type())) ||
                relationRowType(trinoType(input.type())).equals(EMPTY_ROW)) {
            throw new TrinoException(IR_ERROR, "input to the DynamicFilterSource operation must be of relation type with non-empty rows");
        }
        this.input = input;

        this.result = new Result(resultName, input.type());

        validateRowSelector(dynamicFilterTargetSelector, relationRowType(trinoType(input.type())), "invalid dynamic filter target selector for DynamicFilterSource operation");
        this.dynamicFilterTargetSelector = singleBlockRegion(dynamicFilterTargetSelector);

        if (trinoType(dynamicFilterTargetSelector.getReturnedType()).getTypeParameters().size() != dynamicFilterIds.size()) {
            throw new TrinoException(IR_ERROR, "dynamic filter target selector for DynamicFilterSource operation does not match dynamic filter IDs");
        }

        // TODO derive attributes from source attributes
        this.attributes = DYNAMIC_FILTER_IDS.asMap(dynamicFilterIds);
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
        return ImmutableList.of(dynamicFilterTargetSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty dynamic filter source";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new DynamicFilterSource(
                result.name(),
                newArgument,
                dynamicFilterTargetSelector.getOnlyBlock(),
                DYNAMIC_FILTER_IDS.getAttribute(attributes),
                ImmutableMap.of());
    }

    public Value argument()
    {
        return input;
    }

    public Block dynamicFilterTargetSelector()
    {
        return dynamicFilterTargetSelector.getOnlyBlock();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitDynamicFilterSource(this, context);
    }
}
