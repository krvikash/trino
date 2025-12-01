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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.dialect.trino.operationmetadata.CoalesceOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.CoalesceOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Coalesce
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> operands;
    private final Map<AttributeKey, Object> attributes;

    public Coalesce(String resultName, List<Value> operands, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        this(resultName, operands, sourceAttributes, ImmutableMap.of());
    }

    public Coalesce(String resultName, List<Value> operands, List<Map<AttributeKey, Object>> sourceAttributes, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(operands, "operands is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (operands.size() < 2) {
            throw new TrinoException(IR_ERROR, "coalesce operation must have at least two operands");
        }

        Type resultType = trinoType(operands.getFirst().type());
        if (!operands.stream()
                .map(Value::type)
                .map(TrinoDialect::trinoType)
                .allMatch(type -> type.equals(resultType))) {
            throw new TrinoException(IR_ERROR, "all operands must be of the same type");
        }

        this.result = new Result(resultName, irType(resultType));

        this.operands = ImmutableList.copyOf(operands);

        if (sourceAttributes.size() != operands.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), operands.size()));
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(CoalesceOperationMetadata.deriveAttributes(ImmutableMap.of(), sourceAttributes));
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
        return operands;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "coalesce :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newOperands = new ArrayList<>(operands);
        newOperands.set(index, newArgument);
        return new Coalesce(
                result.name(),
                newOperands,
                emptySourceAttributes(operands.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Coalesce(newName, operands, emptySourceAttributes(operands.size()));
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return ImmutableMap.of();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitCoalesce(this, context);
    }
}
