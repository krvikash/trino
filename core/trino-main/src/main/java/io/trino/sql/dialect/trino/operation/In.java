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
import io.trino.sql.dialect.trino.operationmetadata.InOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.InOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class In
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final List<Value> inputList;
    private final Map<AttributeKey, Object> attributes;

    public In(String resultName, Value input, List<Value> inputList, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(inputList, "inputList is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        Type inputType = trinoType(input.type());
        if (!inputList.stream()
                .map(Value::type)
                .map(TrinoDialect::trinoType)
                .allMatch(type -> type.equals(inputType))) {
            throw new TrinoException(IR_ERROR, "all values must be of the same type");
        }

        this.input = input;

        this.inputList = ImmutableList.copyOf(inputList);

        if (sourceAttributes.size() != 1 + inputList.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), 1 + inputList.size()));
        }

        this.attributes = InOperationMetadata.deriveAttributes(ImmutableMap.of(), sourceAttributes);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.<Value>builder()
                .add(input)
                .addAll(inputList)
                .build();
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
        return "in :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newInputList = new ArrayList<>(inputList);
        if (index >= 1 && index < 1 + inputList.size()) {
            newInputList.set(index - 1, newArgument);
        }
        return new In(
                result.name(),
                index == 0 ? newArgument : input,
                newInputList,
                emptySourceAttributes(1 + inputList.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new In(newName, input, inputList, emptySourceAttributes(1 + inputList.size()));
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return ImmutableMap.of();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitIn(this, context);
    }
}
