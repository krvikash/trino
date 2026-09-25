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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.InOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.InOperationMetadata.NAME;
import static java.util.Objects.requireNonNull;

public final class In
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Value valueList;
    private final Attributes attributes;

    public In(String resultName, Value input, Value valueList, List<Attributes> sourceAttributes)
    {
        this(resultName, input, valueList, sourceAttributes, Attributes.empty());
    }

    public In(String resultName, Value input, Value valueList, List<Attributes> sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(valueList, "valueList is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        Type inputType = trinoType(input.type());
        Type valueListType = trinoType(valueList.type());
        if (!(valueListType instanceof ArrayType arrayType)) {
            throw new TrinoException(IR_ERROR, "value list must be of array type");
        }
        if (!arrayType.getElementType().equals(inputType)) {
            throw new TrinoException(IR_ERROR, "value list element type must match the input type");
        }

        this.input = input;
        this.valueList = valueList;

        if (sourceAttributes.size() != 2) {
            throw new TrinoException(IR_ERROR, "In operation must have exactly two source attribute maps");
        }

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(InOperationMetadata.deriveAttributes(Attributes.empty(), sourceAttributes));
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
        return ImmutableList.of(input, valueList);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Attributes attributes()
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
        return new In(
                result.name(),
                index == 0 ? newArgument : input,
                index == 1 ? newArgument : valueList,
                emptySourceAttributes(2));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new In(newName, input, valueList, emptySourceAttributes(2));
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Value input()
    {
        return input;
    }

    public Value valueList()
    {
        return valueList;
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitIn(this, context);
    }
}
