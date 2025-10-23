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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
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
import static io.trino.sql.dialect.trino.operationmetadata.ArrayOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Array
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> elements;
    private final Map<AttributeKey, Object> attributes;

    public Array(String resultName, Type elementType, List<Value> elements, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(elementType, "elementType is null");
        requireNonNull(elements, "elements is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(new ArrayType(elementType)));

        elements.stream()
                .forEach(element -> {
                    if (!trinoType(element.type()).equals(elementType)) {
                        throw new TrinoException(IR_ERROR, format("type of array element: %s does not match the declared type: %s", trinoType(element.type()).getDisplayName(), elementType.getDisplayName()));
                    }
                });
        this.elements = ImmutableList.copyOf(elements);

        if (sourceAttributes.size() != elements.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), elements.size()));
        }

        // TODO derive attributes from source attributes
        this.attributes = ImmutableMap.of();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return elements;
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
        return "array :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newArguments = new ArrayList<>(elements);
        newArguments.set(index, newArgument);
        return new Array(
                result.name(),
                ((ArrayType) trinoType(result.type())).getElementType(),
                newArguments,
                emptySourceAttributes(elements.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Array(newName, ((ArrayType) trinoType(result.type())).getElementType(), elements, emptySourceAttributes(elements.size()));
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitArray(this, context);
    }
}
